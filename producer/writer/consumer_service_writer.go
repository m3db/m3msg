// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package writer

import (
	"fmt"
	"sync"

	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3msg/topic"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
)

var (
	acceptAllFilter = producer.FilterFunc(
		func(d producer.Data) bool {
			return true
		},
	)
)

type consumerServiceWriter interface {
	// Write writes data.
	Write(d producer.RefCountedData) error

	// Init will initialize the consumer service writer.
	Init()

	// Close closes the writer and the background watch thread.
	// It should block until all the data for the given consumer service
	// have been consumed.
	Close()

	// RegisterFilter registers a filter for the consumer service.
	RegisterFilter(fn producer.FilterFunc)

	// UnregisterFilter unregisters the filter for the consumer service.
	UnregisterFilter()

	// ServiceID returns the service id of the writer.
	ServiceID() services.ServiceID
}

type consumerServiceWriterMetrics struct {
	placementError  tally.Counter
	placementUpdate tally.Counter
	invalidShard    tally.Counter
	filteredPassed  tally.Counter
	filterNotPassed tally.Counter
}

func newConsumerServiceWriterMetrics(m tally.Scope) consumerServiceWriterMetrics {
	return consumerServiceWriterMetrics{
		placementUpdate: m.Counter("placement-update"),
		placementError:  m.Counter("placement-error"),
		invalidShard:    m.Counter("invalid-shard"),
		filteredPassed:  m.Counter("filter-passed"),
		filterNotPassed: m.Counter("filter-not-passed"),
	}
}

type consumerServiceWriterImpl struct {
	sync.RWMutex

	cs           topic.ConsumerService
	numShards    uint32
	ps           placement.Service
	shardWriters []shardWriter
	opts         Options
	logger       log.Logger

	value           runtime.Value
	dataFilter      producer.FilterFunc
	router          ackRouter
	consumerWriters map[string]consumerWriter
	closed          bool
	m               consumerServiceWriterMetrics

	processFn runtime.ProcessFn
}

// TODO: Remove the nolint comment after adding usage of this function.
// nolint: deadcode
func newConsumerServiceWriter(
	cs topic.ConsumerService,
	numShards uint32,
	mPool messagePool,
	opts Options,
) (consumerServiceWriter, error) {
	ps, err := opts.ServiceDiscovery().PlacementService(cs.ServiceID(), nil)
	if err != nil {
		return nil, err
	}
	router := newAckRouter(int(numShards))
	w := &consumerServiceWriterImpl{
		cs:              cs,
		numShards:       numShards,
		ps:              ps,
		shardWriters:    initShardWriters(router, cs, numShards, mPool, opts),
		opts:            opts,
		logger:          opts.InstrumentOptions().Logger(),
		dataFilter:      acceptAllFilter,
		router:          router,
		consumerWriters: make(map[string]consumerWriter),
		closed:          false,
		m:               newConsumerServiceWriterMetrics(opts.InstrumentOptions().MetricsScope()),
	}
	w.processFn = w.process
	return w, nil
}

func initShardWriters(
	router ackRouter,
	cs topic.ConsumerService,
	numberOfShards uint32,
	mPool messagePool,
	opts Options,
) []shardWriter {
	sws := make([]shardWriter, numberOfShards)
	for i := range sws {
		switch cs.ConsumptionType() {
		case topic.Shared:
			sws[i] = newSharedShardWriter(uint32(i), router, mPool, opts)
		case topic.Replicated:
			sws[i] = newReplicatedShardWriter(uint32(i), numberOfShards, router, mPool, opts)
		}
	}
	return sws
}

func (w *consumerServiceWriterImpl) Write(d producer.RefCountedData) error {
	shard := d.Shard()
	if shard >= w.numShards {
		w.m.invalidShard.Inc(1)
		return fmt.Errorf("could not write data for shard %d which is larger than max shard id %d", shard, w.numShards)
	}
	if d.Accept(w.dataFilter) {
		w.shardWriters[shard].Write(d)
		w.m.filteredPassed.Inc(1)
	}
	// It is not an error if the data does not pass the filter.
	w.m.filterNotPassed.Inc(1)
	return nil
}

func (w *consumerServiceWriterImpl) Init() {
	updatableFn := func() (runtime.Updatable, error) {
		return w.ps.Watch()
	}
	getFn := func(updatable runtime.Updatable) (interface{}, error) {
		update, err := updatable.(placement.Watch).Get()
		if err != nil {
			w.m.placementError.Inc(1)
			w.logger.Errorf("invalid placement update from kv, %v", err)
			return nil, err
		}
		w.m.placementUpdate.Inc(1)
		return update, nil
	}
	vOptions := runtime.NewOptions().
		SetInitWatchTimeout(w.opts.PlacementWatchInitTimeout()).
		SetInstrumentOptions(w.opts.InstrumentOptions()).
		SetUpdatableFn(updatableFn).SetGetFn(getFn).SetProcessFn(w.processFn)
	w.value = runtime.NewValue(vOptions)
	if err := w.startWatch(); err == nil {
		return
	}
	// Since the consumer service writer could potentially be added during
	// runtime while producer is already running, in case we failed to get
	// a placement watch, we will retry forever in the background to establish
	// the watch.
	retrier := retry.NewRetrier(w.opts.PlacementWatchRetryOptions().SetForever(true))
	continueFn := func(int) bool {
		return !w.isClosed()
	}
	go retrier.AttemptWhile(continueFn, func() error {
		return w.startWatch()
	})
}

func (w *consumerServiceWriterImpl) startWatch() error {
	err := w.value.Watch()
	if err != nil && runtime.IsCreateWatchError(err) {
		return err
	}
	return nil
}
func (w *consumerServiceWriterImpl) process(update interface{}) error {
	p := update.(placement.Placement)
	// NB(cw): Lock can be removed as w.consumerWriters is only accessed in this thread.
	w.Lock()
	newConsumerWriters, tobeDeleted := w.diffPlacementWithLock(p)
	for i, sw := range w.shardWriters {
		sw.UpdateInstances(p.InstancesForShard(uint32(i)), newConsumerWriters)
	}
	oldConsumerWriters := w.consumerWriters
	w.consumerWriters = newConsumerWriters
	w.Unlock()
	go func() {
		for _, addr := range tobeDeleted {
			cw, ok := oldConsumerWriters[addr]
			if ok {
				cw.Close()
			}
		}
	}()
	return nil
}

func (w *consumerServiceWriterImpl) diffPlacementWithLock(newPlacement placement.Placement) (map[string]consumerWriter, []string) {
	var (
		newInstances       = newPlacement.Instances()
		newConsumerWriters = make(map[string]consumerWriter, len(newInstances))
		toBeDeleted        []string
	)
	for _, instance := range newInstances {
		id := instance.Endpoint()
		cw, ok := w.consumerWriters[id]
		if ok {
			newConsumerWriters[id] = cw
			continue
		}
		cw = newConsumerWriter(instance.Endpoint(), w.router, w.opts)
		cw.Init()
		newConsumerWriters[id] = cw
	}

	for id := range w.consumerWriters {
		if _, ok := newConsumerWriters[id]; !ok {
			toBeDeleted = append(toBeDeleted, id)
		}
	}
	return newConsumerWriters, toBeDeleted
}

func (w *consumerServiceWriterImpl) Close() {
	w.Lock()
	if w.closed {
		w.Unlock()
		return
	}
	w.closed = true
	w.Unlock()

	// Blocks until all messages consuemd.
	for _, sw := range w.shardWriters {
		sw.Close()
	}
	w.value.Unwatch()
	for _, cw := range w.consumerWriters {
		cw.Close()
	}
}

func (w *consumerServiceWriterImpl) ServiceID() services.ServiceID {
	return w.cs.ServiceID()
}

func (w *consumerServiceWriterImpl) RegisterFilter(filter producer.FilterFunc) {
	w.Lock()
	w.dataFilter = filter
	w.Unlock()
}

func (w *consumerServiceWriterImpl) UnregisterFilter() {
	w.Lock()
	w.dataFilter = acceptAllFilter
	w.Unlock()
}

func (w *consumerServiceWriterImpl) isClosed() bool {
	w.RLock()
	c := w.closed
	w.RUnlock()
	return c
}
