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
	"time"

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

	// Init will initialize the writer with the current placement
	// and setup the background thread to watch for updates.
	// It will return error if no placement is available within timeout
	// for the service but will still watch for future updates.
	Init() error

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
}

func newConsumerServiceWriterMetrics(m tally.Scope) consumerServiceWriterMetrics {
	return consumerServiceWriterMetrics{
		placementUpdate: m.Counter("placement-update"),
		placementError:  m.Counter("placement-error"),
		invalidShard:    m.Counter("invalid-shard"),
	}
}

type processPlacementFn func(p placement.Placement)

type consumerServiceWriterImpl struct {
	sync.RWMutex

	cs             topic.ConsumerService
	numberOfShards uint32
	ps             placement.Service
	shardWriters   []shardWriter
	opts           Options
	logger         log.Logger

	dataFilter      producer.FilterFunc
	router          ackRouter
	consumerWriters map[string]consumerWriter
	isClosed        bool
	doneCh          chan struct{}
	wg              sync.WaitGroup
	m               consumerServiceWriterMetrics

	processPlacementFn processPlacementFn
}

// TODO: Remove the nolint comment after adding usage of this function.
// nolint: deadcode
func newConsumerServiceWriter(
	cs topic.ConsumerService,
	numberOfShards uint32,
	mPool messagePool,
	opts Options,
) (consumerServiceWriter, error) {
	ps, err := opts.ServiceDiscovery().PlacementService(cs.ServiceID(), nil)
	if err != nil {
		return nil, err
	}
	router := newAckRouter(int(numberOfShards))
	w := &consumerServiceWriterImpl{
		cs:              cs,
		numberOfShards:  numberOfShards,
		ps:              ps,
		shardWriters:    initShardWriters(router, cs, numberOfShards, mPool, opts),
		opts:            opts,
		logger:          opts.InstrumentOptions().Logger(),
		dataFilter:      acceptAllFilter,
		router:          router,
		consumerWriters: make(map[string]consumerWriter),
		isClosed:        false,
		doneCh:          make(chan struct{}),
		m:               newConsumerServiceWriterMetrics(opts.InstrumentOptions().MetricsScope()),
	}
	w.processPlacementFn = w.processPlacement
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
	if shard >= w.numberOfShards {
		w.m.invalidShard.Inc(1)
		return fmt.Errorf("could not write data for shard %d which is larger than max shard id %d", shard, w.numberOfShards)
	}
	if d.Filter(w.dataFilter) {
		w.shardWriters[shard].Write(d)
	}
	// It is not an error if the data does not pass the filter.
	return nil
}

func (w *consumerServiceWriterImpl) Init() error {
	var (
		pw      placement.Watch
		err     error
		retrier = retry.NewRetrier(w.opts.PlacementWatchRetryOptions().SetForever(true))
	)
	retrier.Attempt(func() error {
		pw, err = w.ps.Watch()
		if err != nil {
			// Unexpected, but if it ever happens, we will retry forever to recreate the watch.
			w.logger.Errorf(
				"could not create placement watch for %s, retry later: %v",
				w.cs.ServiceID().String(),
				err,
			)
			return err
		}
		return nil
	})
	err = w.waitForInitialPlacement(pw)

	w.wg.Add(1)
	go func() {
		w.watchPlacementUpdatesForever(pw)
		w.wg.Done()
	}()
	return err
}

func (w *consumerServiceWriterImpl) waitForInitialPlacement(pw placement.Watch) error {
	timeout := w.opts.PlacementWatchInitTimeout()
	select {
	case <-pw.C():
		return w.processPlacementWatchUpdate(pw)
	case <-time.After(timeout):
		return fmt.Errorf("could not receive initial placement for %s within %v", w.cs.ServiceID().String(), timeout)
	}
}

func (w *consumerServiceWriterImpl) watchPlacementUpdatesForever(pw placement.Watch) {
	for {
		select {
		case <-pw.C():
			w.processPlacementWatchUpdate(pw)
		case <-w.doneCh:
			for _, cw := range w.consumerWriters {
				cw.Close()
			}
			return
		}
	}
}

func (w *consumerServiceWriterImpl) processPlacementWatchUpdate(pw placement.Watch) error {
	p, err := pw.Get()
	if err != nil {
		w.m.placementError.Inc(1)
		w.logger.Errorf("could not process placement update from kv, %v", err)
		return err
	}
	w.m.placementUpdate.Inc(1)
	w.processPlacementFn(p)
	return nil
}

func (w *consumerServiceWriterImpl) processPlacement(p placement.Placement) {
	// NB(cw): Lock is not needed as w.consumerWriters is only accessed in this thread.
	newConsumerWriters, tobeDeleted := w.diffPlacement(p)
	for i, sw := range w.shardWriters {
		sw.UpdateInstances(p.InstancesForShard(uint32(i)), newConsumerWriters)
	}
	oldConsumerWriters := w.consumerWriters
	w.consumerWriters = newConsumerWriters
	go func() {
		for _, addr := range tobeDeleted {
			cw, ok := oldConsumerWriters[addr]
			if ok {
				cw.Close()
			}
		}
	}()
}

func (w *consumerServiceWriterImpl) diffPlacement(newPlacement placement.Placement) (map[string]consumerWriter, []string) {
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
	if w.isClosed {
		w.Unlock()
		return
	}
	w.isClosed = true
	w.Unlock()

	// Blocks until all messages consuemd.
	for _, sw := range w.shardWriters {
		sw.Close()
	}
	close(w.doneCh)
	w.wg.Wait()
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
