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

package buffer

import (
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3msg/producer/data"

	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"
)

var (
	errBufferFull                         = errors.New("buffer full")
	errBufferClosed                       = errors.New("buffer closed")
	errInvalidDataLargerThanMaxBufferSize = errors.New("invalid data, larger than max buffer size")
)

type bufferMetrics struct {
	messageDropped  tally.Counter
	bytesDropped    tally.Counter
	messageBuffered tally.Gauge
	bytesBuffered   tally.Gauge
}

func newBufferMetrics(scope tally.Scope) *bufferMetrics {
	return &bufferMetrics{
		messageDropped:  scope.Counter("message-dropped"),
		bytesDropped:    scope.Counter("bytes-dropped"),
		messageBuffered: scope.Gauge("message-buffered"),
		bytesBuffered:   scope.Gauge("bytes-buffered"),
	}
}

type buffer struct {
	sync.Mutex

	strategy        OnFullStrategy
	maxBufferSize   uint64
	cleanupInterval time.Duration
	buffer          *list.List
	m               *bufferMetrics

	size     *atomic.Uint64
	doneCh   chan struct{}
	isClosed bool
}

// NewBuffer returns a new buffer.
func NewBuffer(opts Options) producer.Buffer {
	if opts == nil {
		opts = NewBufferOptions()
	}
	return &buffer{
		size:            atomic.NewUint64(0),
		buffer:          list.New(),
		isClosed:        false,
		doneCh:          make(chan struct{}),
		strategy:        opts.OnFullStrategy(),
		maxBufferSize:   opts.MaxBufferSize(),
		cleanupInterval: opts.CleanupInterval(),
		m:               newBufferMetrics(opts.InstrumentOptions().MetricsScope()),
	}
}

func (b *buffer) Buffer(d producer.Data) (producer.RefCountedData, error) {
	b.Lock()
	if b.isClosed {
		b.Unlock()
		return nil, errBufferClosed
	}
	var (
		dataSize      = uint64(d.Size())
		maxBufferSize = b.maxBufferSize
	)
	if dataSize > maxBufferSize {
		b.Unlock()
		return nil, errInvalidDataLargerThanMaxBufferSize
	}
	targetBufferSize := maxBufferSize - dataSize
	if b.size.Load() > targetBufferSize {
		if err := b.produceOnFullWithLock(targetBufferSize); err != nil {
			b.Unlock()
			return nil, err
		}
	}
	b.size.Add(dataSize)
	rd := data.NewRefCountedData(d, b.subSize)
	b.buffer.PushBack(rd)
	b.Unlock()
	return rd, nil
}

func (b *buffer) produceOnFullWithLock(targetSize uint64) error {
	switch b.strategy {
	case ReturnError:
		return errBufferFull
	case DropEarliest:
		b.dropEarliestUntilTargetWithLock(targetSize)
	}
	return nil
}

func (b *buffer) dropEarliestUntilTargetWithLock(targetSize uint64) {
	var next *list.Element
	for e := b.buffer.Front(); e != nil && b.size.Load() > targetSize; e = next {
		next = e.Next()
		d := e.Value.(producer.RefCountedData)
		if !d.IsClosed() {
			b.m.messageDropped.Inc(1)
			b.m.bytesDropped.Inc(int64(d.Size()))
			d.Drop()
		}
		b.buffer.Remove(e)
	}
}

func (b *buffer) Init() {
	go b.cleanupForever()
}

func (b *buffer) cleanupForever() {
	ticker := time.NewTicker(b.cleanupInterval)
	for {
		select {
		case <-ticker.C:
			b.Lock()
			b.cleanupWithLock()
			b.m.messageBuffered.Update(float64(b.buffer.Len()))
			b.Unlock()
			b.m.bytesBuffered.Update(float64(b.size.Load()))
		case <-b.doneCh:
			ticker.Stop()
			return
		}
	}
}

func (b *buffer) cleanupWithLock() {
	var next *list.Element
	for e := b.buffer.Front(); e != nil; e = next {
		next = e.Next()
		d := e.Value.(producer.RefCountedData)
		if d.IsClosed() {
			b.buffer.Remove(e)
		}
	}
}

func (b *buffer) Close() {
	// Stop taking writes right away.
	b.Lock()
	if b.isClosed {
		b.Unlock()
		return
	}
	b.isClosed = true
	b.Unlock()

	// Block until all data consumed.
	for {
		b.Lock()
		l := b.buffer.Len()
		b.Unlock()
		if l == 0 {
			break
		}
	}
	close(b.doneCh)
}

func (b *buffer) subSize(d producer.RefCountedData) {
	b.size.Sub(d.Size())
}
