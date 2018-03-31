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

package data

import (
	"sync"

	"github.com/m3db/m3msg/producer"

	"go.uber.org/atomic"
)

// OnFinalizeFn will be called when the data is being finalized.
type OnFinalizeFn func(d producer.RefCountedData)

type refCountedData struct {
	sync.RWMutex
	producer.Data

	refCount     *atomic.Int32
	onFinalizeFn OnFinalizeFn
	isClosed     bool
}

// NewRefCountedData creates RefCountedData.
func NewRefCountedData(data producer.Data, fn OnFinalizeFn) producer.RefCountedData {
	return &refCountedData{
		Data:         data,
		refCount:     atomic.NewInt32(0),
		onFinalizeFn: fn,
		isClosed:     false,
	}
}

func (d *refCountedData) Filter(fn producer.FilterFunc) bool {
	return fn(d.Data)
}

func (d *refCountedData) IncRef() {
	d.refCount.Inc()
}

func (d *refCountedData) DecRef() {
	rc := d.refCount.Dec()
	if rc == 0 {
		d.finalize(producer.Consumed)
	}
	if rc < 0 {
		panic("invalid ref count")
	}
}

func (d *refCountedData) Bytes() ([]byte, bool, producer.DoneFn) {
	d.RLock()
	return d.Data.Bytes(), !d.isClosed, d.RUnlock
}

func (d *refCountedData) Size() uint64 {
	return uint64(d.Data.Size())
}

func (d *refCountedData) Drop() {
	d.finalize(producer.Dropped)
}

func (d *refCountedData) IsClosed() bool {
	d.RLock()
	r := d.isClosed
	d.RUnlock()
	return r
}

func (d *refCountedData) finalize(r producer.DataFinalizeReason) {
	d.Lock()
	if d.isClosed {
		d.Unlock()
		return
	}
	d.isClosed = true
	if d.onFinalizeFn != nil {
		d.onFinalizeFn(d)
	}
	d.Data.Finalize(r)
	d.Unlock()
}
