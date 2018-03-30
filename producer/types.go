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

package producer

import (
	"github.com/m3db/m3cluster/services"
)

// DataFinalizeReason defines the reason why the data is being finalized by Producer.
type DataFinalizeReason int

const (
	// Consumed means the data has been fully consumed.
	Consumed DataFinalizeReason = iota

	// Expired means the data has been expired.
	Expired
)

// Data contains the data that will be produced by the producer.
// It should only be finalized by the producer.
type Data interface {
	// Shard returns the shard of the data.
	Shard() uint32

	// Bytes returns the bytes of the data.
	Bytes() []byte

	// Size returns the size of the bytes of the data.
	Size() uint32

	// Finalize will be called by producer to indicate the end of its lifecycle.
	Finalize(DataFinalizeReason)
}

// Producer produces data to a topic.
type Producer interface {
	// Produce produces the data.
	Produce(data Data) error

	// RegisterFilter registers a filter to a consumer service.
	RegisterFilter(sid services.ServiceID, fn FilterFunc)

	// RemoveFilter removes the filter of a consumer service.
	RemoveFilter(sid services.ServiceID)

	// Init initializes a producer.
	Init()

	// Close stops the producer from accepting new requests immediately.
	// It will also block until all the data buffered in Producer has been consumed.
	Close()
}

// FilterFunc can filter data.
type FilterFunc func(data Data) bool

// Options configs a producer.
type Options interface {
	// Buffer returns the buffer.
	Buffer() Buffer

	// SetBuffer sets the buffer.
	SetBuffer(value Buffer) Options

	// Writer returns the writer.
	Writer() Writer

	// SetWriter sets the writer.
	SetWriter(value Writer) Options
}

// Buffer buffers all the data in the producer.
type Buffer interface {
	// Buffer buffers a data and returns a reference counted data.
	Buffer(data Data) (RefCountedData, error)

	// Init initializes the buffer.
	Init()

	// Close stops the buffer from accepting new requests immediately.
	// It will also block until all the data buffered has been consumed.
	Close()
}

// Writer writes all the data out to the consumer services.
type Writer interface {
	// Write writes a reference counted data out.
	Write(shard uint32, d RefCountedData) error

	// RegisterFilter registers a filter to a consumer service.
	RegisterFilter(sid services.ServiceID, fn FilterFunc)

	//RemoveFilter removes the filter of a consumer service.
	RemoveFilter(sid services.ServiceID)

	// Init initializes a writer.
	Init() error

	// Close closes the writer.
	Close()
}

// RefCountedData is a reference counted data.
type RefCountedData interface {
	// IncRef increments the ref count.
	IncRef()

	// DecRef decrements the ref count.
	DecRef()

	// IsClosed returns true if the underlying data has been closed.
	IsClosed() bool

	// Filter filters the data.
	Filter(fn FilterFunc) bool

	// Bytes returns:
	// - underlying bytes for the data,
	// - a bool to indicate if the bytes are valid,
	// - a DoneFn to be called after the bytes are used.
	// The DoneFn is used to prevent the underlying bytes
	// being recycled while the user is still using it.
	Bytes() ([]byte, bool, DoneFn)

	// Size returns the size of the data.
	Size() uint64

	// Drop drops the data without waiting for it to be acked by consumers.
	Drop()
}

// DoneFn should be called when user is done using the bytes.
type DoneFn func()