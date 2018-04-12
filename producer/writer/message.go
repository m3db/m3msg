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
	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3msg/protocol/proto"

	"go.uber.org/atomic"
)

type message struct {
	producer.RefCountedData

	pb         msgpb.Message
	meta       metadata
	retryNanos int64
	retried    int
	isClosed   *atomic.Bool
}

func newMessage() *message {
	return &message{
		retryNanos: 0,
		retried:    0,
		isClosed:   atomic.NewBool(false),
	}
}

// Reset resets the message.
func (m *message) Reset(meta metadata, data producer.RefCountedData) {
	m.meta = meta
	m.RefCountedData = data

	m.pb.Metadata.Shard = meta.shard
	m.pb.Metadata.Id = meta.id
	m.pb.Value = data.Bytes()

	m.retryNanos = 0
	m.retried = 0
	m.isClosed.Store(false)
}

// RetryNanos returns the timestamp for next retry in nano seconds.
// This is NOT thread safe.
func (m *message) RetryNanos() int64 {
	return m.retryNanos
}

// SetRetryNanos sets the next retry nanos.
// This is NOT thread safe.
func (m *message) SetRetryNanos(value int64) {
	m.retryNanos = value
}

// RetriedTimes returns how many times this message has been retried.
// This is NOT thread safe.
func (m *message) RetriedTimes() int {
	return m.retried
}

// IncRetriedTimes increments the retried times.
// This is NOT thread safe.
func (m *message) IncRetriedTimes() {
	m.retried++
}

// IsDroppedOrConsumed returns true if the message has been dropped or consumed.
func (m *message) IsDroppedOrAcked() bool {
	return m.isClosed.Load() || m.RefCountedData.IsDroppedOrConsumed()
}

// Ack acknowledges the message. Duplicated acks on the same message might cause panic.
func (m *message) Ack() {
	m.RefCountedData.DecRef()
	m.isClosed.Store(true)
}

// Metadata returns the metadata.
func (m *message) Metadata() metadata {
	return m.meta
}

// Marshaler returns the marshaler and a bool to indicate whether the marshaler is valid.
func (m *message) Marshaler() (proto.Marshaler, bool) {
	return &m.pb, !m.RefCountedData.IsDroppedOrConsumed()
}
