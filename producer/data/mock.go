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
	"github.com/m3db/m3msg/producer"
)

// MockData is a mock for data.
type MockData struct {
	data        string
	closeCalled int
	closeReason producer.DataFinalizeReason
}

// NewMockData returns a mock of data.
func NewMockData(str string) *MockData {
	return &MockData{
		data:        str,
		closeCalled: 0,
	}
}

// Shard returns the shard of the data.
func (d *MockData) Shard() uint32 {
	return 0
}

// Bytes returns the bytes of the data.
func (d *MockData) Bytes() []byte {
	return []byte(d.data)
}

// Size returns the size of the bytes of the data.
func (d *MockData) Size() uint32 {
	return uint32(len(d.data))
}

// Finalize will be called by producer to indicate the end of its lifecycle.
func (d *MockData) Finalize(r producer.DataFinalizeReason) {
	d.closeCalled++
	d.closeReason = r
}

// CloseCalled returns the number of times the data is closed.
func (d *MockData) CloseCalled() int {
	return d.closeCalled
}

// CloseReason returns the reason why the data is closed.
func (d *MockData) CloseReason() producer.DataFinalizeReason {
	return d.closeReason
}
