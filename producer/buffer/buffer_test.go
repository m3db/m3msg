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
	"testing"
	"time"

	"github.com/m3db/m3msg/producer/data"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	d := data.NewMockData("foo")
	b := NewBuffer(nil)
	bd, err := b.Buffer(d)
	require.NoError(t, err)
	require.Equal(t, bd.Size(), uint64(d.Size()))
	require.Equal(t, 0, d.CloseCalled())
	require.Equal(t, bd.Size(), b.(*buffer).size.Load())

	// Finalize the data will reduce the buffer size.
	bd.IncRef()
	bd.DecRef()
	require.Equal(t, 1, d.CloseCalled())
	require.Equal(t, bd.Size(), uint64(d.Size()))
	require.Equal(t, 0, int(b.(*buffer).size.Load()))
}

func TestBufferWithSmallSize(t *testing.T) {
	d := data.NewMockData("foo")
	b := NewBuffer(NewBufferOptions().SetMaxBufferSize(1))
	_, err := b.Buffer(d)
	require.Error(t, err)
}

func TestBufferCleanupEarliest(t *testing.T) {
	d := data.NewMockData("foo")
	b := NewBuffer(NewBufferOptions())
	bd, err := b.Buffer(d)
	require.NoError(t, err)
	require.Equal(t, bd.Size(), uint64(d.Size()))
	require.Equal(t, 0, d.CloseCalled())
	require.Equal(t, bd.Size(), b.(*buffer).size.Load())

	b.(*buffer).dropEarliestUntilTargetWithLock(0)
	require.Equal(t, 1, d.CloseCalled())
	require.Equal(t, 0, int(b.(*buffer).size.Load()))
}

func TestBufferCleanupBackground(t *testing.T) {
	d := data.NewMockData("foo")
	b := NewBuffer(NewBufferOptions().SetCleanupInterval(100 * time.Millisecond).SetInstrumentOptions(instrument.NewOptions())).(*buffer)
	bd, err := b.Buffer(d)
	require.NoError(t, err)
	require.Equal(t, bd.Size(), uint64(d.Size()))
	require.Equal(t, 0, d.CloseCalled())
	require.Equal(t, bd.Size(), b.size.Load())

	b.Init()
	bd.IncRef()
	bd.DecRef()

	b.Close()
	require.Equal(t, 1, d.CloseCalled())
	require.Equal(t, 0, int(b.size.Load()))
	_, err = b.Buffer(d)
	require.Error(t, err)

	// Safe to close again.
	b.Close()
	require.Equal(t, 1, d.CloseCalled())
	require.Equal(t, 0, int(b.size.Load()))
}

func TestBufferDropEarliestOnFull(t *testing.T) {
	d1 := data.NewMockData("foo1")
	d2 := data.NewMockData("foo2")
	d3 := data.NewMockData("foo3")
	b := NewBuffer(NewBufferOptions().SetMaxBufferSize(int(3 * d1.Size())))

	rd1, err := b.Buffer(d1)
	require.NoError(t, err)
	rd2, err := b.Buffer(d2)
	require.NoError(t, err)
	rd3, err := b.Buffer(d3)
	require.NoError(t, err)
	require.False(t, rd1.IsClosed())
	require.False(t, rd2.IsClosed())
	require.False(t, rd3.IsClosed())

	_, err = b.Buffer(data.NewMockData("foobar"))
	require.NoError(t, err)
	require.True(t, rd1.IsClosed())
	require.True(t, rd2.IsClosed())
	require.False(t, rd3.IsClosed())
}

func TestBufferReturnErrorOnFull(t *testing.T) {
	d1 := data.NewMockData("foo1")
	d2 := data.NewMockData("foo2")
	d3 := data.NewMockData("foo3")
	b := NewBuffer(
		NewBufferOptions().
			SetMaxBufferSize(int(3 * d1.Size())).
			SetOnFullStrategy(ReturnError),
	)

	rd1, err := b.Buffer(d1)
	require.NoError(t, err)
	rd2, err := b.Buffer(d2)
	require.NoError(t, err)
	rd3, err := b.Buffer(d3)
	require.NoError(t, err)
	require.False(t, rd1.IsClosed())
	require.False(t, rd2.IsClosed())
	require.False(t, rd3.IsClosed())

	_, err = b.Buffer(data.NewMockData("foobar"))
	require.Error(t, err)
}

func BenchmarkProduce(b *testing.B) {
	size := 200
	buffer := NewBuffer(
		NewBufferOptions().
			SetMaxBufferSize(1000 * 1000 * 200).
			SetOnFullStrategy(DropEarliest),
	)
	bytes := make([]byte, size)
	testData := data.NewMockData(string(bytes))

	for n := 0; n < b.N; n++ {
		_, err := buffer.Buffer(testData)
		if err != nil {
			b.FailNow()
		}
	}
}
