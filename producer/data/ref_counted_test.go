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
	"testing"
	"time"

	"github.com/m3db/m3msg/producer"

	"github.com/stretchr/testify/require"
)

func TestRefCountedDataConsume(t *testing.T) {
	md := NewMockData("foo")
	rd := NewRefCountedData(md, nil)
	require.Equal(t, rd.Size(), uint64(md.Size()))
	require.False(t, rd.IsClosed())
	require.Equal(t, 0, md.CloseCalled())

	rd.IncRef()
	rd.DecRef()
	require.True(t, rd.IsClosed())
	require.Equal(t, 1, md.CloseCalled())
	require.Equal(t, producer.Consumed, md.CloseReason())

	rd.IncRef()
	rd.DecRef()
	require.True(t, rd.IsClosed())
	require.Equal(t, 1, md.CloseCalled())
	require.Equal(t, producer.Consumed, md.CloseReason())

	rd.Drop()
	require.True(t, rd.IsClosed())
	require.Equal(t, 1, md.CloseCalled())
	require.Equal(t, producer.Consumed, md.CloseReason())
}

func TestRefCountedDataDrop(t *testing.T) {
	md := NewMockData("foo")
	rd := NewRefCountedData(md, nil)
	require.Equal(t, rd.Size(), uint64(md.Size()))
	require.False(t, rd.IsClosed())
	require.Equal(t, 0, md.CloseCalled())

	rd.Drop()
	require.True(t, rd.IsClosed())
	require.Equal(t, 1, md.CloseCalled())
	require.Equal(t, producer.Dropped, md.CloseReason())

	rd.IncRef()
	rd.DecRef()
	require.True(t, rd.IsClosed())
	require.Equal(t, 1, md.CloseCalled())
	require.Equal(t, producer.Dropped, md.CloseReason())

	rd.Drop()
	require.True(t, rd.IsClosed())
	require.Equal(t, 1, md.CloseCalled())
	require.Equal(t, producer.Dropped, md.CloseReason())
}

func TestRefCountedDataBytesReadBlocking(t *testing.T) {
	str := "foo"
	md := NewMockData(str)
	rd := NewRefCountedData(md, nil)

	b, ok, fn := rd.Bytes()
	require.Equal(t, str, string(b))
	require.True(t, ok)
	require.NotNil(t, fn)

	doneCh := make(chan struct{})
	go func() {
		rd.Drop()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		require.FailNow(t, "not expected")
	case <-time.After(time.Second):
	}
}

func TestRefCountedDataBytesInvalidAfterClose(t *testing.T) {
	md := NewMockData("foo")
	rd := NewRefCountedData(md, nil)
	rd.Drop()
	_, ok, _ := rd.Bytes()
	require.False(t, ok)
}

func TestRefCountedDataDecPanic(t *testing.T) {
	md := NewMockData("foo")
	rd := NewRefCountedData(md, nil)
	require.Panics(t, rd.DecRef)
}

func TestRefCountedDataFilter(t *testing.T) {
	str := "foo"
	md := NewMockData(str)
	rd := NewRefCountedData(md, nil)

	var called int
	filter := func(data producer.Data) bool {
		called++
		if data.Shard() == 0 && string(data.Bytes()) == str {
			return true
		}
		return false
	}

	require.True(t, rd.Filter(filter))
	require.False(t, NewRefCountedData(NewMockData("bar"), nil).Filter(filter))
}

func TestRefCountedDataOnDropFn(t *testing.T) {
	str := "foo"
	md := NewMockData(str)

	var called int
	fn := func(d producer.RefCountedData) {
		called++
		require.Equal(t, md, d.(*refCountedData).Data)
	}

	rd := NewRefCountedData(md, fn)
	rd.Drop()
	require.Equal(t, 1, called)
}
