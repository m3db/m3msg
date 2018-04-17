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
	"testing"

	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3msg/producer/data"
	"github.com/m3db/m3x/pool"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestMessagePool(t *testing.T) {
	p := newMessagePool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	rd := data.NewRefCountedData(md, nil)
	rd.IncRef()

	m := p.Get()
	require.Nil(t, m.pb.Value)
	md.EXPECT().Bytes().Return([]byte("foo"))
	m.Reset(metadata{}, rd)
	m.SetRetryAtNanosWithLock(100)

	pb, ok := m.Marshaler()
	require.True(t, ok)
	require.Equal(t, []byte("foo"), pb.(*msgpb.Message).Value)

	md.EXPECT().Finalize(producer.Consumed)
	m.Ack()
	require.True(t, m.IsDroppedOrConsumed())
	p.Put(m)

	m = p.Get()
	require.True(t, m.IsDroppedOrConsumed())

	md.EXPECT().Bytes().Return([]byte("foo"))
	m.Reset(metadata{}, data.NewRefCountedData(md, nil))
	require.False(t, m.IsDroppedOrConsumed())
}
