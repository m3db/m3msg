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
	"net"
	"testing"
	"time"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3msg/producer/data"
	"github.com/m3db/m3msg/protocol/proto"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestSharedShardWriter(t *testing.T) {
	defer leaktest.Check(t)()

	a := newAckRouter(2)
	opts := testOptions()
	sw := newSharedShardWriter(1, a, testMessagePool(opts), opts)
	defer sw.Close()

	cw1 := newConsumerWriter("i1", a, opts)
	cw1.Init()
	defer cw1.Close()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	addr2 := lis.Addr().String()
	cw2 := newConsumerWriter(addr2, a, opts)
	cw2.Init()
	defer cw2.Close()

	cws := make(map[string]consumerWriter)
	cws["i1"] = cw1
	cws[addr2] = cw2

	i1 := placement.NewInstance().SetEndpoint("i1")
	i2 := placement.NewInstance().SetEndpoint(addr2)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncodeDecoderOptions())
	}()

	sw.UpdateInstances(
		[]placement.Instance{i1},
		cws,
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Bytes().Return([]byte("foo"))
	md.EXPECT().Finalize(producer.Consumed)

	sw.Write(data.NewRefCountedData(md, nil))

	mw := sw.(*sharedShardWriter).mw.(*messageWriterImpl)
	mw.RLock()
	require.Equal(t, 1, len(mw.consumerWriters))
	require.Equal(t, 1, mw.queue.Len())
	mw.RUnlock()

	sw.UpdateInstances(
		[]placement.Instance{i1, i2},
		cws,
	)
	mw.RLock()
	require.Equal(t, 2, len(mw.consumerWriters))
	mw.RUnlock()
	for {
		mw.RLock()
		l := mw.queue.Len()
		mw.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestReplicatedShardWriter(t *testing.T) {
	defer leaktest.Check(t)()

	a := newAckRouter(3)
	opts := testOptions()
	sw := newReplicatedShardWriter(1, 200, a, testMessagePool(opts), opts).(*replicatedShardWriter)
	defer sw.Close()

	lis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis1.Close()

	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis2.Close()

	addr1 := lis1.Addr().String()
	cw1 := newConsumerWriter(addr1, a, opts)
	cw1.Init()
	defer cw1.Close()

	addr2 := lis2.Addr().String()
	cw2 := newConsumerWriter(addr2, a, opts)
	cw2.Init()
	defer cw2.Close()

	cw3 := newConsumerWriter("i3", a, opts)
	cw3.Init()
	defer cw3.Close()

	cws := make(map[string]consumerWriter)
	cws[addr1] = cw1
	cws[addr2] = cw2
	cws["i3"] = cw3
	i1 := placement.NewInstance().
		SetEndpoint(addr1).
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)}))
	i2 := placement.NewInstance().
		SetEndpoint(addr2).
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)}))
	i3 := placement.NewInstance().
		SetEndpoint("i3").
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)}))

	go func() {
		testConsumeAndAckOnConnectionListener(t, lis1, opts.EncodeDecoderOptions())
	}()

	go func() {
		testConsumeAndAckOnConnectionListener(t, lis2, opts.EncodeDecoderOptions())
	}()

	sw.UpdateInstances(
		[]placement.Instance{i1, i3},
		cws,
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Bytes().Return([]byte("foo")).Times(2)

	sw.Write(data.NewRefCountedData(md, nil))

	require.Equal(t, 2, len(sw.messageWriters))

	mw1 := sw.messageWriters[i1.Endpoint()].(*messageWriterImpl)
	for {
		mw1.RLock()
		l := mw1.queue.Len()
		mw1.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mw2 := sw.messageWriters[i3.Endpoint()].(*messageWriterImpl)
	for {
		mw2.RLock()
		l := mw2.queue.Len()
		mw2.RUnlock()
		if l == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	md.EXPECT().Finalize(producer.Consumed)
	sw.UpdateInstances(
		[]placement.Instance{i1, i2},
		cws,
	)
	for {
		mw2.RLock()
		l := mw2.queue.Len()
		mw2.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestReplicatedShardWriterRemoveMessageWriter(t *testing.T) {
	defer leaktest.Check(t)()

	router := newAckRouter(2).(*router)
	opts := testOptions()
	sw := newReplicatedShardWriter(1, 200, router, testMessagePool(opts), opts).(*replicatedShardWriter)

	lis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis1.Close()

	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis2.Close()

	addr1 := lis1.Addr().String()
	cw1 := newConsumerWriter(addr1, router, opts)
	cw1.Init()
	defer cw1.Close()

	addr2 := lis2.Addr().String()
	cw2 := newConsumerWriter(addr2, router, opts)
	cw2.Init()
	defer cw2.Close()

	cws := make(map[string]consumerWriter)
	cws[addr1] = cw1
	cws[addr2] = cw2
	i1 := placement.NewInstance().
		SetEndpoint(addr1).
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)}))
	i2 := placement.NewInstance().
		SetEndpoint(addr2).
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)}))

	go func() {
		testConsumeAndAckOnConnectionListener(t, lis1, opts.EncodeDecoderOptions())
	}()

	sw.UpdateInstances(
		[]placement.Instance{i1, i2},
		cws,
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Bytes().Return([]byte("foo")).Times(2)

	sw.Write(data.NewRefCountedData(md, nil))

	require.Equal(t, 2, len(sw.messageWriters))
	mw1 := sw.messageWriters[i1.Endpoint()].(*messageWriterImpl)
	for {
		mw1.RLock()
		l := mw1.queue.Len()
		mw1.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	mw2 := sw.messageWriters[i2.Endpoint()].(*messageWriterImpl)
	mw2.RLock()
	require.Equal(t, 1, mw2.queue.Len())
	mw2.RUnlock()

	conn, err := lis2.Accept()
	require.NoError(t, err)
	defer conn.Close()
	server := proto.NewEncodeDecoder(conn, opts.EncodeDecoderOptions())

	var msg msgpb.Message
	require.NoError(t, server.Decode(&msg))
	sw.UpdateInstances(
		[]placement.Instance{i1},
		cws,
	)
	mw2.RLock()
	require.Equal(t, 1, mw2.queue.Len())
	mw2.RUnlock()

	require.Equal(t, 1, len(sw.messageWriters))

	md.EXPECT().Finalize(producer.Consumed)
	require.NoError(t, server.Encode(&msgpb.Ack{Metadata: []msgpb.Metadata{msg.Metadata}}))
	// Make sure mw2 is closed and removed from router.
	for {
		router.RLock()
		l := len(router.mws)
		router.RUnlock()
		if l == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	mw2.RLock()
	require.Equal(t, 0, mw2.queue.Len())
	mw2.RUnlock()

	sw.Close()
}

func TestReplicatedShardWriterUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	a := newAckRouter(4)
	opts := testOptions()
	sw := newReplicatedShardWriter(1, 200, a, testMessagePool(opts), opts).(*replicatedShardWriter)
	defer sw.Close()

	cw1 := newConsumerWriter("i1", a, opts)
	cw2 := newConsumerWriter("i2", a, opts)
	cw3 := newConsumerWriter("i3", a, opts)
	cw4 := newConsumerWriter("i4", a, opts)
	cws := make(map[string]consumerWriter)
	cws["i1"] = cw1
	cws["i2"] = cw2
	cws["i3"] = cw3
	cws["i4"] = cw4

	i1 := placement.NewInstance().
		SetEndpoint("i1").
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetCutoffNanos(801).SetCutoverNanos(401)}))
	i2 := placement.NewInstance().
		SetEndpoint("i2").
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetCutoffNanos(802).SetCutoverNanos(402)}))
	i3 := placement.NewInstance().
		SetEndpoint("i3").
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetCutoffNanos(803).SetCutoverNanos(403)}))
	i4 := placement.NewInstance().
		SetEndpoint("i4").
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetCutoffNanos(804).SetCutoverNanos(404)}))

	sw.UpdateInstances([]placement.Instance{i1, i2}, cws)
	require.Equal(t, 2, int(sw.replicaID))
	require.Equal(t, 2, len(sw.messageWriters))
	mw1 := sw.messageWriters[i1.Endpoint()]
	require.NotNil(t, mw1)
	require.Equal(t, 801, int(mw1.CutoffNanos()))
	require.Equal(t, 401, int(mw1.CutoverNanos()))
	require.NotNil(t, sw.messageWriters[i2.Endpoint()])

	sw.UpdateInstances([]placement.Instance{i2, i3}, cws)
	require.Equal(t, 2, int(sw.replicaID))
	require.Equal(t, 2, len(sw.messageWriters))
	mw2 := sw.messageWriters[i2.Endpoint()]
	require.NotNil(t, mw2)
	mw3 := sw.messageWriters[i3.Endpoint()]
	require.NotNil(t, mw3)
	require.Equal(t, mw1, mw3)
	require.Equal(t, 803, int(mw3.CutoffNanos()))
	require.Equal(t, 403, int(mw3.CutoverNanos()))
	m := make(map[uint64]int, 2)
	m[mw2.ReplicatedShardID()] = 1
	m[mw3.ReplicatedShardID()] = 1
	require.Equal(t, map[uint64]int{1: 1, 201: 1}, m)

	sw.UpdateInstances([]placement.Instance{i3}, cws)
	require.Equal(t, 2, int(sw.replicaID))
	require.Equal(t, 1, len(sw.messageWriters))
	require.NotNil(t, sw.messageWriters[i3.Endpoint()])
	for {
		mw2.(*messageWriterImpl).RLock()
		isClosed := mw2.(*messageWriterImpl).isClosed
		mw2.(*messageWriterImpl).RUnlock()
		if isClosed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	sw.UpdateInstances([]placement.Instance{i1, i2, i3}, cws)
	require.Equal(t, 4, int(sw.replicaID))
	require.Equal(t, 3, len(sw.messageWriters))
	newmw1 := sw.messageWriters[i1.Endpoint()]
	require.NotNil(t, newmw1)
	require.NotEqual(t, mw1, newmw1)
	newmw2 := sw.messageWriters[i2.Endpoint()]
	require.NotNil(t, newmw2)
	require.NotEqual(t, mw2, newmw2)
	newmw3 := sw.messageWriters[i3.Endpoint()]
	require.NotNil(t, newmw3)
	require.Equal(t, mw3, newmw3)
	m = make(map[uint64]int, 3)
	m[newmw1.ReplicatedShardID()] = 1
	m[newmw2.ReplicatedShardID()] = 1
	m[newmw3.ReplicatedShardID()] = 1
	require.Equal(t, map[uint64]int{601: 1, 401: 1, mw3.ReplicatedShardID(): 1}, m)

	sw.UpdateInstances([]placement.Instance{i2, i4}, cws)
	require.Equal(t, 4, int(sw.replicaID))
	require.Equal(t, 2, len(sw.messageWriters))
	require.NotNil(t, sw.messageWriters[i2.Endpoint()])
	require.NotNil(t, sw.messageWriters[i4.Endpoint()])

	sw.UpdateInstances([]placement.Instance{i1}, cws)
	require.Equal(t, 4, int(sw.replicaID))
	require.Equal(t, 1, len(sw.messageWriters))
	require.NotNil(t, sw.messageWriters[i1.Endpoint()])
}
