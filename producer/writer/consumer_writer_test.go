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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3x/pool"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testMsg = msgpb.Message{
	Metadata: &msgpb.Metadata{
		Shard: 100,
		Id:    200,
	},
	Value: []byte("foooooooo"),
}

func TestNewConsumerWriter(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRouter := NewMockackRouter(ctrl)
	opts := testOptions()
	w := newConsumerWriter(lis.Addr().String(), mockRouter, opts).(*consumerWriterImpl)
	require.Equal(t, 0, len(w.resetCh))

	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncodeDecoderOptions())
	}()

	require.NoError(t, w.Write(&testMsg))

	var wg sync.WaitGroup
	wg.Add(1)
	mockRouter.EXPECT().
		Ack(metadataFromProto(testMsg.Metadata)).
		Do(func(interface{}) { wg.Done() }).
		Return(nil)

	w.Init()
	wg.Wait()

	w.Close()
}

func TestNewConsumerWriterWithInvalidAddr(t *testing.T) {
	w := newConsumerWriter("badAddress", nil, nil).(*consumerWriterImpl)
	require.Equal(t, 1, len(w.resetCh))
}

func TestSignalResetConnection(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	w := newConsumerWriter(lis.Addr().String(), nil, testOptions()).(*consumerWriterImpl)
	require.Equal(t, 0, len(w.resetCh))

	w.signalReset()
	require.Equal(t, 0, len(w.resetCh))

	now := time.Now()
	w.nowFn = func() time.Time { return now.Add(1 * time.Hour) }
	w.signalReset()
	require.Equal(t, 1, len(w.resetCh))

	w.nowFn = func() time.Time { return now.Add(2 * time.Hour) }
	w.signalReset()
	require.Equal(t, 1, len(w.resetCh))
}

func TestResetConnection(t *testing.T) {
	w := newConsumerWriter(
		"badAddress",
		nil,
		testOptions(),
	).(*consumerWriterImpl)
	require.Error(t, w.Write(&testMsg))

	var called int
	conn := new(net.TCPConn)
	w.connectFn = func(addr string) (net.Conn, error) {
		called++
		require.Equal(t, "badAddress", addr)
		return conn, nil
	}
	w.resetWithConnectFn(w.connectWithRetryForever)
	require.Equal(t, 1, called)
}

func TestWriteErrorReset(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	opts := testOptions()
	w := newConsumerWriter(lis.Addr().String(), nil, opts).(*consumerWriterImpl)

	w.encdec.Reset(new(net.TCPConn))
	require.Equal(t, 0, len(w.resetCh))

	now := time.Now()
	w.nowFn = func() time.Time { return now.Add(1 * time.Hour) }
	require.Error(t, w.Write(&testMsg))
	require.Equal(t, 1, len(w.resetCh))

	clientConn, serverConn := net.Pipe()
	go func() {
		server := proto.NewEncodeDecoder(
			serverConn,
			opts.EncodeDecoderOptions(),
		)
		var msg msgpb.Message
		require.NoError(t, server.Decode(&msg))
		require.Equal(t, testMsg, msg)
		require.NoError(t, server.Encode(&testMsg))
	}()

	var called int
	w.connectFn = func(addr string) (net.Conn, error) {
		called++
		return clientConn, nil
	}
	w.resetWithConnectFn(w.connectWithRetryForever)

	require.NoError(t, w.Write(&testMsg))
	var msg msgpb.Message
	require.NoError(t, w.encdec.Decode(&msg))
	require.Equal(t, testMsg, msg)
}

func TestAutoReset(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRouter := NewMockackRouter(ctrl)
	opts := testOptions()
	w := newConsumerWriter(
		"badAddress",
		mockRouter,
		opts,
	).(*consumerWriterImpl)
	require.Equal(t, 1, len(w.resetCh))
	require.Error(t, w.Write(&testMsg))

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go func() {
		testConsumeAndAckOnConnection(t, serverConn, opts.EncodeDecoderOptions())
	}()

	w.connectFn = func(addr string) (net.Conn, error) {
		return clientConn, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	mockRouter.EXPECT().
		Ack(metadataFromProto(testMsg.Metadata)).
		Do(func(interface{}) { wg.Done() }).
		Return(nil)

	w.Init()

	for {
		w.Lock()
		l := len(w.resetCh)
		w.Unlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, w.Write(&testMsg))
	wg.Wait()

	w.Close()
}

func TestConsumerWriterClose(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	w := newConsumerWriter(lis.Addr().String(), nil, nil).(*consumerWriterImpl)
	require.Equal(t, 0, len(w.resetCh))
	w.Close()
	// Safe to close again.
	w.Close()
	_, ok := <-w.doneCh
	require.False(t, ok)
}

func TestConsumerWriterCloseWhileDecoding(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	opts := testOptions()
	w := newConsumerWriter(lis.Addr().String(), nil, opts).(*consumerWriterImpl)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		w.decodeLock.Lock()
		wg.Done()
		require.Error(t, w.encdec.Decode(&testMsg))
		w.decodeLock.Unlock()
	}()
	wg.Wait()
	w.Close()
}

func TestConsumerWriterResetWhileDecoding(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	opts := testOptions()
	w := newConsumerWriter(lis.Addr().String(), nil, opts).(*consumerWriterImpl)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		w.decodeLock.Lock()
		wg.Done()
		require.Error(t, w.encdec.Decode(&testMsg))
		w.decodeLock.Unlock()
	}()
	wg.Wait()
	w.reset(defaultConn)
}

func testOptions() Options {
	connOpts := proto.NewEncodeDecoderOptions().
		SetEncoderOptions(proto.NewBaseOptions().SetBufferSize(1))

	return NewOptions().
		SetTopicName("topicName").
		SetTopicWatchInitTimeout(100 * time.Millisecond).
		SetMessagePoolOptions(pool.NewObjectPoolOptions().SetSize(1)).
		SetAckErrorRetryDelay(100 * time.Millisecond).
		SetMessageRetryBackoff(100 * time.Millisecond).
		SetPlacementWatchInitTimeout(100 * time.Millisecond).
		SetEncodeDecoderOptions(connOpts).
		SetCloseCheckInterval(100 * time.Microsecond).
		SetConnectionResetDelay(100 * time.Millisecond)
}

func testConsumeAndAckOnConnection(t *testing.T, conn net.Conn, opts proto.EncodeDecoderOptions) {
	server := proto.NewEncodeDecoder(
		conn,
		opts,
	)

	var msg msgpb.Message
	assert.NoError(t, server.Decode(&msg))

	assert.NoError(t, server.Encode(&msgpb.Ack{
		Metadata: []*msgpb.Metadata{
			msg.Metadata,
		},
	}))
}

func testConsumeAndAckOnConnectionListener(t *testing.T, lis net.Listener, opts proto.EncodeDecoderOptions) {
	conn, err := lis.Accept()
	require.NoError(t, err)
	defer conn.Close()

	testConsumeAndAckOnConnection(t, conn, opts)
}
