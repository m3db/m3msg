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

package proto

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3msg/generated/proto/msgpb"

	"github.com/stretchr/testify/require"
)

var (
	testMsg msgpb.Message
)

func TestNewAddressEncodeDecoderWithInvalidAddr(t *testing.T) {
	c := NewAddressEncodeDecoder("badAddress", nil).(*addrEncdec)
	require.False(t, c.hasValidConnection())
	require.Equal(t, 1, len(c.resetCh))
}

func TestNewAddressEncodeDecoderWithValidAddr(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	c := NewAddressEncodeDecoder(lis.Addr().String(), nil).(*addrEncdec)
	require.True(t, c.hasValidConnection())
	require.Equal(t, 0, len(c.resetCh))
}

func TestAddressEncodeDecodeWithNoValidConnection(t *testing.T) {
	c := NewAddressEncodeDecoder("badAddress", nil).(*addrEncdec)
	require.False(t, c.hasValidConnection())

	err := c.Encode(&testMsg)
	require.Error(t, err)
	require.Equal(t, errNoValidConnection, err)

	err = c.Decode(&testMsg)
	require.Error(t, err)
	require.Equal(t, errNoValidConnection, err)
}

func TestSignalResetConnection(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	c := NewAddressEncodeDecoder(lis.Addr().String(), NewAddressEncodeDecoderOptions().SetReconnectDelay(0)).(*addrEncdec)
	require.True(t, c.hasValidConnection())
	require.NoError(t, c.Encode(&testMsg))

	c.signalInvalidConnection()
	require.False(t, c.hasValidConnection())
	require.Error(t, c.Encode(&testMsg))
	require.Equal(t, 1, len(c.resetCh))

	c.signalInvalidConnection()
	require.False(t, c.hasValidConnection())
	require.Equal(t, 1, len(c.resetCh))

	c.reconnectDelayNanos = int64(time.Hour)
	c.signalInvalidConnection()
	require.False(t, c.hasValidConnection())
	require.Equal(t, 1, len(c.resetCh))
}

func TestNoResetBeforeDelay(t *testing.T) {
	c := NewAddressEncodeDecoder(
		"badAddress",
		NewAddressEncodeDecoderOptions().
			SetReconnectDelay(time.Hour),
	).(*addrEncdec)
	require.False(t, c.hasValidConnection())
	require.Error(t, c.Encode(&testMsg))
	require.Equal(t, 1, len(c.resetCh))
	<-c.resetCh
	require.Equal(t, 0, len(c.resetCh))
	var called int
	c.connectFn = func(addr string) (net.Conn, error) {
		called++
		return nil, nil
	}
	c.signalInvalidConnection()
	require.Equal(t, 0, len(c.resetCh))
	require.Equal(t, 0, called)
}

func TestResetConnectionAfterDelay(t *testing.T) {
	c := NewAddressEncodeDecoder(
		"badAddress",
		NewAddressEncodeDecoderOptions().
			SetReconnectDelay(0),
	).(*addrEncdec)
	require.False(t, c.hasValidConnection())
	require.Error(t, c.Encode(&testMsg))
	var called int
	conn := new(net.TCPConn)
	c.connectFn = func(addr string) (net.Conn, error) {
		called++
		require.Equal(t, "badAddress", addr)
		return conn, nil
	}
	c.resetConn(c.connectWithRetryForever)
	require.Equal(t, 1, called)
	require.Equal(t, c.encdec.(*connEncdec).conn, conn)

	require.NoError(t, c.Encode(&testMsg))
}

func TestEncodeErrorReset(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	c := NewAddressEncodeDecoder(
		lis.Addr().String(),
		NewAddressEncodeDecoderOptions().
			SetReconnectDelay(0).
			SetConnectionEncodeDecoderOptions(
				NewConnectionEncodeDecoderOptions().SetEncoderOptions(NewEncodeDecoderOptions().SetBufferSize(1)),
			),
	).(*addrEncdec)
	require.True(t, c.hasValidConnection())

	c.encdec.(*connEncdec).resetWriter(errWriter{})
	require.Error(t, c.Encode(&testMsg))
	require.False(t, c.hasValidConnection())

	clientConn, serverConn := net.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		server := NewConnectionEncodeDecoder(
			serverConn,
			NewConnectionEncodeDecoderOptions().
				SetEncoderOptions(NewEncodeDecoderOptions().SetBufferSize(1)),
		)
		var msg msgpb.Message
		require.NoError(t, server.Decode(&msg))
		require.Equal(t, testMsg, msg)
		require.NoError(t, server.Encode(&testMsg))
		wg.Done()
	}()

	c.connectFn = func(addr string) (net.Conn, error) {
		return clientConn, nil
	}
	c.resetConn(c.connectWithRetryForever)
	require.Equal(t, c.encdec.(*connEncdec).conn, clientConn)

	require.NoError(t, c.Encode(&testMsg))
	c.encdec.(*connEncdec).enc.w.Flush()
	var msg msgpb.Message
	require.NoError(t, c.Decode(&msg))
	require.Equal(t, testMsg, msg)
	wg.Wait()
}

func TestDecodeErrorReset(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	c := NewAddressEncodeDecoder(
		lis.Addr().String(),
		NewAddressEncodeDecoderOptions().
			SetReconnectDelay(0).
			SetConnectionEncodeDecoderOptions(
				NewConnectionEncodeDecoderOptions().SetEncoderOptions(NewEncodeDecoderOptions().SetBufferSize(1)),
			),
	).(*addrEncdec)
	require.True(t, c.hasValidConnection())

	c.encdec.(*connEncdec).resetReader(errReader{})
	require.Error(t, c.Decode(&testMsg))
	require.False(t, c.hasValidConnection())

	clientConn, serverConn := net.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		server := NewConnectionEncodeDecoder(
			serverConn,
			NewConnectionEncodeDecoderOptions().
				SetEncoderOptions(NewEncodeDecoderOptions().SetBufferSize(1)),
		)
		var msg msgpb.Message
		require.NoError(t, server.Decode(&msg))
		require.Equal(t, testMsg, msg)
		require.NoError(t, server.Encode(&testMsg))
		wg.Done()
	}()

	c.connectFn = func(addr string) (net.Conn, error) {
		return clientConn, nil
	}
	c.resetConn(c.connectWithRetryForever)
	require.Equal(t, c.encdec.(*connEncdec).conn, clientConn)

	require.NoError(t, c.Encode(&testMsg))
	var msg msgpb.Message
	require.NoError(t, c.Decode(&msg))
	require.Equal(t, testMsg, msg)
	wg.Wait()
}

func TestAutoReset(t *testing.T) {
	c := NewAddressEncodeDecoder(
		"badAddress",
		NewAddressEncodeDecoderOptions().
			SetReconnectDelay(0).
			SetConnectionEncodeDecoderOptions(
				NewConnectionEncodeDecoderOptions().SetEncoderOptions(NewEncodeDecoderOptions().SetBufferSize(1)),
			),
	).(*addrEncdec)
	require.False(t, c.hasValidConnection())
	require.Equal(t, 1, len(c.resetCh))

	clientConn, serverConn := net.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		server := NewConnectionEncodeDecoder(
			serverConn,
			NewConnectionEncodeDecoderOptions().
				SetEncoderOptions(NewEncodeDecoderOptions().SetBufferSize(1)),
		)
		var msg msgpb.Message
		require.NoError(t, server.Decode(&msg))
		require.Equal(t, testMsg, msg)
		require.NoError(t, server.Encode(&testMsg))
		wg.Done()
	}()

	var called int
	c.connectFn = func(addr string) (net.Conn, error) {
		called++
		return clientConn, nil
	}
	c.Init()

	for !c.hasValidConnection() {
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, c.hasValidConnection())
	require.Equal(t, 0, len(c.resetCh))
	require.Equal(t, 1, called)
	require.Equal(t, c.encdec.(*connEncdec).conn, clientConn)

	require.NoError(t, c.Encode(&testMsg))
	var msg msgpb.Message
	require.NoError(t, c.Decode(&msg))
	require.Equal(t, testMsg, msg)
	wg.Wait()
}

func TestAddressEncodeDecoderClose(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	c := NewAddressEncodeDecoder(lis.Addr().String(), nil).(*addrEncdec)
	require.True(t, c.hasValidConnection())
	c.Close()
	// Safe to close again
	c.Close()
	require.False(t, c.hasValidConnection())
	_, ok := <-c.doneCh
	require.False(t, ok)
}

func TestAddressEncodeDecoderReset(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	c := NewAddressEncodeDecoder(lis.Addr().String(), nil).(*addrEncdec)
	require.True(t, c.hasValidConnection())

	addr1 := "badAddr"
	c.ResetAddr(addr1)
	require.Equal(t, addr1, c.addr)
	require.False(t, c.hasValidConnection())
	select {
	case <-c.doneCh:
		require.FailNow(t, "not expected")
	default:
	}
	require.Equal(t, 1, len(c.resetCh))

	c.Close()
	require.True(t, c.isClosed())

	c.ResetAddr(lis.Addr().String())
	require.False(t, c.isClosed())
	require.Equal(t, lis.Addr().String(), c.addr)
	require.True(t, c.hasValidConnection())
	select {
	case <-c.doneCh:
		require.FailNow(t, "not expected")
	default:
	}
	require.Equal(t, 0, len(c.resetCh))
}

type errWriter struct{}

func (w errWriter) Write(p []byte) (int, error) {
	return 2, errors.New("mock err")
}

type errReader struct{}

func (r errReader) Read(p []byte) (int, error) {
	return 0, errors.New("mock err")
}
