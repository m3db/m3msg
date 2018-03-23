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
	"net"
	"testing"

	"github.com/m3db/m3msg/generated/proto/msgpb"

	"github.com/stretchr/testify/require"
)

func TestNewEncodeDecoderWithLock(t *testing.T) {
	c := NewConnectionEncodeDecoder(nil, nil).(*connEncdec)
	require.Equal(t, defaultLock, c.encLock)
	require.Equal(t, defaultLock, c.decLock)
	c = NewConnectionEncodeDecoder(
		nil,
		NewConnectionEncodeDecoderOptions().
			SetEncodeWithLock(true).
			SetDecodeWithLock(true),
	).(*connEncdec)
	require.NotEqual(t, defaultLock, c.encLock)
	require.NotEqual(t, defaultLock, c.decLock)
}

func TestConnectionEncodeDecoderReset(t *testing.T) {
	c := NewConnectionEncodeDecoder(new(net.TCPConn), nil).(*connEncdec)
	c.Close()
	// Safe to close again.
	c.Close()
	require.True(t, c.isClosed)

	c.ResetConn(new(net.TCPConn))
	require.False(t, c.isClosed)
}

func TestConnectionEncodeDecodeRoundTrip(t *testing.T) {
	c := NewConnectionEncodeDecoder(
		nil,
		NewConnectionEncodeDecoderOptions().
			SetEncoderOptions(NewEncodeDecoderOptions().SetBufferSize(1)),
	).(*connEncdec)

	clientConn, serverConn := net.Pipe()
	c.resetWriter(clientConn)
	c.resetReader(serverConn)

	go func() {
		require.NoError(t, c.Encode(&testMsg))
	}()
	var msg msgpb.Message
	require.NoError(t, c.Decode(&msg))
	require.Equal(t, testMsg, msg)
}
