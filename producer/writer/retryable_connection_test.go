// Copyright (c) 2018 Uber Technologies, In
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

	"github.com/stretchr/testify/require"
)

func TestSignalResetConnection(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	w := newRetryableConnection(
		lis.Addr().String(),
		testOptions().SetConnectionResetDelay(time.Minute),
	)
	require.Equal(t, 0, len(w.resetCh))

	w.signalResetWithLock()
	require.Equal(t, 0, len(w.resetCh))

	now := time.Now()
	w.nowFn = func() time.Time { return now.Add(1 * time.Hour) }
	w.signalResetWithLock()
	require.Equal(t, 1, len(w.resetCh))

	w.nowFn = func() time.Time { return now.Add(2 * time.Hour) }
	w.signalResetWithLock()
	require.Equal(t, 1, len(w.resetCh))
}

func TestResetConnection(t *testing.T) {
	w := newRetryableConnection("badAddress", testOptions())
	require.Equal(t, 1, len(w.resetCh))
	_, err := w.Write([]byte("foo"))
	require.Error(t, err)
	require.Equal(t, errConnNotInitialized, err)

	var called int
	conn := new(net.TCPConn)
	w.connectFn = func(addr string) (net.Conn, error) {
		called++
		require.Equal(t, "badAddress", addr)
		return conn, nil
	}
	w.resetWithConnectFn(w.connectWithRetry)
	require.Equal(t, 1, called)
}

func TestWriteErrorReset(t *testing.T) {
	w := newRetryableConnection("badAddress", testOptions())

	w.reset(new(net.TCPConn))
	require.Equal(t, 0, len(w.resetCh))

	now := time.Now()
	w.nowFn = func() time.Time { return now.Add(1 * time.Hour) }
	_, err := w.Write([]byte("foo"))
	require.Error(t, err)
	require.Equal(t, 1, len(w.resetCh))
}

func TestReadErrorReset(t *testing.T) {
	w := newRetryableConnection("badAddress", testOptions())

	w.reset(new(net.TCPConn))
	require.Equal(t, 0, len(w.resetCh))

	now := time.Now()
	w.nowFn = func() time.Time { return now.Add(1 * time.Hour) }
	_, err := w.Read([]byte("foo"))
	require.Error(t, err)
	require.Equal(t, 1, len(w.resetCh))
}
