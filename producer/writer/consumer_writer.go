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
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
)

const (
	defaultRetryForever = true
)

var (
	defaultConn = new(net.TCPConn)
)

type consumerWriter interface {
	// Write writes the marshaler, it should be thread safe.
	Write(m proto.Marshaler) error

	// Init initializes the consumer writer.
	Init()

	// Close closes the consumer writer.
	Close()
}

type consumerWriterMetrics struct {
	ackReadErr        tally.Counter
	resetConn         tally.Counter
	resetConnError    tally.Counter
	connectError      tally.Counter
	setKeepAliveError tally.Counter
}

func newConsumerWriterMetrics(scope tally.Scope) consumerWriterMetrics {
	return consumerWriterMetrics{
		ackReadErr:        scope.Counter("ack-read-error"),
		resetConn:         scope.Counter("reset-conn"),
		resetConnError:    scope.Counter("reset-conn-error"),
		connectError:      scope.Counter("connect-error"),
		setKeepAliveError: scope.Counter("set-keep-alive-error"),
	}
}

type connectFn func(addr string) (net.Conn, error)

type consumerWriterImpl struct {
	sync.RWMutex

	// encodeLock controls the access to the encode function.
	encodeLock sync.Mutex
	// decodeLock controls the access to the decode function.
	decodeLock sync.Mutex
	encdec     proto.EncodeDecoder

	addr            string
	retrier         retry.Retrier
	router          ackRouter
	opts            Options
	resetDelayNanos int64
	logger          log.Logger

	lastResetNanos int64
	closed         bool
	doneCh         chan struct{}
	resetCh        chan struct{}
	closeWG        sync.WaitGroup
	m              consumerWriterMetrics

	nowFn     clock.NowFn
	connectFn connectFn
}

// TODO: Remove the nolint comment after adding usage of this function.
// nolint: deadcode
func newConsumerWriter(
	addr string,
	router ackRouter,
	opts Options,
) consumerWriter {
	if opts == nil {
		opts = NewOptions()
	}
	w := &consumerWriterImpl{
		encdec:          proto.NewEncodeDecoder(defaultConn, opts.EncodeDecoderOptions()),
		addr:            addr,
		retrier:         retry.NewRetrier(opts.ConnectionRetryOptions().SetForever(defaultRetryForever)),
		router:          router,
		opts:            opts,
		resetDelayNanos: int64(opts.ConnectionResetDelay()),
		logger:          opts.InstrumentOptions().Logger(),
		lastResetNanos:  0,
		closed:          false,
		doneCh:          make(chan struct{}),
		resetCh:         make(chan struct{}, 1),
		m:               newConsumerWriterMetrics(opts.InstrumentOptions().MetricsScope()),
		nowFn:           time.Now,
	}

	w.connectFn = w.connectOnce
	if err := w.resetWithConnectFn(w.connectFn); err != nil {
		w.signalReset()
	}
	return w
}

// Write should fail fast so that the write could be tried on other
// consumer writers that are sharing the data.
func (w *consumerWriterImpl) Write(m proto.Marshaler) error {
	w.encodeLock.Lock()
	err := w.encdec.Encode(m)
	w.encodeLock.Unlock()
	if err != nil {
		w.signalReset()
	}
	return err
}

func (w *consumerWriterImpl) signalReset() {
	w.RLock()
	lastResetNanos := w.lastResetNanos
	w.RUnlock()
	if w.nowFn().UnixNano() < lastResetNanos+w.resetDelayNanos {
		return
	}
	select {
	case w.resetCh <- struct{}{}:
	default:
	}
}

func (w *consumerWriterImpl) Init() {
	w.closeWG.Add(2)
	go w.resetConnectionForever()
	go w.readAcksForever()
}

func (w *consumerWriterImpl) resetConnectionForever() {
	defer w.closeWG.Done()

	for {
		select {
		case <-w.resetCh:
			w.resetWithConnectFn(w.connectWithRetryForever)
		case <-w.doneCh:
			return
		}
	}
}

func (w *consumerWriterImpl) resetWithConnectFn(fn connectFn) error {
	w.m.resetConn.Inc(1)
	conn, err := fn(w.addr)
	if err != nil {
		w.logger.Errorf("could not connect to %s, %v", w.addr, err)
		w.m.resetConnError.Inc(1)
		return err
	}
	w.reset(conn)
	return nil
}

func (w *consumerWriterImpl) reset(conn net.Conn) {
	w.Lock()
	defer w.Unlock()
	// Close wakes up any blocking encode/decode function on the encodeDecoder.
	// We are doing this mostly for the decode function.
	// After which we could acquire the encode lock and decode lock.
	w.encdec.Close()

	w.decodeLock.Lock()
	defer w.decodeLock.Unlock()
	w.encodeLock.Lock()
	defer w.encodeLock.Unlock()

	// NB(cw): Must set the conn with both encode lock and decode lock
	// to avoid data race.
	w.encdec.Reset(conn)
	w.lastResetNanos = w.nowFn().UnixNano()
	w.cleanUpResetChannel()
}

func (w *consumerWriterImpl) cleanUpResetChannel() {
	for {
		select {
		case <-w.resetCh:
		default:
			return
		}
	}
}

func (w *consumerWriterImpl) readAcksForever() {
	defer w.closeWG.Done()

	var acks msgpb.Ack
	for !w.isClosed() {
		w.decodeLock.Lock()
		err := w.encdec.Decode(&acks)
		w.decodeLock.Unlock()
		if err != nil {
			w.signalReset()
			w.m.ackReadErr.Inc(1)
			// Adding some delay here to avoid this being retried in a tight loop
			// when underlying connection is misbehaving.
			time.Sleep(w.opts.AckErrorRetryDelay())
			continue
		}
		for _, m := range acks.Metadata {
			if err := w.router.Ack(metadataFromProto(m)); err != nil {
				w.logger.Errorf("could not ack metadata, %v", err)
			}
		}
		// NB(cw) The proto needs to be cleaned up because the gogo protobuf
		// unmarshalling will append to the underlying slice.
		acks.Metadata = acks.Metadata[:0]
	}
}

func (w *consumerWriterImpl) Close() {
	w.Lock()
	if w.closed {
		w.Unlock()
		return
	}
	w.closed = true
	w.encdec.Close()
	w.Unlock()

	close(w.doneCh)
	w.closeWG.Wait()
}

func (w *consumerWriterImpl) connectOnce(addr string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, w.opts.DialTimeout())
	if err != nil {
		w.m.resetConnError.Inc(1)
		return nil, err
	}
	tcpConn := conn.(*net.TCPConn)
	if err = tcpConn.SetKeepAlive(true); err != nil {
		w.m.setKeepAliveError.Inc(1)
	}
	return tcpConn, nil
}

func (w *consumerWriterImpl) connectWithRetryForever(addr string) (net.Conn, error) {
	continueFn := func(int) bool {
		return !w.isClosed()
	}
	var (
		conn net.Conn
		err  error
	)
	fn := func() error {
		conn, err = w.connectFn(addr)
		return err
	}
	if attemptErr := w.retrier.AttemptWhile(
		continueFn,
		fn,
	); attemptErr != nil {
		return nil, fmt.Errorf("failed to connect to address %s, %v", addr, attemptErr)
	}
	return conn, nil
}

func (w *consumerWriterImpl) isClosed() bool {
	w.RLock()
	res := w.closed
	w.RUnlock()
	return res
}

func metadataFromProto(m *msgpb.Metadata) metadata {
	return metadata{
		shard: m.Shard,
		id:    m.Id,
	}
}
