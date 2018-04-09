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
	"go.uber.org/atomic"
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
	ackError          tally.Counter
	decodeError       tally.Counter
	encodeError       tally.Counter
	resetConn         tally.Counter
	resetError        tally.Counter
	connectError      tally.Counter
	setKeepAliveError tally.Counter
}

func newConsumerWriterMetrics(scope tally.Scope) consumerWriterMetrics {
	return consumerWriterMetrics{
		ackError:          scope.Counter("ack-error"),
		decodeError:       scope.Counter("decode-error"),
		encodeError:       scope.Counter("encode-error"),
		resetConn:         scope.Counter("reset-conn"),
		resetError:        scope.Counter("reset-conn-error"),
		connectError:      scope.Counter("connect-error"),
		setKeepAliveError: scope.Counter("set-keep-alive-error"),
	}
}

type connectFn func(addr string) (net.Conn, error)

type consumerWriterImpl struct {
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

	lastResetNanos *atomic.Int64
	closed         *atomic.Bool
	doneCh         chan struct{}
	resetCh        chan struct{}
	wg             sync.WaitGroup
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
		lastResetNanos:  atomic.NewInt64(0),
		closed:          atomic.NewBool(false),
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
		w.m.encodeError.Inc(1)
		w.signalReset()
	}
	return err
}

func (w *consumerWriterImpl) signalReset() {
	// Avoid resetting too frequent.
	if w.nowFn().UnixNano() < w.lastResetNanos.Load()+w.resetDelayNanos {
		return
	}
	select {
	case w.resetCh <- struct{}{}:
	default:
	}
}

func (w *consumerWriterImpl) Init() {
	w.wg.Add(2)
	go w.readAcksForever()
	go w.resetConnectionForever()
}

func (w *consumerWriterImpl) readAcksForever() {
	defer w.wg.Done()

	var acks msgpb.Ack
	for !w.closed.Load() {
		w.decodeLock.Lock()
		err := w.encdec.Decode(&acks)
		w.decodeLock.Unlock()
		if err != nil {
			w.m.decodeError.Inc(1)
			w.signalReset()
			// Adding some delay here to avoid this being retried in a tight loop
			// when underlying connection is misbehaving.
			time.Sleep(w.opts.AckErrorRetryDelay())
			continue
		}
		for _, m := range acks.Metadata {
			if err := w.router.Ack(newMetadataFromProto(m)); err != nil {
				w.m.ackError.Inc(1)
				w.logger.Errorf("could not ack metadata, %v", err)
			}
		}
		// NB(cw) The proto needs to be cleaned up because the gogo protobuf
		// unmarshalling will append to the underlying slice.
		acks.Metadata = acks.Metadata[:0]
	}
}

func (w *consumerWriterImpl) resetConnectionForever() {
	defer w.wg.Done()

	for {
		select {
		case <-w.resetCh:
			w.resetWithConnectFn(w.connectWithRetry)
		case <-w.doneCh:
			return
		}
	}
}

func (w *consumerWriterImpl) resetWithConnectFn(fn connectFn) error {
	w.m.resetConn.Inc(1)
	conn, err := fn(w.addr)
	if err != nil {
		w.m.resetError.Inc(1)
		w.logger.Errorf("could not connect to %s, %v", w.addr, err)
		return err
	}
	w.reset(conn)
	return nil
}

func (w *consumerWriterImpl) connectWithRetry(addr string) (net.Conn, error) {
	continueFn := func(int) bool {
		return !w.closed.Load()
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

func (w *consumerWriterImpl) connectOnce(addr string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, w.opts.DialTimeout())
	if err != nil {
		w.m.connectError.Inc(1)
		return nil, err
	}
	tcpConn := conn.(*net.TCPConn)
	if err = tcpConn.SetKeepAlive(true); err != nil {
		w.m.setKeepAliveError.Inc(1)
	}
	return tcpConn, nil
}

func (w *consumerWriterImpl) reset(conn net.Conn) {
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
	w.lastResetNanos.Store(w.nowFn().UnixNano())
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

func (w *consumerWriterImpl) Close() {
	if !w.closed.CAS(false, true) {
		// Already closed.
		return
	}
	w.encdec.Close()
	close(w.doneCh)
	w.wg.Wait()
}
