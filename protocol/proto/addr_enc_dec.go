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
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
)

const (
	defaultRetryForever = true
)

var (
	errNoValidConnOrClosed = errors.New("no valid connection or closed")
)

type connectFn func(addr string) (net.Conn, error)

type addrEncdec struct {
	sync.RWMutex

	encdec              ConnectionEncodeDecoder
	addr                string
	validConn           bool
	resetCh             chan struct{}
	resetNanos          int64
	retrier             retry.Retrier
	dialTimeout         time.Duration
	reconnectDelayNanos int64
	doneCh              chan struct{}
	closed              bool
	logger              log.Logger
	m                   *addrEncdecMetrics

	nowFn     clock.NowFn
	connectFn connectFn
}

type addrEncdecMetrics struct {
	resetConn      tally.Counter
	resetConnError tally.Counter
}

// NewAddressEncodeDecoder creates an EncodeDecoder that
// manages the connection to the target address.
func NewAddressEncodeDecoder(
	addr string,
	opts AddressEncodeDecoderOptions,
) AddressEncodeDecoder {
	if opts == nil {
		opts = NewAddressEncodeDecoderOptions()
	}
	c := addrEncdec{
		encdec:     NewConnectionEncodeDecoder(nil, opts.ConnectionEncodeDecoderOptions()),
		addr:       addr,
		validConn:  false,
		resetCh:    make(chan struct{}, 1),
		resetNanos: time.Now().UnixNano(),
		// Should retry forever on reconnect.
		retrier:             retry.NewRetrier(opts.ConnectionRetryOptions().SetForever(defaultRetryForever)),
		dialTimeout:         opts.DialTimeout(),
		reconnectDelayNanos: opts.ReconnectDelay().Nanoseconds(),
		doneCh:              make(chan struct{}),
		closed:              false,
		logger:              opts.InstrumentOptions().Logger(),
		m:                   newAddrEncdecMetrics(opts.InstrumentOptions().MetricsScope()),
		nowFn:               time.Now,
	}

	c.connectFn = c.connectOnce
	if err := c.resetConn(c.connectFn); err != nil {
		c.signalReset()
	}
	return &c
}

func newAddrEncdecMetrics(m tally.Scope) *addrEncdecMetrics {
	return &addrEncdecMetrics{
		resetConn:      m.Counter("reset-conn"),
		resetConnError: m.Counter("reset-conn-error"),
	}
}

func (c *addrEncdec) Encode(msg Marshaler) error {
	c.RLock()
	if !c.canEncodeDecodeWithLock() {
		c.RUnlock()
		return errNoValidConnOrClosed
	}
	if err := c.encdec.Encode(msg); err != nil {
		c.RUnlock()
		c.signalInvalidConnection()
		return err
	}
	c.RUnlock()
	return nil
}

func (c *addrEncdec) Decode(acks Unmarshaler) error {
	c.RLock()
	if !c.canEncodeDecodeWithLock() {
		c.RUnlock()
		return errNoValidConnOrClosed
	}
	if err := c.encdec.Decode(acks); err != nil {
		c.RUnlock()
		c.signalInvalidConnection()
		return err
	}
	c.RUnlock()
	return nil
}

func (c *addrEncdec) Init() {
	go c.resetConnection()
}

func (c *addrEncdec) Close() {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return
	}
	c.closed = true
	c.validConn = false
	c.encdec.Close()
	close(c.doneCh)
}

func (c *addrEncdec) resetConnection() {
	for {
		select {
		case <-c.resetCh:
			c.m.resetConn.Inc(1)
			c.resetConn(c.connectWithRetryForever)
		case <-c.doneCh:
			return
		}
	}
}

// TODO(cw) Not exposing this function right now since we don't really need to poll the addr encdec.
// ResetAddr resets the address.
func (c *addrEncdec) ResetAddr(addr string) {
	c.addr = addr
	c.resetNanos = 0
	c.validConn = false
	c.doneCh = make(chan struct{})
	c.closed = false
	c.cleanUpResetCh()
	if err := c.resetConn(c.connectFn); err != nil {
		c.signalReset()
	}
}

func (c *addrEncdec) cleanUpResetCh() {
	for {
		select {
		case <-c.resetCh:
		default:
			return
		}
	}
}

func (c *addrEncdec) resetConn(fn connectFn) error {
	conn, err := fn(c.addr)
	if err != nil {
		c.logger.Errorf("could not connect to %s, %v", c.addr, err)
		c.m.resetConnError.Inc(1)
		return err
	}
	c.encdec.ResetConn(conn)
	c.Lock()
	c.validConn = true
	c.Unlock()
	return nil
}

func (c *addrEncdec) signalInvalidConnection() {
	c.Lock()
	// Delay between signals
	if c.resetNanos > 0 && c.nowFn().UnixNano() < c.resetNanos+c.reconnectDelayNanos {
		c.Unlock()
		return
	}
	c.validConn = false
	c.resetNanos = c.nowFn().UnixNano()
	c.signalReset()
	c.Unlock()
}

func (c *addrEncdec) signalReset() {
	select {
	case c.resetCh <- struct{}{}:
	default:
	}
}

func (c *addrEncdec) connectOnce(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, c.dialTimeout)
}

func (c *addrEncdec) connectWithRetryForever(addr string) (net.Conn, error) {
	continueFn := func(int) bool {
		return !c.isClosed()
	}
	var (
		conn net.Conn
		err  error
	)
	fn := func() error {
		conn, err = c.connectFn(addr)
		return err
	}
	if attemptErr := c.retrier.AttemptWhile(
		continueFn,
		fn,
	); attemptErr != nil {
		return nil, fmt.Errorf("connectWithRetry failed: %v", attemptErr)
	}
	return conn, nil
}

func (c *addrEncdec) canEncodeDecodeWithLock() bool {
	return c.validConn && !c.closed
}

func (c *addrEncdec) isClosed() bool {
	c.RLock()
	res := c.closed
	c.RUnlock()
	return res
}
