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
	"io"
	"net"
	"sync"
)

var (
	defaultLock noopLock
)

type connEncdec struct {
	sync.Mutex

	conn     net.Conn
	encLock  sync.Locker
	enc      *encoder
	decLock  sync.Locker
	dec      *decoder
	isClosed bool
	pool     ConnectionEncodeDecoderPool
}

// NewConnectionEncodeDecoder creates an EncodeDecoder on a connection.
func NewConnectionEncodeDecoder(
	conn net.Conn,
	opts ConnectionEncodeDecoderOptions,
) ConnectionEncodeDecoder {
	if opts == nil {
		opts = NewConnectionEncodeDecoderOptions()
	}
	c := connEncdec{
		encLock: defaultLock,
		enc:     newEncoder(conn, opts.EncoderOptions()),
		decLock: defaultLock,
		dec:     newDecoder(conn, opts.DecoderOptions()),
		pool:    opts.ConnectionEncodeDecoderPool(),
	}
	if opts.EncodeWithLock() {
		c.encLock = new(sync.Mutex)
	}
	if opts.DecodeWithLock() {
		c.decLock = new(sync.Mutex)
	}
	return &c
}

func (c *connEncdec) Encode(msg Marshaler) error {
	c.encLock.Lock()
	err := c.enc.Encode(msg)
	c.encLock.Unlock()
	return err
}

func (c *connEncdec) Decode(acks Unmarshaler) error {
	c.decLock.Lock()
	err := c.dec.Decode(acks)
	c.decLock.Unlock()
	return err
}

func (c *connEncdec) Close() {
	c.Lock()
	if c.isClosed {
		c.Unlock()
		return
	}
	c.isClosed = true
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = nil
	if c.pool != nil {
		c.pool.Put(c)
	}
	c.Unlock()
}

func (c *connEncdec) ResetConn(conn net.Conn) {
	c.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.resetWriter(conn)
	c.resetReader(conn)
	c.conn = conn
	c.isClosed = false
	c.Unlock()
}

func (c *connEncdec) resetWriter(w io.Writer) {
	c.encLock.Lock()
	c.enc.resetWriter(w)
	c.encLock.Unlock()
}

func (c *connEncdec) resetReader(r io.Reader) {
	c.decLock.Lock()
	c.dec.resetReader(r)
	c.decLock.Unlock()
}

type noopLock struct{}

func (l noopLock) Lock()   {}
func (l noopLock) Unlock() {}
