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
)

type encdec struct {
	conn     net.Conn
	enc      *encoder
	dec      *decoder
	isClosed bool
	pool     EncodeDecoderPool
}

// NewEncodeDecoder creates an EncodeDecoder.
func NewEncodeDecoder(
	conn net.Conn,
	opts EncodeDecoderOptions,
) EncodeDecoder {
	if opts == nil {
		opts = NewEncodeDecoderOptions()
	}
	c := encdec{
		conn:     conn,
		enc:      newEncoder(conn, opts.EncoderOptions()),
		dec:      newDecoder(conn, opts.DecoderOptions()),
		isClosed: false,
		pool:     opts.EncodeDecoderPool(),
	}
	return &c
}

func (c *encdec) Encode(msg Marshaler) error {
	err := c.enc.Encode(msg)
	return err
}

func (c *encdec) Decode(acks Unmarshaler) error {
	err := c.dec.Decode(acks)
	return err
}

func (c *encdec) Close() {
	if c.isClosed {
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
}

func (c *encdec) Reset(conn net.Conn) {
	if c.conn != nil {
		c.conn.Close()
	}
	c.enc.resetWriter(conn)
	c.dec.resetReader(conn)
	c.conn = conn
	c.isClosed = false
}
