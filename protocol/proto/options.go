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
	"time"

	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
)

const (
	defaultDataBufferSize = 16384
	defaultDialTimeout    = 10 * time.Second
	defaultReconnectDelay = 5 * time.Second
)

// NewEncodeDecoderOptions creates a new EncodeDecoderOptions.
func NewEncodeDecoderOptions() EncodeDecoderOptions {
	return &encdecOptions{
		bufferSize: defaultDataBufferSize,
	}
}

type encdecOptions struct {
	bytesPool  pool.BytesPool
	bufferSize int
}

func (opts *encdecOptions) BytesPool() pool.BytesPool {
	return opts.bytesPool
}

func (opts *encdecOptions) SetBytesPool(value pool.BytesPool) EncodeDecoderOptions {
	o := *opts
	o.bytesPool = value
	return &o
}

func (opts *encdecOptions) BufferSize() int {
	return opts.bufferSize
}

func (opts *encdecOptions) SetBufferSize(value int) EncodeDecoderOptions {
	o := *opts
	o.bufferSize = value
	return &o
}

// NewConnectionEncodeDecoderOptions creates a ConnectionEncodeDecoderOptions.
func NewConnectionEncodeDecoderOptions() ConnectionEncodeDecoderOptions {
	return &connOptions{
		enableWriteLock: false,
		enableReadLock:  false,
		encOpts:         NewEncodeDecoderOptions(),
		decOpts:         NewEncodeDecoderOptions(),
	}
}

type connOptions struct {
	enableWriteLock bool
	enableReadLock  bool
	encOpts         EncodeDecoderOptions
	decOpts         EncodeDecoderOptions
	pool            ConnectionEncodeDecoderPool
}

func (opts *connOptions) EncodeWithLock() bool {
	return opts.enableWriteLock
}

func (opts *connOptions) SetEncodeWithLock(value bool) ConnectionEncodeDecoderOptions {
	o := *opts
	o.enableWriteLock = value
	return &o
}

func (opts *connOptions) DecodeWithLock() bool {
	return opts.enableReadLock
}

func (opts *connOptions) SetDecodeWithLock(value bool) ConnectionEncodeDecoderOptions {
	o := *opts
	o.enableReadLock = value
	return &o
}

func (opts *connOptions) EncoderOptions() EncodeDecoderOptions {
	return opts.encOpts
}

func (opts *connOptions) SetEncoderOptions(value EncodeDecoderOptions) ConnectionEncodeDecoderOptions {
	o := *opts
	o.encOpts = value
	return &o
}

func (opts *connOptions) DecoderOptions() EncodeDecoderOptions {
	return opts.decOpts
}

func (opts *connOptions) SetDecoderOptions(value EncodeDecoderOptions) ConnectionEncodeDecoderOptions {
	o := *opts
	o.decOpts = value
	return &o
}

func (opts *connOptions) ConnectionEncodeDecoderPool() ConnectionEncodeDecoderPool {
	return opts.pool
}

func (opts *connOptions) SetConnectionEncodeDecoderPool(pool ConnectionEncodeDecoderPool) ConnectionEncodeDecoderOptions {
	o := *opts
	o.pool = pool
	return &o
}

// NewAddressEncodeDecoderOptions creates a new AddressEncodeDecoderOptions.
func NewAddressEncodeDecoderOptions() AddressEncodeDecoderOptions {
	return &addrOptions{
		cOpts:          NewConnectionEncodeDecoderOptions(),
		dialTimeout:    defaultDialTimeout,
		reconnectDelay: defaultReconnectDelay,
		rOpts:          retry.NewOptions(),
		iOpts:          instrument.NewOptions(),
	}
}

type addrOptions struct {
	cOpts          ConnectionEncodeDecoderOptions
	rOpts          retry.Options
	dialTimeout    time.Duration
	reconnectDelay time.Duration
	iOpts          instrument.Options
}

func (opts *addrOptions) ConnectionEncodeDecoderOptions() ConnectionEncodeDecoderOptions {
	return opts.cOpts
}

func (opts *addrOptions) SetConnectionEncodeDecoderOptions(value ConnectionEncodeDecoderOptions) AddressEncodeDecoderOptions {
	o := *opts
	o.cOpts = value
	return &o
}

func (opts *addrOptions) ConnectionRetryOptions() retry.Options {
	return opts.rOpts
}

func (opts *addrOptions) SetConnectionRetryOptions(value retry.Options) AddressEncodeDecoderOptions {
	o := *opts
	o.rOpts = value
	return &o
}

func (opts *addrOptions) DialTimeout() time.Duration {
	return opts.dialTimeout
}

func (opts *addrOptions) SetDialTimeout(value time.Duration) AddressEncodeDecoderOptions {
	o := *opts
	o.dialTimeout = value
	return &o
}

func (opts *addrOptions) ReconnectDelay() time.Duration {
	return opts.reconnectDelay
}

func (opts *addrOptions) SetReconnectDelay(value time.Duration) AddressEncodeDecoderOptions {
	o := *opts
	o.reconnectDelay = value
	return &o
}

func (opts *addrOptions) InstrumentOptions() instrument.Options {
	return opts.iOpts
}

func (opts *addrOptions) SetInstrumentOptions(value instrument.Options) AddressEncodeDecoderOptions {
	o := *opts
	o.iOpts = value
	return &o
}
