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
	"time"

	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
)

// Marshaler can be marshaled.
type Marshaler interface {
	Size() int
	MarshalTo(data []byte) (int, error)
}

// Unmarshaler can be unmarshaled from bytes.
type Unmarshaler interface {
	Unmarshal(data []byte) error
}

// Encoder encodes the marshaler.
type Encoder interface {
	Encode(m Marshaler) error
}

// Decoder decodes into an unmarshaler.
type Decoder interface {
	Decode(m Unmarshaler) error
}

// EncodeDecoder can encode and decode.
type EncodeDecoder interface {
	Encoder
	Decoder
	Close()
}

// ConnectionEncodeDecoder is an EncodeDecoder based on a connection.
type ConnectionEncodeDecoder interface {
	EncodeDecoder

	// ResetConn resets the connection.
	ResetConn(conn net.Conn)
}

// AddressEncodeDecoder is an EncodeDecoder based on a server address.
type AddressEncodeDecoder interface {
	EncodeDecoder

	// Init initializes the AddressEncodeDecoder.
	Init()
}

// ConnectionEncodeDecoderPool is a pool of ConnectionEncodeDecoders.
type ConnectionEncodeDecoderPool interface {
	// Init initializes the ConnectionEncodeDecoderPool pool.
	Init(alloc ConnectionEncodeDecoderAlloc)

	// Get returns an ConnectionEncodeDecoder from the pool.
	Get() ConnectionEncodeDecoder

	// Put puts an ConnectionEncodeDecoder into the pool.
	Put(c ConnectionEncodeDecoder)
}

// ConnectionEncodeDecoderAlloc allocates a ConnectionEncodeDecoder.
type ConnectionEncodeDecoderAlloc func() ConnectionEncodeDecoder

// EncodeDecoderOptions configures an EncodeDecoder.
type EncodeDecoderOptions interface {
	// BytesPool returns the bytes pool.
	BytesPool() pool.BytesPool

	// SetBytesPool sets the bytes pool.
	SetBytesPool(value pool.BytesPool) EncodeDecoderOptions

	// BufferSize returns the size of buffer before a write or a read.
	BufferSize() int

	// SetBufferSize sets the buffer size.
	SetBufferSize(value int) EncodeDecoderOptions
}

// ConnectionEncodeDecoderOptions configures a ConnectionEncodeDecoder.
type ConnectionEncodeDecoderOptions interface {
	// EncodeWithLock returns whether the encode function should be guarded with a lock.
	EncodeWithLock() bool

	// SetEncodeWithLock sets EncodeWithLock.
	SetEncodeWithLock(value bool) ConnectionEncodeDecoderOptions

	// DecodeWithLock returns whether the decode function should be guarded with a lock.
	DecodeWithLock() bool

	// SetDecodeWithLock sets DecodeWithLock.
	SetDecodeWithLock(value bool) ConnectionEncodeDecoderOptions

	// EncoderOptions returns the options for encoder.
	EncoderOptions() EncodeDecoderOptions

	// SetEncoderOptions sets the options for encoder.
	SetEncoderOptions(value EncodeDecoderOptions) ConnectionEncodeDecoderOptions

	// DecoderOptions returns the options for decoder.
	DecoderOptions() EncodeDecoderOptions

	// SetDecoderOptions sets the options for decoder.
	SetDecoderOptions(value EncodeDecoderOptions) ConnectionEncodeDecoderOptions

	// ConnectionEncodeDecoderPool returns the pool for ConnectionEncodeDecoder.
	ConnectionEncodeDecoderPool() ConnectionEncodeDecoderPool

	// SetConnectionEncodeDecoderPool sets the pool for ConnectionEncodeDecoder.
	SetConnectionEncodeDecoderPool(pool ConnectionEncodeDecoderPool) ConnectionEncodeDecoderOptions
}

// AddressEncodeDecoderOptions configures an AddressEncodeDecoder.
type AddressEncodeDecoderOptions interface {
	// ConnectionEncodeDecoderOptions returns the options for ConnectionEncodeDecoder.
	ConnectionEncodeDecoderOptions() ConnectionEncodeDecoderOptions

	// SetConnectionEncodeDecoderOptions sets the options for ConnectionEncodeDecoder.
	SetConnectionEncodeDecoderOptions(value ConnectionEncodeDecoderOptions) AddressEncodeDecoderOptions

	// ConnectionRetryOptions returns the options for connection retrier.
	ConnectionRetryOptions() retry.Options

	// SetConnectionRetryOptions sets the options for connection retrier.
	SetConnectionRetryOptions(value retry.Options) AddressEncodeDecoderOptions

	// DialTimeout returns the dial timeout.
	DialTimeout() time.Duration

	// SetDialTimeout sets the dial timeout.
	SetDialTimeout(value time.Duration) AddressEncodeDecoderOptions

	// ReconnectDelay returns the delay between reconnections.
	ReconnectDelay() time.Duration

	// SetReconnectDelay sets the delay between reconnections.
	SetReconnectDelay(value time.Duration) AddressEncodeDecoderOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) AddressEncodeDecoderOptions
}
