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

	"github.com/m3db/m3x/pool"
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

	// Close closes the EncodeDecoder.
	Close()

	// Reset resets the EncodeDecoder.
	Reset(conn net.Conn)
}

// EncodeDecoderPool is a pool of EncodeDecoders.
type EncodeDecoderPool interface {
	// Init initializes the EncodeDecoder pool.
	Init(alloc EncodeDecoderAlloc)

	// Get returns an EncodeDecoder from the pool.
	Get() EncodeDecoder

	// Put puts an EncodeDecoder into the pool.
	Put(c EncodeDecoder)
}

// EncodeDecoderAlloc allocates an EncodeDecoder.
type EncodeDecoderAlloc func() EncodeDecoder

// BaseOptions configures a base encoder or decoder.
type BaseOptions interface {
	// BytesPool returns the bytes pool.
	BytesPool() pool.BytesPool

	// SetBytesPool sets the bytes pool.
	SetBytesPool(value pool.BytesPool) BaseOptions

	// BufferSize returns the size of buffer before a write or a read.
	BufferSize() int

	// SetBufferSize sets the buffer size.
	SetBufferSize(value int) BaseOptions
}

// EncodeDecoderOptions configures an EncodeDecoder.
type EncodeDecoderOptions interface {
	// EncoderOptions returns the options for encoder.
	EncoderOptions() BaseOptions

	// SetEncoderOptions sets the options for encoder.
	SetEncoderOptions(value BaseOptions) EncodeDecoderOptions

	// DecoderOptions returns the options for decoder.
	DecoderOptions() BaseOptions

	// SetDecoderOptions sets the options for decoder.
	SetDecoderOptions(value BaseOptions) EncodeDecoderOptions

	// EncodeDecoderPool returns the pool for EncodeDecoder.
	EncodeDecoderPool() EncodeDecoderPool

	// SetEncodeDecoderPool sets the pool for EncodeDecoder.
	SetEncodeDecoderPool(pool EncodeDecoderPool) EncodeDecoderOptions
}
