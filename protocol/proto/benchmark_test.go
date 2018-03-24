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
	"bytes"
	"testing"

	"github.com/m3db/m3msg/generated/proto/msgpb"
)

func BenchmarkAddressEncodeDecoerRoundTrip(b *testing.B) {
	a := NewAddressEncodeDecoder(
		"",
		NewAddressEncodeDecoderOptions().
			SetReconnectDelay(0).
			SetConnectionEncodeDecoderOptions(
				NewConnectionEncodeDecoderOptions().
					SetEncoderOptions(NewEncodeDecoderOptions().SetBufferSize(1)),
			),
	).(*addrEncdec)
	mimicTCP := bytes.NewBuffer(nil)
	a.encdec.(*connEncdec).resetWriter(mimicTCP)
	a.encdec.(*connEncdec).resetReader(mimicTCP)
	a.validConn = true
	encodeMsg := msgpb.Message{
		Metadata: &msgpb.Metadata{},
		Value:    make([]byte, 200),
	}
	decodeMsg := msgpb.Message{}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		encodeMsg.Metadata.Id = uint64(n)
		if err := a.Encode(&encodeMsg); err != nil {
			b.FailNow()
		}
		if err := a.Decode(&decodeMsg); err != nil {
			b.FailNow()
		}
		if decodeMsg.Metadata.Id != uint64(n) {
			b.FailNow()
		}
	}
}

func BenchmarkConnectionEncodeDecoerRoundTrip(b *testing.B) {
	c := NewConnectionEncodeDecoder(
		nil,
		NewConnectionEncodeDecoderOptions().
			SetEncoderOptions(NewEncodeDecoderOptions().SetBufferSize(1)),
	).(*connEncdec)
	mimicTCP := bytes.NewBuffer(nil)
	c.resetWriter(mimicTCP)
	c.resetReader(mimicTCP)
	encodeMsg := msgpb.Message{
		Metadata: &msgpb.Metadata{},
		Value:    make([]byte, 200),
	}
	decodeMsg := msgpb.Message{}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		encodeMsg.Metadata.Id = uint64(n)
		if err := c.Encode(&encodeMsg); err != nil {
			b.FailNow()
		}
		if err := c.Decode(&decodeMsg); err != nil {
			b.FailNow()
		}
		if decodeMsg.Metadata.Id != uint64(n) {
			b.FailNow()
		}
	}
}

func BenchmarkEncodeDecoerRoundTrip(b *testing.B) {
	mimicTCP := bytes.NewBuffer(nil)
	encoder := NewEncoder(mimicTCP, NewEncodeDecoderOptions().SetBufferSize(1))
	decoder := NewDecoder(mimicTCP, NewEncodeDecoderOptions().SetBufferSize(1))
	encodeMsg := msgpb.Message{
		Metadata: &msgpb.Metadata{},
		Value:    make([]byte, 200),
	}
	decodeMsg := msgpb.Message{}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		encodeMsg.Metadata.Id = uint64(n)
		if err := encoder.Encode(&encodeMsg); err != nil {
			b.FailNow()
		}
		if err := decoder.Decode(&decodeMsg); err != nil {
			b.FailNow()
		}
		if decodeMsg.Metadata.Id != uint64(n) {
			b.FailNow()
		}
	}
}
