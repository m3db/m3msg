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

package consumer

import (
	"net"
	"sync"
	"testing"

	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/protocol/proto"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
)

func TestConsumerServer(t *testing.T) {
	defer leaktest.Check(t)()

	var (
		count = 0
		bytes []byte
		wg    sync.WaitGroup
	)
	wg.Add(1)
	messageFn := func(m Message) {
		count++
		bytes = m.Bytes()
		m.Ack()
		wg.Done()
	}

	opts := NewServerOptions().SetConsumerOptions(testOptions()).SetMessageFn(messageFn)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s, err := NewServer("a", opts)
	require.NoError(t, err)

	s.Serve(l)

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	producer := proto.NewEncodeDecoder(conn, opts.ConsumerOptions().EncodeDecoderOptions())
	err = producer.Encode(&testMsg1)
	require.NoError(t, err)

	wg.Wait()
	require.Equal(t, testMsg1.Value, bytes)

	var ack msgpb.Ack
	err = producer.Decode(&ack)
	require.NoError(t, err)
	require.Equal(t, 1, len(ack.Metadata))
	require.Equal(t, testMsg1.Metadata, ack.Metadata[0])

	s.Close()
	s.Close()

	_, err = net.Dial("tcp", l.Addr().String())
	require.Error(t, err)
}
