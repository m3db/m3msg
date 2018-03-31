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

package producer

import (
	"github.com/m3db/m3cluster/services"
)

type producer struct {
	b Buffer
	w Writer
}

// NewProducer returns a new producer.
func NewProducer(opts Options) Producer {
	return &producer{
		b: opts.Buffer(),
		w: opts.Writer(),
	}
}

func (p *producer) Init() {
	p.b.Init()
	p.w.Init()
}

func (p *producer) Produce(data Data) error {
	rd, err := p.b.Buffer(data)
	if err != nil {
		return err
	}
	return p.w.Write(rd)
}

func (p *producer) RegisterFilter(sid services.ServiceID, fn FilterFunc) {
	p.w.RegisterFilter(sid, fn)
}

func (p *producer) RemoveFilter(sid services.ServiceID) {
	p.w.RemoveFilter(sid)
}

func (p *producer) Close() {
	// Must close buffer first, it will stop receiving new writes
	// and return when all data cleared up. We can safely close the writer after that.
	p.b.Close()
	p.w.Close()
}
