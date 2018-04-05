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
	"time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3msg/topic"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
)

var (
	defaultDialTimeout               = 10 * time.Second
	defaultMessageRetryDelay         = 5 * time.Second
	defaultAckErrRetryDelay          = 10 * time.Second
	defaultPlacementWatchRetryDelay  = 5 * time.Second
	defaultPlacementWatchInitTimeout = 5 * time.Second
	defaultTopicWatchInitTimeout     = 5 * time.Second
	defaultCloseCheckInterval        = 2 * time.Second
	defaultConnectionResetDelay      = 2 * time.Second
)

// Options configs the writer.
type Options interface {
	// TopicName returns the topic name.
	TopicName() string

	// SetTopicName sets the topic name.
	SetTopicName(value string) Options

	// TopicService returns the topic service.
	TopicService() topic.Service

	// SetTopicService sets the topic service.
	SetTopicService(value topic.Service) Options

	// TopicWatchInitTimeout returns the timeout for topic watch initialization.
	TopicWatchInitTimeout() time.Duration

	// SetTopicWatchInitTimeout sets the timeout for topic watch initialization.
	SetTopicWatchInitTimeout(value time.Duration) Options

	// ServiceDiscovery returns the client to service discovery service.
	ServiceDiscovery() services.Services

	// SetServiceDiscovery sets the client to service discovery services.
	SetServiceDiscovery(value services.Services) Options

	// PlacementWatchRetryDelay returns the delay before retrying on placement watch errors.
	PlacementWatchRetryDelay() time.Duration

	// SetPlacementWatchRetryDelay sets the delay before retrying on placement watch errors.
	SetPlacementWatchRetryDelay(value time.Duration) Options

	// PlacementWatchInitTimeout returns the timeout for placement watch initialization.
	PlacementWatchInitTimeout() time.Duration

	// SetPlacementWatchInitTimeout sets the timeout for placement watch initialization.
	SetPlacementWatchInitTimeout(value time.Duration) Options

	// MessagePoolOptions returns the options of pool for messages.
	MessagePoolOptions() pool.ObjectPoolOptions

	// SetMessagePoolOptions sets the options of pool for messages.
	SetMessagePoolOptions(value pool.ObjectPoolOptions) Options

	// MessageRetryBackoff returns the backoff before retrying messages.
	MessageRetryBackoff() time.Duration

	// MessageRetryBackoff sets the backoff before retrying messages.
	SetMessageRetryBackoff(value time.Duration) Options

	// CloseCheckInterval returns the close check interval.
	CloseCheckInterval() time.Duration

	// SetCloseCheckInterval sets the close check interval.
	SetCloseCheckInterval(value time.Duration) Options

	// AckErrorRetryDelay returns the delay before retring on ack errors.
	AckErrorRetryDelay() time.Duration

	// SetAckErrorRetryDelay sets the delay before retring on ack errors.
	SetAckErrorRetryDelay(value time.Duration) Options

	// DialTimeout returns the dial timeout.
	DialTimeout() time.Duration

	// SetDialTimeout sets the dial timeout.
	SetDialTimeout(value time.Duration) Options

	// ConnectionResetDelay returns the delay before resetting connection.
	ConnectionResetDelay() time.Duration

	// SetConnectionResetDelay sets the delay before resetting connection.
	SetConnectionResetDelay(value time.Duration) Options

	// ConnectionRetryOptions returns the options for connection retrier.
	ConnectionRetryOptions() retry.Options

	// SetConnectionRetryOptions sets the options for connection retrier.
	SetConnectionRetryOptions(value retry.Options) Options

	// EncodeDecoderOptions returns the options for EncodeDecoder.
	EncodeDecoderOptions() proto.EncodeDecoderOptions

	// SetEncodeDecoderOptions sets the options for EncodeDecoder.
	SetEncodeDecoderOptions(value proto.EncodeDecoderOptions) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options
}

type writerOptions struct {
	topic                     string
	topicService              topic.Service
	topicWatchInitTimeout     time.Duration
	dialTimeout               time.Duration
	ackErrRetryDelay          time.Duration
	messageRetryDelay         time.Duration
	messagePoolOptions        pool.ObjectPoolOptions
	services                  services.Services
	placementWatchRetryDelay  time.Duration
	placementWatchInitTimeout time.Duration
	connectionResetDelay      time.Duration
	closeCheckInterval        time.Duration
	rOpts                     retry.Options
	encdecOpts                proto.EncodeDecoderOptions
	iOpts                     instrument.Options
}

// NewOptions creates Options.
func NewOptions() Options {
	return &writerOptions{
		dialTimeout:               defaultDialTimeout,
		ackErrRetryDelay:          defaultAckErrRetryDelay,
		messageRetryDelay:         defaultMessageRetryDelay,
		messagePoolOptions:        pool.NewObjectPoolOptions(),
		placementWatchRetryDelay:  defaultPlacementWatchRetryDelay,
		placementWatchInitTimeout: defaultPlacementWatchInitTimeout,
		topicWatchInitTimeout:     defaultTopicWatchInitTimeout,
		closeCheckInterval:        defaultCloseCheckInterval,
		connectionResetDelay:      defaultConnectionResetDelay,
		rOpts:                     retry.NewOptions(),
		encdecOpts:                proto.NewEncodeDecoderOptions(),
		iOpts:                     instrument.NewOptions(),
	}
}

func (opts *writerOptions) TopicName() string {
	return opts.topic
}

func (opts *writerOptions) SetTopicName(value string) Options {
	o := *opts
	o.topic = value
	return &o
}

func (opts *writerOptions) TopicService() topic.Service {
	return opts.topicService
}

func (opts *writerOptions) SetTopicService(value topic.Service) Options {
	o := *opts
	o.topicService = value
	return &o
}

func (opts *writerOptions) TopicWatchInitTimeout() time.Duration {
	return opts.topicWatchInitTimeout
}

func (opts *writerOptions) SetTopicWatchInitTimeout(value time.Duration) Options {
	o := *opts
	o.topicWatchInitTimeout = value
	return &o
}

func (opts *writerOptions) ServiceDiscovery() services.Services {
	return opts.services
}

func (opts *writerOptions) SetServiceDiscovery(value services.Services) Options {
	o := *opts
	o.services = value
	return &o
}

func (opts *writerOptions) PlacementWatchRetryDelay() time.Duration {
	return opts.placementWatchRetryDelay
}

func (opts *writerOptions) SetPlacementWatchRetryDelay(value time.Duration) Options {
	o := *opts
	o.placementWatchRetryDelay = value
	return &o
}

func (opts *writerOptions) PlacementWatchInitTimeout() time.Duration {
	return opts.placementWatchInitTimeout
}

func (opts *writerOptions) SetPlacementWatchInitTimeout(value time.Duration) Options {
	o := *opts
	o.placementWatchInitTimeout = value
	return &o
}

func (opts *writerOptions) MessagePoolOptions() pool.ObjectPoolOptions {
	return opts.messagePoolOptions
}

func (opts *writerOptions) SetMessagePoolOptions(value pool.ObjectPoolOptions) Options {
	o := *opts
	o.messagePoolOptions = value
	return &o
}

func (opts *writerOptions) MessageRetryBackoff() time.Duration {
	return opts.messageRetryDelay
}

func (opts *writerOptions) SetMessageRetryBackoff(value time.Duration) Options {
	o := *opts
	o.messageRetryDelay = value
	return &o
}

func (opts *writerOptions) CloseCheckInterval() time.Duration {
	return opts.closeCheckInterval
}

func (opts *writerOptions) SetCloseCheckInterval(value time.Duration) Options {
	o := *opts
	o.closeCheckInterval = value
	return &o
}

func (opts *writerOptions) AckErrorRetryDelay() time.Duration {
	return opts.ackErrRetryDelay
}

func (opts *writerOptions) SetAckErrorRetryDelay(value time.Duration) Options {
	o := *opts
	o.ackErrRetryDelay = value
	return &o
}

func (opts *writerOptions) DialTimeout() time.Duration {
	return opts.dialTimeout
}

func (opts *writerOptions) SetDialTimeout(value time.Duration) Options {
	o := *opts
	o.dialTimeout = value
	return &o
}

func (opts *writerOptions) ConnectionRetryOptions() retry.Options {
	return opts.rOpts
}

func (opts *writerOptions) SetConnectionRetryOptions(value retry.Options) Options {
	o := *opts
	o.rOpts = value
	return &o
}

func (opts *writerOptions) ConnectionResetDelay() time.Duration {
	return opts.connectionResetDelay
}

func (opts *writerOptions) SetConnectionResetDelay(value time.Duration) Options {
	o := *opts
	o.connectionResetDelay = value
	return &o
}

func (opts *writerOptions) EncodeDecoderOptions() proto.EncodeDecoderOptions {
	return opts.encdecOpts
}

func (opts *writerOptions) SetEncodeDecoderOptions(value proto.EncodeDecoderOptions) Options {
	o := *opts
	o.encdecOpts = value
	return &o
}

func (opts *writerOptions) InstrumentOptions() instrument.Options {
	return opts.iOpts
}

func (opts *writerOptions) SetInstrumentOptions(value instrument.Options) Options {
	o := *opts
	o.iOpts = value
	return &o
}
