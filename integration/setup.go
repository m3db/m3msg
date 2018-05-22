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

package integration

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/placement/service"
	"github.com/m3db/m3cluster/placement/storage"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3msg/consumer"
	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3msg/producer/config"
	"github.com/m3db/m3msg/topic"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	xsync "github.com/m3db/m3x/sync"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	yaml "gopkg.in/yaml.v2"
)

const (
	numConcurrentMessages = 10
	numberOfShards        = 10
	msgPerShard           = 200
	closeTimeout          = 30 * time.Second
)

type consumerServiceConfig struct {
	ct        topic.ConsumptionType
	instances int
	replicas  int
}

type op struct {
	progressPct int
	fn          func()
}

func newTestSetup(
	t *testing.T,
	ctrl *gomock.Controller,
	numProducers int,
	configs []consumerServiceConfig,
) *setup {
	log.SimpleLogger.Debugf("setting up a test with %d producers", numProducers)

	configService := client.NewMockClient(ctrl)
	configService.EXPECT().Store(gomock.Any()).Return(mem.NewStore(), nil).AnyTimes()

	sd := services.NewMockServices(ctrl)
	configService.EXPECT().Services(gomock.Any()).Return(sd, nil).AnyTimes()

	var (
		testConsumerServices  []*testConsumerService
		topicConsumerServices []topic.ConsumerService
		totalConsumed         = atomic.NewInt64(0)
	)
	for i, config := range configs {
		log.SimpleLogger.Debugf("setting up a consumer service in %s mode with %d replicas", config.ct.String(), config.replicas)

		sid := serviceID(i)
		consumerService := topic.NewConsumerService().SetServiceID(sid).SetConsumptionType(config.ct)
		topicConsumerServices = append(topicConsumerServices, consumerService)

		ps := testPlacementService(mem.NewStore(), sid)
		sd.EXPECT().PlacementService(sid, gomock.Any()).Return(ps, nil).Times(numProducers)

		cs := testConsumerService{
			consumed:         make(map[string]struct{}),
			sid:              sid,
			placementService: ps,
			consumerService:  consumerService,
		}
		testConsumerServices = append(testConsumerServices, &cs)
		var instances []placement.Instance
		for i := 0; i < config.instances; i++ {
			c := newTestConsumer(t, &cs)
			c.consumeAndAck(totalConsumed)
			cs.testConsumers = append(cs.testConsumers, c)
			instances = append(instances, c.instance)
		}
		p, err := ps.BuildInitialPlacement(instances, numberOfShards, config.replicas)
		require.NoError(t, err)
		require.Equal(t, len(instances), p.NumInstances())
	}

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(configService))
	require.NoError(t, err)

	testTopic := topic.NewTopic().
		SetName("topicName").
		SetNumberOfShards(uint32(numberOfShards)).
		SetConsumerServices(topicConsumerServices)
	err = ts.CheckAndSet(testTopic.Name(), 0, testTopic)
	require.NoError(t, err)

	var producers []producer.Producer
	for i := 0; i < numProducers; i++ {
		p := testProducer(t, configService)
		require.NoError(t, p.Init())
		producers = append(producers, p)
	}

	return &setup{
		configs:          configs,
		producers:        producers,
		consumerServices: testConsumerServices,
		totalConsumed:    totalConsumed,
	}
}

type setup struct {
	configs          []consumerServiceConfig
	producers        []producer.Producer
	consumerServices []*testConsumerService
	totalConsumed    *atomic.Int64
	extraOps         []op
}

func (s *setup) TotalMessages() int {
	return msgPerShard * numberOfShards * len(s.producers)
}

func (s *setup) Run(
	t *testing.T,
	ctrl *gomock.Controller,
) {
	numWritesPerProducer := msgPerShard * numberOfShards
	mockData := make([]producer.Data, 0, numWritesPerProducer)
	for i := 0; i < numberOfShards; i++ {
		for j := 0; j < msgPerShard; j++ {
			b := fmt.Sprintf("foo%d-%d", i, j)
			md := producer.NewMockData(ctrl)
			md.EXPECT().Size().Return(uint32(len(b))).AnyTimes()
			md.EXPECT().Bytes().Return([]byte(b)).AnyTimes()
			md.EXPECT().Shard().Return(uint32(i)).AnyTimes()
			md.EXPECT().Finalize(producer.Consumed).Times(len(s.producers))
			mockData = append(mockData, md)
		}
	}

	ops := make(map[int]func(), len(s.extraOps))
	for _, op := range s.extraOps {
		num := op.progressPct * numWritesPerProducer / 100
		ops[num] = op.fn
	}
	log.SimpleLogger.Debug("producing data")
	for i := 0; i < numWritesPerProducer; i++ {
		if fn, ok := ops[i]; ok {
			fn()
		}
		d := mockData[i]
		for _, p := range s.producers {
			require.NoError(t, p.Produce(d))
		}
	}
	log.SimpleLogger.Debug("produced all the data")
	s.CloseProducers(closeTimeout)
	s.CloseConsumers()
	for _, cs := range s.consumerServices {
		require.Equal(t, numWritesPerProducer, len(cs.consumed))
	}

	expectedConsumeReplica := 0
	for _, csc := range s.configs {
		if csc.ct == topic.Shared {
			expectedConsumeReplica++
			continue
		}
		expectedConsumeReplica += csc.replicas
	}
	expectedConsumed := expectedConsumeReplica * numWritesPerProducer * len(s.producers)
	require.True(t, int(s.totalConsumed.Load()) >= expectedConsumed, fmt.Sprintf("expect %d, consumed %d", expectedConsumed, s.totalConsumed.Load()))
	log.SimpleLogger.Debug("done")
}

func (s setup) CloseProducers(dur time.Duration) {
	doneCh := make(chan struct{})

	go func() {
		for _, p := range s.producers {
			p.Close(producer.WaitForConsumption)
			log.SimpleLogger.Debug("producer closed")
		}
		close(doneCh)
	}()

	select {
	case <-time.After(dur):
		panic(fmt.Sprintf("taking more than %v to close producers %v", dur, time.Now()))
	case <-doneCh:
		log.SimpleLogger.Debugf("producer closed in %v", dur)
		return
	}
}

func (s setup) CloseConsumers() {
	for _, cs := range s.consumerServices {
		cs.Close()
	}
}

func (s *setup) ScheduleOperations(pct int, fn func()) {
	if pct < 0 || pct > 100 {
		return
	}
	s.extraOps = append(s.extraOps, op{progressPct: pct, fn: fn})
}

func (s *setup) KillConnection(t *testing.T, idx int) {
	require.True(t, idx < len(s.consumerServices))
	cs := s.consumerServices[idx]

	testConsumers := cs.testConsumers
	require.NotEmpty(t, testConsumers)
	c := testConsumers[len(testConsumers)-1]
	c.closeOneConsumer()

	log.SimpleLogger.Debugf("killed a consumer on instance: %s", c.instance.ID())
	p, _, err := cs.placementService.Placement()
	require.NoError(t, err)
	log.SimpleLogger.Debugf("placement: %s", p.String())
}

func (s *setup) KillInstance(t *testing.T, idx int) {
	require.True(t, idx < len(s.consumerServices))
	cs := s.consumerServices[idx]

	testConsumers := cs.testConsumers
	require.NotEmpty(t, testConsumers)
	c := testConsumers[len(testConsumers)-1]
	c.Close()

	log.SimpleLogger.Debugf("killed instance: %s", c.instance.ID())
	p, _, err := cs.placementService.Placement()
	require.NoError(t, err)
	log.SimpleLogger.Debugf("placement: %s", p.String())
}

func (s *setup) AddInstance(t *testing.T, idx int) {
	require.True(t, idx < len(s.consumerServices))
	cs := s.consumerServices[idx]

	newConsumer := newTestConsumer(t, cs)
	newConsumer.consumeAndAck(s.totalConsumed)

	p, _, err := cs.placementService.Placement()
	require.NoError(t, err)
	log.SimpleLogger.Debugf("old placement: %s", p.String())

	p, _, err = cs.placementService.AddInstances([]placement.Instance{newConsumer.instance})
	require.NoError(t, err)
	log.SimpleLogger.Debugf("new placement: %s", p.String())
	cs.testConsumers = append(cs.testConsumers, newConsumer)
}

func (s *setup) RemoveInstance(t *testing.T, idx int) {
	require.True(t, idx < len(s.consumerServices))
	cs := s.consumerServices[idx]

	testConsumers := cs.testConsumers
	require.NotEmpty(t, testConsumers)
	l := len(testConsumers)
	oldConsumer := testConsumers[l-1]
	defer oldConsumer.Close()

	p, _, err := cs.placementService.Placement()
	require.NoError(t, err)
	log.SimpleLogger.Debugf("old placement: %s", p.String())

	p, err = cs.placementService.RemoveInstances([]string{oldConsumer.instance.ID()})
	require.NoError(t, err)
	log.SimpleLogger.Debugf("new placement: %s", p.String())
	cs.testConsumers = testConsumers[:l-1]
}

func (s *setup) ReplaceInstance(t *testing.T, idx int) {
	require.True(t, idx < len(s.consumerServices))
	cs := s.consumerServices[idx]

	newConsumer := newTestConsumer(t, cs)
	newConsumer.consumeAndAck(s.totalConsumed)

	testConsumers := cs.testConsumers
	require.NotEmpty(t, testConsumers)
	l := len(testConsumers)
	oldConsumer := testConsumers[l-1]
	defer oldConsumer.Close()

	p, _, err := cs.placementService.Placement()
	require.NoError(t, err)
	log.SimpleLogger.Debugf("old placement: %s", p.String())

	p, _, err = cs.placementService.ReplaceInstances(
		[]string{oldConsumer.instance.ID()},
		[]placement.Instance{newConsumer.instance},
	)
	require.NoError(t, err)
	log.SimpleLogger.Debugf("new placement: %s", p.String())
	cs.testConsumers[l-1] = newConsumer
}

type testConsumerService struct {
	sync.Mutex

	consumed         map[string]struct{}
	sid              services.ServiceID
	placementService placement.Service
	consumerService  topic.ConsumerService
	testConsumers    []*testConsumer
}

func (cs *testConsumerService) markConsumed(b []byte) {
	cs.Lock()
	defer cs.Unlock()

	cs.consumed[string(b)] = struct{}{}
}

func (cs *testConsumerService) Close() {
	for _, c := range cs.testConsumers {
		c.Close()
	}
}

type testConsumer struct {
	sync.RWMutex

	cs        *testConsumerService
	listener  consumer.Listener
	consumers []consumer.Consumer
	instance  placement.Instance
	consumed  int
	closed    bool
	doneCh    chan struct{}
}

func (c *testConsumer) Close() {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return
	}
	c.closed = true
	c.listener.Close()
	close(c.doneCh)
}

func newTestConsumer(t *testing.T, cs *testConsumerService) *testConsumer {
	consumerListener, err := consumer.NewListener("127.0.0.1:0", testConsumerOptions(t))
	require.NoError(t, err)

	addr := consumerListener.Addr().String()
	c := &testConsumer{
		cs:       cs,
		listener: consumerListener,
		instance: placement.NewInstance().
			SetID(addr).
			SetEndpoint(addr).
			SetIsolationGroup(addr).
			SetWeight(1),
		consumed: 0,
		closed:   false,
		doneCh:   make(chan struct{}),
	}
	return c
}

func (c *testConsumer) closeOneConsumer() {
	for {
		c.Lock()
		l := len(c.consumers)
		if l == 0 {
			c.Unlock()
			time.Sleep(200 * time.Millisecond)
			continue
		}
		c.consumers[l-1].Close()
		c.consumers = c.consumers[:l-1]
		c.Unlock()
		break
	}
}

func (c *testConsumer) consumeAndAck(totalConsumed *atomic.Int64) {
	wp := xsync.NewWorkerPool(numConcurrentMessages)
	wp.Init()

	go func() {
		for {
			consumer, err := c.listener.Accept()
			if err != nil {
				return
			}
			c.Lock()
			c.consumers = append(c.consumers, consumer)
			c.Unlock()
			go func() {
				for {
					select {
					case <-c.doneCh:
						consumer.Close()
						return
					default:
						msg, err := consumer.Message()
						if err != nil {
							consumer.Close()
							return
						}

						wp.Go(
							func() {
								totalConsumed.Inc()
								c.cs.markConsumed(msg.Bytes())
								msg.Ack()
								c.Lock()
								c.consumed++
								c.Unlock()
							},
						)
					}
				}
			}()
		}
	}()
}

func testPlacementService(store kv.Store, sid services.ServiceID) placement.Service {
	opts := placement.NewOptions().SetShardStateMode(placement.StableShardStateOnly)
	return service.NewPlacementService(storage.NewPlacementStorage(store, sid.String(), opts), opts)
}

func testProducer(
	t *testing.T,
	cs client.Client,
) producer.Producer {
	str := `
writer:
  topicName: topicName
  topicWatchInitTimeout: 100ms
  placementWatchInitTimeout: 100ms
  messagePool:
    size: 1
  messageRetry:
    initialBackoff: 10ms
    maxBackoff: 50ms
  messageQueueScanInterval: 10ms
  closeCheckInterval: 100ms
  ackErrorRetry: 
    initialBackoff: 10ms
    maxBackoff: 50ms
  connection:
    dialTimeout: 500ms
    keepAlivePeriod: 2s
    retry:
      initialBackoff: 10ms
      maxBackoff: 50ms
    writeBufferSize: 1
    resetDelay: 50ms
`

	var cfg config.ProducerConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))

	p, err := cfg.NewProducer(cs, instrument.NewOptions())
	require.NoError(t, err)
	return p
}

func testConsumerOptions(t *testing.T) consumer.Options {
	str := `
ackBufferSize: 1
connectionWriteBufferSize: 1
`
	var cfg consumer.Configuration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))

	return cfg.NewOptions(instrument.NewOptions())
}

func serviceID(id int) services.ServiceID {
	return services.NewServiceID().SetName("serviceName" + strconv.Itoa(id))
}
