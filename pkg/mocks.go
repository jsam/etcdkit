package etcdkit

import (
	"context"

	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// MockEventBus implements EventBusInterface for testing
type MockEventBus struct {
	mock.Mock
}

func (m *MockEventBus) Subscribe(topic string, ch chan<- *Event) error {
	args := m.Called(topic, mock.Anything)
	return args.Error(0)
}

func (m *MockEventBus) Unsubscribe(topic string, ch chan<- *Event) {
	m.Called(topic, mock.Anything)
}

func (m *MockEventBus) GetHistory(ctx context.Context, topic string, limit int64) ([]*Event, error) {
	args := m.Called(ctx, topic, limit)
	return args.Get(0).([]*Event), args.Error(1)
}

func (m *MockEventBus) Publish(ctx context.Context, event *Event) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *MockEventBus) Prefix() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockEventBus) Client() EtcdClientInterface {
	args := m.Called()
	return args.Get(0).(EtcdClientInterface)
}

// MockEtcdClient implements EtcdClientInterface for testing
type MockEtcdClient struct {
	mock.Mock
}

func (m *MockEtcdClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(*clientv3.GetResponse), args.Error(1)
}

func (m *MockEtcdClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	args := m.Called(ctx, key, val, opts)
	return args.Get(0).(*clientv3.PutResponse), args.Error(1)
}

func (m *MockEtcdClient) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(clientv3.WatchChan)
}

func (m *MockEtcdClient) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	args := m.Called(ctx, ttl)
	return args.Get(0).(*clientv3.LeaseGrantResponse), args.Error(1)
}

func (m *MockEtcdClient) Close() error {
	args := m.Called()
	return args.Error(0)
}
