package etcdkit

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func createTestEvent(topic, key, value string) clientv3.WatchResponse {
	event := &Event{
		ID:        "1",
		Topic:     topic,
		Data:      []byte(value),
		Timestamp: time.Now(),
	}
	data, _ := json.Marshal(event)
	return clientv3.WatchResponse{
		Events: []*clientv3.Event{
			{
				Type: clientv3.EventTypePut,
				Kv:   &mvccpb.KeyValue{Key: []byte(key), Value: data},
			},
		},
	}
}

// Mock etcd client
type mockEtcdClient struct {
	mock.Mock
}

func (m *mockEtcdClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(*clientv3.GetResponse), args.Error(1)
}

func (m *mockEtcdClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	args := m.Called(ctx, key, val, opts)
	return args.Get(0).(*clientv3.PutResponse), args.Error(1)
}

func (m *mockEtcdClient) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(clientv3.WatchChan)
}

func (m *mockEtcdClient) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	args := m.Called(ctx, ttl)
	return args.Get(0).(*clientv3.LeaseGrantResponse), args.Error(1)
}

func (m *mockEtcdClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestNewEventBus(t *testing.T) {
	eb, err := NewEventBus([]string{"localhost:2379"}, "test_prefix")
	assert.NoError(t, err)
	assert.NotNil(t, eb)
	assert.Equal(t, "test_prefix", eb.prefix)
}

func TestCheckConnection(t *testing.T) {
	mockClient := new(mockEtcdClient)
	eb := &EventBus{client: mockClient, prefix: "test"}

	mockClient.On("Get", mock.Anything, "health_check", mock.Anything).Return(&clientv3.GetResponse{}, nil)

	err := eb.CheckConnection(context.Background())
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

func TestPublish(t *testing.T) {
	mockClient := new(mockEtcdClient)
	eb := &EventBus{client: mockClient, prefix: "test"}

	event := NewEvent("topic1", []byte("test data"))

	mockClient.On("Grant", mock.Anything, int64(24*60*60)).Return(&clientv3.LeaseGrantResponse{ID: 1}, nil)
	mockClient.On("Put", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&clientv3.PutResponse{}, nil)

	err := eb.Publish(context.Background(), event)
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

func TestSubscribe(t *testing.T) {
	mockClient := new(mockEtcdClient)
	eb := &EventBus{
		client:      mockClient,
		prefix:      "test",
		subscribers: make(map[string][]chan<- *Event),
	}

	watchChan := make(chan clientv3.WatchResponse)
	mockClient.On("Watch", mock.Anything, "test/topic1", mock.Anything).Return((clientv3.WatchChan)(watchChan))

	go func() {
		watchChan <- createTestEvent("topic1", "test/topic1/key1", "test data")
		close(watchChan)
	}()

	eventChan := make(chan *Event)
	go eb.Subscribe("topic1", eventChan)

	event := <-eventChan
	assert.Equal(t, "topic1", event.Topic)
	assert.Equal(t, []byte("test data"), event.Data)
}

func TestUnsubscribe(t *testing.T) {
	mockClient := new(mockEtcdClient)
	eb := &EventBus{
		client:      mockClient,
		prefix:      "test",
		subscribers: make(map[string][]chan<- *Event),
	}

	watchChan := make(chan clientv3.WatchResponse)
	mockClient.On("Watch", mock.Anything, "test/topic1", mock.Anything).Return(clientv3.WatchChan(watchChan))

	ch1 := make(chan *Event)
	ch2 := make(chan *Event)

	eb.Subscribe("topic1", ch1)
	eb.Subscribe("topic1", ch2)

	eb.Unsubscribe("topic1", ch1)
	assert.Len(t, eb.subscribers["topic1"], 1)

	// Instead of directly comparing the channels, we'll check if the remaining channel is ch2
	assert.Equal(t, 1, len(eb.subscribers["topic1"]))
	if len(eb.subscribers["topic1"]) == 1 {
		remainingChan := eb.subscribers["topic1"][0]
		assert.Equal(t, fmt.Sprintf("%p", ch2), fmt.Sprintf("%p", remainingChan))
	}

	// Close the watch channel to stop the goroutine
	close(watchChan)
}

func TestGetHistory(t *testing.T) {
	mockClient := new(mockEtcdClient)
	eb := &EventBus{client: mockClient, prefix: "test"}

	// Create some test events
	events := []*Event{
		NewEvent("topic1", []byte("data1")),
		NewEvent("topic1", []byte("data2")),
		NewEvent("topic1", []byte("data3")),
	}

	// Prepare the mock response
	mockResponse := &clientv3.GetResponse{
		Kvs: make([]*mvccpb.KeyValue, len(events)),
	}

	for i, event := range events {
		eventData, _ := event.Marshal()
		mockResponse.Kvs[i] = &mvccpb.KeyValue{
			Key:   []byte(fmt.Sprintf("test/topic1/%s", event.ID)),
			Value: eventData,
		}
	}

	// Set up the mock expectation
	mockClient.On("Get", mock.Anything, "test/topic1", mock.Anything).Return(mockResponse, nil)

	// Call GetHistory
	retrievedEvents, err := eb.GetHistory(context.Background(), "topic1", 10)

	// Assert no error occurred
	assert.NoError(t, err)

	// Assert the correct number of events was retrieved
	assert.Equal(t, len(events), len(retrievedEvents))

	// Assert each event was correctly unmarshalled
	for i, event := range events {
		assert.Equal(t, event.ID, retrievedEvents[i].ID)
		assert.Equal(t, event.Topic, retrievedEvents[i].Topic)
		assert.Equal(t, event.Data, retrievedEvents[i].Data)
		assert.WithinDuration(t, event.Timestamp, retrievedEvents[i].Timestamp, time.Second)
	}

	// Assert that the mock expectations were met
	mockClient.AssertExpectations(t)
}

func TestMatchTopic(t *testing.T) {
	testCases := []struct {
		subscription string
		event        string
		expected     bool
	}{
		{"foo/bar", "foo/bar", true},
		{"foo/+", "foo/bar", true},
		{"foo/#", "foo/bar/baz", true},
		{"foo/bar", "foo/baz", false},
		{"foo/+/baz", "foo/bar/baz", true},
		{"foo/+/baz", "foo/bar/qux", false},
		{"foo/#", "bar/baz", false},
	}

	for _, tc := range testCases {
		t.Run(tc.subscription+" - "+tc.event, func(t *testing.T) {
			result := matchTopic(tc.subscription, tc.event)
			assert.Equal(t, tc.expected, result)
		})
	}
}
