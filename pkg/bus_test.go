package etcdkit

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

func TestNewEventBus(t *testing.T) {
	eb, err := NewEventBus([]string{"localhost:2379"}, "test_prefix")
	assert.NoError(t, err)
	assert.NotNil(t, eb)
	assert.Equal(t, "test_prefix", eb.prefix)
}

func TestCheckConnection(t *testing.T) {
	mockClient := new(MockEtcdClient)
	eb := &EventBus{client: mockClient, prefix: "test"}

	mockClient.On("Get", mock.Anything, "health_check", mock.Anything).Return(&clientv3.GetResponse{}, nil)

	err := eb.CheckConnection(context.Background())
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

func TestPublish(t *testing.T) {
	mockClient := new(MockEtcdClient)
	eb := &EventBus{client: mockClient, prefix: "test"}

	event := NewEvent("topic1", []byte("test data"))

	mockClient.On("Grant", mock.Anything, int64(24*60*60)).Return(&clientv3.LeaseGrantResponse{ID: 1}, nil)
	mockClient.On("Put", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&clientv3.PutResponse{}, nil)

	err := eb.Publish(context.Background(), event)
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

func TestSubscribe(t *testing.T) {
	mockClient := new(MockEtcdClient)
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

func TestWildcardSubscription(t *testing.T) {
	mockClient := new(MockEtcdClient)
	eb := &EventBus{
		client:      mockClient,
		prefix:      "test",
		subscribers: make(map[string][]chan<- *Event),
	}

	watchChan := make(chan clientv3.WatchResponse)
	mockClient.On("Watch", mock.Anything, "test/sports/", mock.Anything).Return((clientv3.WatchChan)(watchChan))

	// Create a channel to receive events
	eventChan := make(chan *Event, 3) // Buffer size of 3 to hold all expected events

	// Subscribe to the wildcard topic
	err := eb.Subscribe("sports/+", eventChan)
	assert.NoError(t, err)

	// Publish events to different sports topics
	go func() {
		watchChan <- createTestEvent("sports/football", "test/sports/football/1", "football data")
		watchChan <- createTestEvent("sports/basketball", "test/sports/basketball/1", "basketball data")
		watchChan <- createTestEvent("sports/tennis", "test/sports/tennis/1", "tennis data")
		close(watchChan)
	}()

	// Collect received events
	receivedEvents := make([]*Event, 0, 3)
	timeout := time.After(2 * time.Second)

	for i := 0; i < 3; i++ {
		select {
		case event := <-eventChan:
			receivedEvents = append(receivedEvents, event)
		case <-timeout:
			t.Fatal("Timeout waiting for events")
		}
	}

	// Check if we received all 3 events
	assert.Equal(t, 3, len(receivedEvents), "Expected to receive 3 events")

	// Check if the received events match the published ones
	expectedTopics := []string{"sports/football", "sports/basketball", "sports/tennis"}
	expectedData := []string{"football data", "basketball data", "tennis data"}

	for i, event := range receivedEvents {
		assert.Contains(t, expectedTopics, event.Topic, "Unexpected topic received")
		assert.Equal(t, []byte(expectedData[i]), event.Data, "Unexpected data for topic %s", event.Topic)
	}

	// Clean up
	eb.Unsubscribe("sports/+", eventChan)
}

func TestIntegrationWildcardSubscription(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Connect to etcd
	endpoints := []string{"localhost:2379"} // Adjust this to your etcd endpoint
	eb, err := NewEventBus(endpoints, "test")
	require.NoError(t, err)
	defer eb.Close()

	// Check connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = eb.CheckConnection(ctx)
	require.NoError(t, err, "Failed to connect to etcd")

	// Create a channel to receive events
	eventChan := make(chan *Event, 3)

	// Subscribe to the wildcard topic
	err = eb.Subscribe("sports/+", eventChan)
	require.NoError(t, err)

	// Publish events to different sports topics
	sportsTopics := []string{"sports/football", "sports/basketball", "sports/tennis"}
	for _, topic := range sportsTopics {
		event := NewEvent(topic, []byte(topic+" data"))
		err = eb.Publish(context.Background(), event)
		require.NoError(t, err)
	}

	// Collect received events
	receivedEvents := make([]*Event, 0, 3)
	timeout := time.After(5 * time.Second)

	for i := 0; i < 3; i++ {
		select {
		case event := <-eventChan:
			receivedEvents = append(receivedEvents, event)
		case <-timeout:
			t.Fatal("Timeout waiting for events")
		}
	}

	// Check if we received all 3 events
	assert.Equal(t, 3, len(receivedEvents), "Expected to receive 3 events")

	// Check if the received events match the published ones
	for _, event := range receivedEvents {
		assert.Contains(t, sportsTopics, event.Topic, "Unexpected topic received")
		assert.Equal(t, []byte(event.Topic+" data"), event.Data, "Unexpected data for topic %s", event.Topic)
	}

	// Clean up
	eb.Unsubscribe("sports/+", eventChan)

	// Verify that the events are in etcd
	for _, topic := range sportsTopics {
		history, err := eb.GetHistory(context.Background(), topic, 1)
		require.NoError(t, err)
		assert.Equal(t, 1, len(history), "Expected 1 event in history for topic %s", topic)
		if len(history) > 0 {
			assert.Equal(t, topic, history[0].Topic)
			assert.Equal(t, []byte(topic+" data"), history[0].Data)
		}
	}
}

func TestUnsubscribe(t *testing.T) {
	mockClient := new(MockEtcdClient)
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
	mockClient := new(MockEtcdClient)
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
