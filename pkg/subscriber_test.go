package etcdkit

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestNewSubscriber(t *testing.T) {
	t.Log("Starting TestNewSubscriber")

	// Set up mocks
	mockEB := new(MockEventBus)
	mockClient := new(MockEtcdClient)

	// Set up expectations
	mockEB.On("Subscribe", "test-topic", mock.Anything).Return(nil)
	mockEB.On("Prefix").Return("test-prefix")
	mockEB.On("Client").Return(mockClient)

	// Create subscriber
	sub, err := NewSubscriber(mockEB, "test-topic")

	// Assertions
	assert.NoError(t, err, "NewSubscriber should not return an error")
	assert.NotNil(t, sub, "Subscriber should not be nil")
	assert.Equal(t, "test-topic", sub.topic, "Topic should be set correctly")
	assert.Equal(t, "test-prefix/acks/test-topic", sub.ackPrefix, "AckPrefix should be set correctly")
	assert.Equal(t, mockClient, sub.client, "Client should be set correctly")
	assert.Equal(t, Broadcast, sub.strategy, "Default strategy should be Broadcast")

	// Verify mock expectations
	mockEB.AssertExpectations(t)

	t.Log("TestNewSubscriber completed successfully")
}

func TestSubscriber_Acknowledge(t *testing.T) {
	t.Log("Starting TestSubscriber_Acknowledge")

	// Set up mocks
	mockEB := new(MockEventBus)
	mockClient := new(MockEtcdClient)

	// Set up expectations
	mockEB.On("Subscribe", "test-topic", mock.Anything).Return(nil)
	mockEB.On("Prefix").Return("test-prefix")
	mockEB.On("Client").Return(mockClient)

	// Create subscriber
	sub, _ := NewSubscriber(mockEB, "test-topic")

	// Test successful acknowledgment
	ctx := context.Background()
	mockClient.On("Put", ctx, "test-prefix/acks/test-topic/event-1", "", mock.Anything).Return(&clientv3.PutResponse{}, nil)
	err := sub.acknowledge(ctx, "event-1")
	assert.NoError(t, err, "Acknowledge should not return an error")

	// Test acknowledgment with cancelled context
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	mockClient.On("Put", cancelledCtx, "test-prefix/acks/test-topic/event-2", "", mock.Anything).Return(&clientv3.PutResponse{}, context.Canceled)
	err = sub.acknowledge(cancelledCtx, "event-2")
	assert.NoError(t, err, "Acknowledge should not return an error even with cancelled context")

	// Test acknowledgment with other error
	mockClient.On("Put", ctx, "test-prefix/acks/test-topic/event-3", "", mock.Anything).Return(&clientv3.PutResponse{}, fmt.Errorf("some other error"))
	err = sub.acknowledge(ctx, "event-3")
	assert.Error(t, err, "Acknowledge should return an error for non-context.Canceled errors")
	assert.Contains(t, err.Error(), "some other error", "Error should contain the original error message")

	// Verify mock expectations
	mockEB.AssertExpectations(t)
	mockClient.AssertExpectations(t)

	t.Log("TestSubscriber_Acknowledge completed successfully")
}

func TestSubscriber_AttachProcessors(t *testing.T) {
	t.Log("Starting TestSubscriber_AttachProcessors")

	// Set up mocks
	mockEB := new(MockEventBus)
	mockClient := new(MockEtcdClient)

	// Set up expectations
	mockEB.On("Subscribe", "test-topic", mock.Anything).Return(nil)
	mockEB.On("Prefix").Return("test-prefix")
	mockEB.On("Client").Return(mockClient)

	// Create subscriber
	sub, _ := NewSubscriber(mockEB, "test-topic")

	// Create test processors
	processor1 := func(e *Event) (*Event, error) { return e, nil }
	processor2 := func(e *Event) (*Event, error) { return e, nil }

	// Attach processors with Pipeline strategy
	sub.AttachProcessors(Pipeline, processor1, processor2)

	assert.Equal(t, Pipeline, sub.strategy, "Strategy should be set to Pipeline")
	assert.Equal(t, 2, len(sub.processors), "Two processors should be attached")

	// Attach processors with Broadcast strategy
	sub.AttachProcessors(Broadcast, processor1)

	assert.Equal(t, Broadcast, sub.strategy, "Strategy should be set to Broadcast")
	assert.Equal(t, 1, len(sub.processors), "One processor should be attached")

	t.Log("TestSubscriber_AttachProcessors completed successfully")
}

func TestSubscriber_Start_NoProcessors(t *testing.T) {
	t.Log("Starting TestSubscriber_Start_NoProcessors")

	// Set up mocks
	mockEB := new(MockEventBus)
	mockClient := new(MockEtcdClient)

	// Set up expectations
	mockEB.On("Subscribe", "test-topic", mock.Anything).Return(nil)
	mockEB.On("Prefix").Return("test-prefix")
	mockEB.On("Client").Return(mockClient)

	// Create subscriber
	sub, _ := NewSubscriber(mockEB, "test-topic")

	// Try to start subscriber without attaching processors
	err := sub.Start()

	assert.Error(t, err, "Start should return an error when no processors are attached")
	assert.Contains(t, err.Error(), "no processors attached", "Error message should mention no processors")

	t.Log("TestSubscriber_Start_NoProcessors completed successfully")
}

func TestSubscriber_Close(t *testing.T) {
	t.Log("Starting TestSubscriber_Close")

	// Set up mocks
	mockEB := new(MockEventBus)
	mockClient := new(MockEtcdClient)

	// Set up expectations
	mockEB.On("Subscribe", "test-topic", mock.Anything).Return(nil)
	mockEB.On("Unsubscribe", "test-topic", mock.Anything).Return()
	mockEB.On("Prefix").Return("test-prefix")
	mockEB.On("Client").Return(mockClient)

	// Add expectation for Put method
	mockClient.On("Put", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&clientv3.PutResponse{}, nil)

	t.Log("Creating subscriber")
	// Create subscriber
	sub, err := NewSubscriber(mockEB, "test-topic")
	assert.Nil(t, err)

	// Use atomic counter for processor calls
	var processorCalled int32
	sub.AttachProcessors(Broadcast, func(e *Event) (*Event, error) {
		t.Log("Processor called")
		atomic.AddInt32(&processorCalled, 1)
		return e, nil
	})

	t.Log("Starting subscriber")
	// Start the subscriber
	err = sub.Start()
	assert.NoError(t, err)

	t.Log("Sending test event")
	// Send a test event
	testEvent := &Event{
		ID:    "test-event-id",
		Topic: "test-topic",
		Data:  []byte("test data"),
	}

	// Use a timeout when sending the event
	sendTimeout := time.After(1 * time.Second)
	select {
	case sub.ch <- testEvent:
		t.Log("Test event sent successfully")
	case <-sendTimeout:
		t.Fatal("Timed out while sending event")
	}

	t.Log("Waiting briefly for event processing")
	time.Sleep(100 * time.Millisecond)

	t.Log("Closing subscriber")
	// Run Close operation and wait for it to complete
	sub.Close()

	t.Log("Running assertions")

	// 1. Check if Unsubscribe was called with the correct topic
	mockEB.AssertCalled(t, "Unsubscribe", "test-topic", mock.Anything)
	t.Log("Unsubscribe assertion passed")

	// 2. Verify that Put was called with the correct key
	mockClient.AssertCalled(t, "Put", mock.Anything, "test-prefix/acks/test-topic/test-event-id", "", mock.Anything)
	t.Log("Put assertion passed")

	// 3. Verify all expectations on mocks were met
	mockEB.AssertExpectations(t)
	mockClient.AssertExpectations(t)
	t.Log("Mock expectations assertions passed")

	// 4. Check the final value of processorCalled
	finalProcessorCalls := atomic.LoadInt32(&processorCalled)
	assert.Equal(t, int32(1), finalProcessorCalls, "Processor should have been called once")
	t.Log("Processor call count assertion passed")

	t.Log("TestSubscriber_Close completed successfully")
}

func TestSubscriber_Start(t *testing.T) {
	mockEB := new(MockEventBus)
	mockClient := new(MockEtcdClient)

	// Set up expectations
	mockClient.On("Put", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&clientv3.PutResponse{}, nil)

	mockEB.On("Subscribe", "test-topic", mock.Anything).Return(nil)
	mockEB.On("Unsubscribe", "test-topic", mock.Anything).Return()
	mockEB.On("Prefix").Return("test-prefix")
	mockEB.On("Client").Return(mockClient)

	sub, err := NewSubscriber(mockEB, "test-topic")
	assert.Nil(t, err)
	// Attach processors
	processor1Called := false
	processor2Called := false
	sub.AttachProcessors(Broadcast,
		func(e *Event) (*Event, error) {
			processor1Called = true
			return e, nil
		},
		func(e *Event) (*Event, error) {
			processor2Called = true
			return e, nil
		},
	)

	// Start the subscriber
	err = sub.Start()
	assert.NoError(t, err)

	// Send a test event
	testEvent := &Event{
		ID:    "test-event-id",
		Topic: "test-topic",
		Data:  []byte("test data"),
	}

	sub.ch <- testEvent

	// Wait for processing to complete
	time.Sleep(500 * time.Millisecond)

	// Stop the subscriber
	sub.Close()

	// Wait for a bit to ensure all goroutines have finished
	time.Sleep(100 * time.Millisecond)

	// Assertions
	assert.True(t, processor1Called, "Processor 1 should have been called")
	assert.True(t, processor2Called, "Processor 2 should have been called")
	mockClient.AssertCalled(t, "Put", mock.Anything, "test-prefix/acks/test-topic/test-event-id", "", mock.Anything)

	// Check if Unsubscribe was called with the correct topic
	mockEB.AssertCalled(t, "Unsubscribe", "test-topic", sub.ch)

	// Check for any errors
	select {
	case err := <-sub.errorChan:
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Channel is closed.
	default:
		// No error, which is expected
	}

	mockEB.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

// Integration Tests
func TestSubscriber_Integration_MultipleEvents_Pipeline(t *testing.T) {
	// Setup a real EventBus with a test etcd cluster
	endpoints := []string{"localhost:2379"}
	eb, err := NewEventBus(endpoints, "test-prefix")
	assert.NoError(t, err)

	// Create a subscriber
	sub, err := NewSubscriber(eb, "test-topic")
	assert.NoError(t, err)

	// Create a channel to collect processed events
	processedEvents := make(chan *Event, 10)

	// Define processors
	processor1 := func(event *Event) (*Event, error) {
		event.Data = append(event.Data, []byte(" - processed by 1")...)
		return event, nil
	}

	processor2 := func(event *Event) (*Event, error) {
		event.Data = append(event.Data, []byte(" - processed by 2")...)
		processedEvents <- event // Collect the fully processed event
		return event, nil
	}

	// Attach processors with pipeline strategy
	sub.AttachProcessors(Pipeline, processor1, processor2)

	// Start the subscriber
	err = sub.Start()
	assert.NoError(t, err)

	// Publish multiple events
	for i := 0; i < 5; i++ {
		event := NewEvent("test-topic", []byte(fmt.Sprintf("test data %d", i)))
		err = eb.Publish(context.Background(), event)
		assert.NoError(t, err)
	}

	// Wait for events to be received and processed
	timeout := time.After(5 * time.Second)
	receivedCount := 0

	for receivedCount < 5 {
		select {
		case event := <-processedEvents:
			receivedCount++
			data := string(event.Data)
			assert.Contains(t, data, "test data")
			assert.Contains(t, data, "processed by 1")
			assert.Contains(t, data, "processed by 2")
		case <-timeout:
			t.Fatal("Timeout waiting for events")
		}
	}

	// Close the subscriber
	sub.Close()

	// Close the EventBus
	eb.Close()
}

func TestSubscriber_Integration_MultipleEvents_Broadcast(t *testing.T) {
	// Setup a real EventBus with a test etcd cluster
	endpoints := []string{"localhost:2379"}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	eb, err := NewEventBus(endpoints, "test-prefix-broadcast")
	require.NoError(t, err)
	//defer eb.Close()

	// Create a subscriber
	sub, err := NewSubscriber(eb, "test-topic")
	require.NoError(t, err)
	//defer sub.Close()

	// Create channels to collect processed events
	processedEvents1 := make(chan *Event, 10)
	processedEvents2 := make(chan *Event, 10)
	errorChan := make(chan error, 10)

	// Define processors
	processor1 := func(event *Event) (*Event, error) {
		newEvent := *event // Create a copy of the event
		newEvent.Data = append(newEvent.Data, []byte(" - processed by 1")...)
		processedEvents1 <- &newEvent
		return &newEvent, nil
	}
	processor2 := func(event *Event) (*Event, error) {
		newEvent := *event // Create a copy of the event
		newEvent.Data = append(newEvent.Data, []byte(" - processed by 2")...)
		processedEvents2 <- &newEvent
		return &newEvent, nil
	}

	// Attach processors with broadcast strategy
	sub.AttachProcessors(Broadcast, processor1, processor2)

	// Start the subscriber
	err = sub.Start()
	require.NoError(t, err)

	// Publish multiple events
	for i := 0; i < 5; i++ {
		event := NewEvent("test-topic", []byte(fmt.Sprintf("test data %d", i)))
		err = eb.Publish(ctx, event)
		require.NoError(t, err)
	}

	// Wait for events to be received and processed
	receivedCount1 := 0
	receivedCount2 := 0
	for receivedCount1 < 5 || receivedCount2 < 5 {
		select {
		case event := <-processedEvents1:
			receivedCount1++
			data := string(event.Data)
			assert.Contains(t, data, "test data")
			assert.Contains(t, data, "processed by 1")
			assert.NotContains(t, data, "processed by 2")
		case event := <-processedEvents2:
			receivedCount2++
			data := string(event.Data)
			assert.Contains(t, data, "test data")
			assert.Contains(t, data, "processed by 2")
			assert.NotContains(t, data, "processed by 1")
		case err := <-errorChan:
			t.Fatalf("Received error: %v", err)
		case <-ctx.Done():
			t.Fatal("Timeout waiting for events")
		}
	}

	assert.Equal(t, 5, receivedCount1, "Processor 1 should receive 5 events")
	assert.Equal(t, 5, receivedCount2, "Processor 2 should receive 5 events")
}
