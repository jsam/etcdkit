package etcdkit_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	etcdkit "github.com/jsam/etcdkit/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	etcdEndpoint = "localhost:2379"
	testPrefix   = "/pubsub_test"
	testTopic    = "sports/football"
)

func TestEventBusEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	eb, err := etcdkit.NewEventBus([]string{etcdEndpoint}, testPrefix)
	require.NoError(t, err, "Failed to create EventBus")
	defer eb.Close()

	t.Run("PublishAndSubscribe", func(t *testing.T) {
		testPublishAndSubscribe(t, ctx, eb)
	})

	t.Run("WildcardSubscription", func(t *testing.T) {
		testWildcardSubscription(t, ctx, eb)
	})

	// t.Run("MultipleSubscribers", func(t *testing.T) {
	// 	testMultipleSubscribers(t, ctx, eb)
	// })

	// t.Run("UnsubscribeAndResubscribe", func(t *testing.T) {
	// 	testUnsubscribeAndResubscribe(t, ctx, eb)
	// })

	// t.Run("HistoryRetrieval", func(t *testing.T) {
	// 	testHistoryRetrieval(t, ctx, eb)
	// })

	// t.Run("AcknowledgementAndRetry", func(t *testing.T) {
	// 	testAcknowledgementAndRetry(t, ctx, eb)
	// })

	// t.Run("ConcurrentPublishSubscribe", func(t *testing.T) {
	// 	testConcurrentPublishSubscribe(t, ctx, eb)
	// })
}

func testPublishAndSubscribe(t *testing.T, ctx context.Context, eb *etcdkit.EventBus) {
	sub, err := etcdkit.NewSubscriber(eb, testTopic)
	require.NoError(t, err, "Failed to create subscriber")
	defer sub.Close()

	eventCount := 5
	receivedEvents := make(chan *etcdkit.Event, eventCount)

	// Start receiving events in a separate goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < eventCount; i++ {
			event, err := sub.Receive(ctx)
			if err != nil {
				if err.Error() == "subscriber closed" {
					return
				}
				t.Errorf("Failed to receive event: %v", err)
				return
			}
			receivedEvents <- event
			err = sub.Acknowledge(ctx, event.ID)
			if err != nil {
				t.Errorf("Failed to acknowledge event: %v", err)
			}
		}
	}()

	// Publish events
	for i := 0; i < eventCount; i++ {
		event := etcdkit.NewEvent(testTopic, []byte(fmt.Sprintf("Event %d", i)))
		err := eb.Publish(ctx, event)
		require.NoError(t, err, "Failed to publish event")
		t.Logf("Published event: %s", string(event.Data))
	}

	// Collect and verify received events
	receivedMap := make(map[string]bool)
	timeout := time.After(10 * time.Second)
	for i := 0; i < eventCount; i++ {
		select {
		case event := <-receivedEvents:
			t.Logf("Received event: %s", string(event.Data))
			receivedMap[string(event.Data)] = true
		case <-timeout:
			t.Fatalf("Timeout waiting for events. Received %d out of %d", i, eventCount)
		}
	}

	// Close the subscriber and wait for the receiving goroutine to finish
	sub.Close()
	wg.Wait()

	assert.Len(t, receivedMap, eventCount, "Did not receive expected number of events")
	for i := 0; i < eventCount; i++ {
		assert.True(t, receivedMap[fmt.Sprintf("Event %d", i)], "Did not receive event %d", i)
	}
}

func testWildcardSubscription(t *testing.T, ctx context.Context, eb *etcdkit.EventBus) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	sub, err := etcdkit.NewSubscriber(eb, "sports/+")
	require.NoError(t, err, "Failed to create wildcard subscriber")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sub.Start(ctx) // Modify the Start method to accept a context
	}()

	time.Sleep(2 * time.Second) // Ensure subscriber is ready

	topics := []string{"sports/football", "sports/basketball", "sports/tennis"}
	eventCount := len(topics)
	receivedEvents := make(chan *etcdkit.Event, eventCount)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < eventCount; i++ {
			t.Logf("Waiting for event %d", i)
			event, err := sub.Receive(ctx)
			if err != nil {
				if err == context.DeadlineExceeded || err == context.Canceled {
					t.Logf("Context done while waiting for event %d", i)
					return
				}
				t.Errorf("Error receiving event %d: %v", i, err)
				return
			}
			t.Logf("Received event %d for topic: %s", i, event.Topic)
			receivedEvents <- event
		}
	}()

	for _, topic := range topics {
		event := etcdkit.NewEvent(topic, []byte(fmt.Sprintf("Event for %s", topic)))
		err := eb.Publish(ctx, event)
		require.NoError(t, err, "Failed to publish event")
		t.Logf("Published event for topic: %s", topic)
	}

	receivedTopics := make(map[string]bool)
	for i := 0; i < eventCount; i++ {
		select {
		case event := <-receivedEvents:
			t.Logf("Processing received event for topic: %s", event.Topic)
			receivedTopics[event.Topic] = true
			err := sub.Acknowledge(ctx, event.ID)
			require.NoError(t, err, "Failed to acknowledge event")
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for all events")
		}
	}

	sub.Close()
	wg.Wait() // Wait for all goroutines to finish

	for _, topic := range topics {
		assert.True(t, receivedTopics[topic], "Did not receive event for topic: %s", topic)
	}
}

// func testMultipleSubscribers(t *testing.T, ctx context.Context, eb *etcdkit.EventBus) {
// 	sub1, err := etcdkit.NewSubscriber(eb, testTopic)
// 	require.NoError(t, err, "Failed to create subscriber 1")
// 	defer sub1.Close()
// 	sub1.Start()

// 	sub2, err := etcdkit.NewSubscriber(eb, testTopic)
// 	require.NoError(t, err, "Failed to create subscriber 2")
// 	defer sub2.Close()
// 	sub2.Start()

// 	time.Sleep(time.Second) // Ensure subscribers are ready

// 	eventCount := 3
// 	receivedEvents1 := make(chan *etcdkit.Event, eventCount)
// 	receivedEvents2 := make(chan *etcdkit.Event, eventCount)

// 	go func() {
// 		for i := 0; i < eventCount; i++ {
// 			event, err := sub1.Receive(ctx)
// 			if err == nil {
// 				receivedEvents1 <- event
// 				sub1.Acknowledge(ctx, event.ID)
// 			}
// 		}
// 	}()

// 	go func() {
// 		for i := 0; i < eventCount; i++ {
// 			event, err := sub2.Receive(ctx)
// 			if err == nil {
// 				receivedEvents2 <- event
// 				sub2.Acknowledge(ctx, event.ID)
// 			}
// 		}
// 	}()

// 	for i := 0; i < eventCount; i++ {
// 		event := etcdkit.NewEvent(testTopic, []byte(fmt.Sprintf("MultiSub Event %d", i)))
// 		err := eb.Publish(ctx, event)
// 		require.NoError(t, err, "Failed to publish event")
// 	}

// 	for i := 0; i < eventCount; i++ {
// 		select {
// 		case event1 := <-receivedEvents1:
// 			assert.Equal(t, fmt.Sprintf("MultiSub Event %d", i), string(event1.Data))
// 		case <-time.After(5 * time.Second):
// 			t.Fatalf("Timeout waiting for event %d on subscriber 1", i)
// 		}

// 		select {
// 		case event2 := <-receivedEvents2:
// 			assert.Equal(t, fmt.Sprintf("MultiSub Event %d", i), string(event2.Data))
// 		case <-time.After(5 * time.Second):
// 			t.Fatalf("Timeout waiting for event %d on subscriber 2", i)
// 		}
// 	}
// }

// func testUnsubscribeAndResubscribe(t *testing.T, ctx context.Context, eb *etcdkit.EventBus) {
// 	sub, err := etcdkit.NewSubscriber(eb, testTopic)
// 	require.NoError(t, err, "Failed to create subscriber")
// 	sub.Start()

// 	time.Sleep(time.Second) // Ensure subscriber is ready

// 	event1 := etcdkit.NewEvent(testTopic, []byte("Before unsubscribe"))
// 	err = eb.Publish(ctx, event1)
// 	require.NoError(t, err, "Failed to publish event")

// 	receivedEvent, err := sub.Receive(ctx)
// 	require.NoError(t, err, "Failed to receive event")
// 	assert.Equal(t, "Before unsubscribe", string(receivedEvent.Data))
// 	sub.Acknowledge(ctx, receivedEvent.ID)

// 	sub.Close()

// 	event2 := etcdkit.NewEvent(testTopic, []byte("During unsubscribe"))
// 	err = eb.Publish(ctx, event2)
// 	require.NoError(t, err, "Failed to publish event")

// 	// Wait a bit to ensure the event is not received
// 	time.Sleep(2 * time.Second)

// 	sub, err = etcdkit.NewSubscriber(eb, testTopic)
// 	require.NoError(t, err, "Failed to create new subscriber")
// 	sub.Start()
// 	defer sub.Close()

// 	time.Sleep(time.Second) // Ensure subscriber is ready

// 	event3 := etcdkit.NewEvent(testTopic, []byte("After resubscribe"))
// 	err = eb.Publish(ctx, event3)
// 	require.NoError(t, err, "Failed to publish event")

// 	receivedEvent, err = sub.Receive(ctx)
// 	require.NoError(t, err, "Failed to receive event after resubscribe")
// 	assert.Equal(t, "After resubscribe", string(receivedEvent.Data))
// 	sub.Acknowledge(ctx, receivedEvent.ID)
// }

// func testHistoryRetrieval(t *testing.T, ctx context.Context, eb *etcdkit.EventBus) {
// 	eventCount := 10
// 	for i := 0; i < eventCount; i++ {
// 		event := etcdkit.NewEvent(testTopic, []byte(fmt.Sprintf("History Event %d", i)))
// 		err := eb.Publish(ctx, event)
// 		require.NoError(t, err, "Failed to publish event")
// 	}

// 	time.Sleep(2 * time.Second) // Allow time for events to be stored

// 	history, err := eb.GetHistory(ctx, testTopic, int64(eventCount/2))
// 	require.NoError(t, err, "Failed to fetch history")
// 	assert.Len(t, history, eventCount/2, "Unexpected number of historical events")

// 	for i, event := range history {
// 		expectedData := fmt.Sprintf("History Event %d", eventCount-1-i)
// 		assert.Equal(t, expectedData, string(event.Data))
// 	}
// }

// func testAcknowledgementAndRetry(t *testing.T, ctx context.Context, eb *etcdkit.EventBus) {
// 	sub, err := etcdkit.NewSubscriber(eb, testTopic)
// 	require.NoError(t, err, "Failed to create subscriber")
// 	defer sub.Close()
// 	sub.Start()

// 	time.Sleep(time.Second) // Ensure subscriber is ready

// 	event := etcdkit.NewEvent(testTopic, []byte("Ack Test Event"))
// 	err = eb.Publish(ctx, event)
// 	require.NoError(t, err, "Failed to publish event")

// 	receivedEvent, err := sub.Receive(ctx)
// 	require.NoError(t, err, "Failed to receive event")
// 	assert.Equal(t, "Ack Test Event", string(receivedEvent.Data))

// 	// Don't acknowledge immediately, wait for a retry
// 	time.Sleep(6 * time.Second) // Longer than the retry interval

// 	retriedEvent, err := sub.Receive(ctx)
// 	require.NoError(t, err, "Failed to receive retried event")
// 	assert.Equal(t, "Ack Test Event", string(retriedEvent.Data))

// 	// Now acknowledge
// 	err = sub.Acknowledge(ctx, retriedEvent.ID)
// 	require.NoError(t, err, "Failed to acknowledge event")

// 	// Wait and check that no more retries occur
// 	time.Sleep(6 * time.Second)

// 	select {
// 	case <-time.After(6 * time.Second):
// 		// No event received, which is expected
// 	case event := <-sub.ReceiveCh(ctx):
// 		t.Fatalf("Unexpectedly received event after acknowledgement: %v", event)
// 	}
// }

// func testConcurrentPublishSubscribe(t *testing.T, ctx context.Context, eb *etcdkit.EventBus) {
// 	concurrency := 5
// 	eventsPerPublisher := 20

// 	var subscribers []*etcdkit.Subscriber
// 	for i := 0; i < concurrency; i++ {
// 		sub, err := etcdkit.NewSubscriber(eb, testTopic)
// 		require.NoError(t, err, "Failed to create subscriber")
// 		defer sub.Close()
// 		sub.Start()
// 		subscribers = append(subscribers, sub)
// 	}

// 	time.Sleep(time.Second) // Ensure subscribers are ready

// 	receivedEvents := make(chan *etcdkit.Event, concurrency*eventsPerPublisher)

// 	// Start subscribers
// 	for i := 0; i < concurrency; i++ {
// 		go func(sub *etcdkit.Subscriber) {
// 			for j := 0; j < eventsPerPublisher; j++ {
// 				event, err := sub.Receive(ctx)
// 				if err == nil {
// 					receivedEvents <- event
// 					sub.Acknowledge(ctx, event.ID)
// 				}
// 			}
// 		}(subscribers[i])
// 	}

// 	// Start publishers
// 	for i := 0; i < concurrency; i++ {
// 		go func(publisherID int) {
// 			for j := 0; j < eventsPerPublisher; j++ {
// 				event := etcdkit.NewEvent(testTopic, []byte(fmt.Sprintf("Event from publisher %d, number %d", publisherID, j)))
// 				err := eb.Publish(ctx, event)
// 				require.NoError(t, err, "Failed to publish event")
// 			}
// 		}(i)
// 	}

// 	// Collect and verify received events
// 	receivedCount := 0
// 	for receivedCount < concurrency*eventsPerPublisher {
// 		select {
// 		case <-receivedEvents:
// 			receivedCount++
// 		case <-time.After(10 * time.Second):
// 			t.Fatalf("Timeout waiting for events. Received %d out of %d", receivedCount, concurrency*eventsPerPublisher)
// 		}
// 	}

// 	assert.Equal(t, concurrency*eventsPerPublisher, receivedCount, "Did not receive expected number of events")
// }
