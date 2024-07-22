package etcdkit

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func matchTopic(subscriptionTopic, eventTopic string) bool {
	fmt.Printf("Matching topics: subscription=%s, event=%s\n", subscriptionTopic, eventTopic)
	subParts := strings.Split(subscriptionTopic, "/")
	eventParts := strings.Split(eventTopic, "/")

	for i := 0; i < len(subParts); i++ {
		// If we've reached the end of eventParts but not subParts, it's only a match if the remaining subPart is "#"
		if i >= len(eventParts) {
			return subParts[i] == "#" && i == len(subParts)-1
		}

		switch subParts[i] {
		case "#":
			return i == len(subParts)-1 // '#' must be the last part
		case "+":
			continue // '+' matches any single part
		default:
			if subParts[i] != eventParts[i] {
				fmt.Printf("Mismatch at part %d: sub=%s, event=%s\n", i, subParts[i], eventParts[i])
				return false
			}
		}
	}

	// If we've reached here, all parts matched. It's a match if we've used up all eventParts.
	result := len(eventParts) == len(subParts)
	fmt.Printf("Match result: %v\n", result)
	return result
}

type EventBus struct {
	client      etcdClientInterface
	prefix      string
	subscribers map[string][]chan<- *Event
	mu          sync.RWMutex
}

func NewEventBus(endpoints []string, prefix string) (*EventBus, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &EventBus{
		client:      client,
		prefix:      prefix,
		subscribers: make(map[string][]chan<- *Event),
	}, nil
}

func (eb *EventBus) CheckConnection(ctx context.Context) error {
	_, err := eb.client.Get(ctx, "health_check")
	return err
}

func (eb *EventBus) Publish(ctx context.Context, event *Event) error {
	key := fmt.Sprintf("%s/%s/%s", eb.prefix, event.Topic, event.ID)
	value, err := event.Marshal()
	if err != nil {
		return err
	}

	// Use a lease to automatically delete old messages after 24 hours
	lease, err := eb.client.Grant(ctx, 24*60*60)
	if err != nil {
		return err
	}

	_, err = eb.client.Put(ctx, key, string(value), clientv3.WithLease(lease.ID))
	if err != nil {
		return err
	}
	fmt.Printf("Published event with key: %s\n", key)
	return err
}

func (eb *EventBus) Subscribe(topic string, ch chan<- *Event) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.subscribers[topic] = append(eb.subscribers[topic], ch)

	go eb.watch(topic, ch)

	return nil
}

func (eb *EventBus) watch(topic string, ch chan<- *Event) {
	topicCleaned := strings.ReplaceAll(topic, "+", "")
	topicCleaned = strings.ReplaceAll(topicCleaned, "#", "")
	watchPrefix := fmt.Sprintf("%s/%s", eb.prefix, topicCleaned)
	fmt.Printf("Watching prefix: %s\n", watchPrefix)

	watchChan := eb.client.Watch(context.Background(), watchPrefix, clientv3.WithPrefix())

	for response := range watchChan {
		if response.Err() != nil {
			fmt.Printf("Watch error: %v\n", response.Err())
			continue
		}
		for _, ev := range response.Events {
			fmt.Printf("Received event from etcd: Type=%s, Key=%s\n", ev.Type, string(ev.Kv.Key))
			if ev.Type == clientv3.EventTypePut {
				event, err := UnmarshalEvent(ev.Kv.Value)
				if err != nil {
					fmt.Printf("Error unmarshalling event: %v\n", err)
					continue
				}
				if matchTopic(topic, event.Topic) {
					fmt.Printf("Sending event to channel: %s\n", event.ID)
					ch <- event
				} else {
					fmt.Printf("Topic mismatch: subscription=%s, event=%s\n", topic, event.Topic)
				}
			}
		}
	}
}

func (eb *EventBus) Unsubscribe(topic string, ch chan<- *Event) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if channels, ok := eb.subscribers[topic]; ok {
		for i, subCh := range channels {
			if subCh == ch {
				eb.subscribers[topic] = append(channels[:i], channels[i+1:]...)
				break
			}
		}
		if len(eb.subscribers[topic]) == 0 {
			delete(eb.subscribers, topic)
		}
	}
}

func (eb *EventBus) Close() error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	for topic, channels := range eb.subscribers {
		for _, ch := range channels {
			close(ch)
		}
		delete(eb.subscribers, topic)
	}

	return eb.client.Close()
}

func (eb *EventBus) GetHistory(ctx context.Context, topic string, limit int64) ([]*Event, error) {
	resp, err := eb.client.Get(ctx, fmt.Sprintf("%s/%s", eb.prefix, topic),
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend),
		clientv3.WithLimit(limit))
	if err != nil {
		return nil, err
	}

	events := make([]*Event, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		event, err := UnmarshalEvent(kv.Value)
		if err != nil {
			continue
		}
		events = append(events, event)
	}

	return events, nil
}
