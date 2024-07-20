package etcdbus

import (
	"context"
	"fmt"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EventBus struct {
	client      *clientv3.Client
	prefix      string
	subscribers map[string][]chan *Event
	mu          sync.RWMutex
}

func NewEventBus(endpoints []string, prefix string) (*EventBus, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
	})
	if err != nil {
		return nil, err
	}

	return &EventBus{
		client:      client,
		prefix:      prefix,
		subscribers: make(map[string][]chan *Event),
	}, nil
}

func (eb *EventBus) Publish(ctx context.Context, event *Event) error {
	key := fmt.Sprintf("%s/%s/%s", eb.prefix, event.Topic, event.ID)
	value, err := event.Marshal()
	if err != nil {
		return err
	}

	_, err = eb.client.Put(ctx, key, string(value))
	return err
}

func (eb *EventBus) Subscribe(topic string) (<-chan *Event, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	ch := make(chan *Event, 100)
	eb.subscribers[topic] = append(eb.subscribers[topic], ch)

	go eb.watch(topic, ch)

	return ch, nil
}

func (eb *EventBus) watch(topic string, ch chan<- *Event) {
	watchChan := eb.client.Watch(context.Background(), fmt.Sprintf("%s/%s/", eb.prefix, topic), clientv3.WithPrefix())

	for response := range watchChan {
		for _, ev := range response.Events {
			if ev.Type == clientv3.EventTypePut {
				event, err := UnmarshalEvent(ev.Kv.Value)
				if err != nil {
					// Handle error (log it or send to an error channel)
					continue
				}
				ch <- event
			}
		}
	}
}

func (eb *EventBus) Unsubscribe(topic string, ch <-chan *Event) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if channels, ok := eb.subscribers[topic]; ok {
		for i, subCh := range channels {
			if subCh == ch {
				close(subCh)
				eb.subscribers[topic] = append(channels[:i], channels[i+1:]...)
				break
			}
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
