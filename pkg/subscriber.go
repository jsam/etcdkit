package etcdkit

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Subscriber struct {
	eb            *EventBus
	topic         string
	ch            <-chan *Event
	sendCh        chan<- *Event
	ackPrefix     string
	client        etcdClientInterface
	pendingAcks   map[string]time.Time
	mu            sync.Mutex
	maxRetries    int
	retryInterval time.Duration
}

func NewSubscriber(eb *EventBus, topic string) (*Subscriber, error) {
	ch := make(chan *Event, 100)
	err := eb.Subscribe(topic, ch)
	if err != nil {
		return nil, err
	}

	return &Subscriber{
		eb:            eb,
		topic:         topic,
		ch:            ch,
		sendCh:        ch,
		ackPrefix:     fmt.Sprintf("%s/acks/%s", eb.prefix, topic),
		client:        eb.client,
		pendingAcks:   make(map[string]time.Time),
		maxRetries:    3,
		retryInterval: 5 * time.Second,
	}, nil
}

func (s *Subscriber) Receive(ctx context.Context) (*Event, error) {
	fmt.Println("Waiting for event...")
	select {
	case event := <-s.ch:
		s.mu.Lock()
		s.pendingAcks[event.ID] = time.Now()
		s.mu.Unlock()
		return event, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Subscriber) Acknowledge(ctx context.Context, eventID string) error {
	s.mu.Lock()
	delete(s.pendingAcks, eventID)
	s.mu.Unlock()

	_, err := s.client.Put(ctx, fmt.Sprintf("%s/%s", s.ackPrefix, eventID), "")
	return err
}

func (s *Subscriber) Start() {
	go s.retryUnacknowledged()
	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			event, err := s.Receive(ctx)
			cancel()
			if err != nil {
				if err != context.DeadlineExceeded {
					fmt.Printf("Error receiving event: %v\n", err)
				}
				continue
			}
			fmt.Printf("Received event: %s\n", string(event.Data))
			s.Acknowledge(context.Background(), event.ID)
		}
	}()
}

func (s *Subscriber) retryUnacknowledged() {
	ticker := time.NewTicker(s.retryInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		for eventID, timestamp := range s.pendingAcks {
			if time.Since(timestamp) > s.retryInterval*time.Duration(s.maxRetries) {
				delete(s.pendingAcks, eventID)
				continue
			}

			events, err := s.eb.GetHistory(context.Background(), s.topic, 1)
			if err != nil || len(events) == 0 {
				continue
			}

			select {
			case s.sendCh <- events[0]:
				// Successfully re-delivered
			default:
				// Channel is full, will retry next time
			}
		}
		s.mu.Unlock()
	}
}

func (s *Subscriber) Close() {
	s.eb.Unsubscribe(s.topic, s.sendCh)
}
