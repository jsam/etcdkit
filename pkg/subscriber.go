package etcdkit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type EventProcessor func(*Event) (*Event, error)

type ProcessingStrategy int

const (
	Broadcast ProcessingStrategy = iota
	Pipeline
)

type Subscriber struct {
	eb         EventBusInterface
	topic      string
	ch         chan *Event
	ackPrefix  string
	client     EtcdClientInterface
	mu         sync.Mutex
	processors []EventProcessor
	strategy   ProcessingStrategy
	stopCh     chan struct{}
	errorChan  chan error
	wg         sync.WaitGroup
}

func NewSubscriber(eb EventBusInterface, topic string) (*Subscriber, error) {
	ch := make(chan *Event, 100)
	err := eb.Subscribe(topic, ch)
	if err != nil {
		return nil, err
	}

	return &Subscriber{
		eb:        eb,
		topic:     topic,
		ch:        ch,
		ackPrefix: fmt.Sprintf("%s/acks/%s", eb.Prefix(), topic),
		client:    eb.Client(),
		stopCh:    make(chan struct{}),
		errorChan: make(chan error, 100),
		strategy:  Broadcast,
	}, nil
}

func (s *Subscriber) receive(ctx context.Context) (*Event, error) {
	select {
	case event := <-s.ch:
		return event, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Subscriber) acknowledge(ctx context.Context, eventID string) error {
	_, err := s.client.Put(ctx, fmt.Sprintf("%s/%s", s.ackPrefix, eventID), "")
	if err != nil && err != context.Canceled {
		return err
	}
	return nil
}
func (s *Subscriber) AttachProcessors(strategy ProcessingStrategy, processors ...EventProcessor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processors = processors
	s.strategy = strategy
}

func (s *Subscriber) Start() error {
	if len(s.processors) == 0 {
		return errors.New("no processors attached")
	}

	go func() {
		for {
			select {
			case <-s.stopCh:
				return
			default:
				s.wg.Add(1)
				go func() {
					defer s.wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					event, err := s.receive(ctx)
					cancel()
					if err != nil {
						if err != context.DeadlineExceeded {
							s.errorChan <- fmt.Errorf("error receiving event: %v", err)
						}
						return
					}

					if err := s.processEvent(event); err != nil {
						s.errorChan <- fmt.Errorf("error processing event: %v", err)
						return
					}

					if err := s.acknowledge(context.Background(), event.ID); err != nil {
						s.errorChan <- fmt.Errorf("error acknowledging event: %v", err)
					}
				}()
			}
		}
	}()

	return nil
}
func (s *Subscriber) processEvent(event *Event) error {
	switch s.strategy {
	case Broadcast:
		return s.broadcastEvent(event)
	case Pipeline:
		return s.pipelineEvent(event)
	default:
		return fmt.Errorf("unknown processing strategy")
	}
}

func (s *Subscriber) broadcastEvent(event *Event) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(s.processors))

	for _, processor := range s.processors {
		wg.Add(1)
		go func(p EventProcessor) {
			defer wg.Done()
			_, err := p(event)
			if err != nil {
				errCh <- err
			}
		}(processor)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Subscriber) pipelineEvent(event *Event) error {
	currentEvent := event
	for _, processor := range s.processors {
		processedEvent, err := processor(currentEvent)
		if err != nil {
			return err
		}
		currentEvent = processedEvent
	}
	return nil
}

func (s *Subscriber) Close() {
	close(s.stopCh)
	s.wg.Wait()
	s.eb.Unsubscribe(s.topic, s.ch)
	close(s.errorChan)
}
