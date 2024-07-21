package etcdkit

// import (
// 	"context"
// 	"testing"
// 	"time"

// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/mock"
// 	clientv3 "go.etcd.io/etcd/client/v3"
// )

// // Mock EventBus
// type mockEventBus struct {
// 	mock.Mock
// }

// func (m *mockEventBus) Subscribe(topic string, ch chan<- *Event) error {
// 	args := m.Called(topic, ch)
// 	return args.Error(0)
// }

// func (m *mockEventBus) Unsubscribe(topic string, ch chan<- *Event) {
// 	m.Called(topic, ch)
// }

// func (m *mockEventBus) GetHistory(ctx context.Context, topic string, limit int64) ([]*Event, error) {
// 	args := m.Called(ctx, topic, limit)
// 	return args.Get(0).([]*Event), args.Error(1)
// }

// func TestNewSubscriber(t *testing.T) {
// 	mockEB := new(mockEventBus)
// 	mockEB.On("Subscribe", "test_topic", mock.Anything).Return(nil)

// 	sub, err := NewSubscriber(mockEB, "test_topic")
// 	assert.NoError(t, err)
// 	assert.NotNil(t, sub)
// 	assert.Equal(t, "test_topic", sub.topic)

// 	mockEB.AssertExpectations(t)
// }

// func TestReceive(t *testing.T) {
// 	mockEB := new(mockEventBus)
// 	sub, _ := NewSubscriber(mockEB, "test_topic")

// 	event := NewEvent("test_topic", []byte("test data"))

// 	go func() {
// 		time.Sleep(10 * time.Millisecond)
// 		sub.sendCh <- event
// 	}()

// 	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
// 	defer cancel()

// 	receivedEvent, err := sub.Receive(ctx)
// 	assert.NoError(t, err)
// 	assert.Equal(t, event, receivedEvent)
// }

// func TestReceiveTimeout(t *testing.T) {
// 	mockEB := new(mockEventBus)
// 	sub, _ := NewSubscriber(mockEB, "test_topic")

// 	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
// 	defer cancel()

// 	_, err := sub.Receive(ctx)
// 	assert.Error(t, err)
// 	assert.Equal(t, context.DeadlineExceeded, err)
// }

// func TestAcknowledge(t *testing.T) {
// 	mockClient := new(mockEtcdClient)
// 	sub := &Subscriber{
// 		client:    mockClient,
// 		ackPrefix: "test/acks/topic",
// 	}

// 	mockClient.On("Put", mock.Anything, "test/acks/topic/event1", "").Return(&clientv3.PutResponse{}, nil)

// 	err := sub.Acknowledge(context.Background(), "event1")
// 	assert.NoError(t, err)

// 	mockClient.AssertExpectations(t)
// }

// func TestRetryUnacknowledged(t *testing.T) {
// 	mockEB := new(mockEventBus)
// 	mockClient := new(mockEtcdClient)
// 	sub := &Subscriber{
// 		eb:            mockEB,
// 		client:        mockClient,
// 		topic:         "test_topic",
// 		pendingAcks:   make(map[string]time.Time),
// 		retryInterval: 10 * time.Millisecond,
// 		maxRetries:    2,
// 	}

// 	event := NewEvent("test_topic", []byte("test data"))
// 	sub.pendingAcks[event.ID] = time.Now().Add(-30 * time.Millisecond)

// 	mockEB.On("GetHistory", mock.Anything, "test_topic", int64(1)).Return([]*Event{event}, nil)

// 	go sub.retryUnacknowledged()

// 	time.Sleep(50 * time.Millisecond)

// 	receivedEvent := <-sub.ch
// 	assert.Equal(t, event, receivedEvent)

// 	mockEB.AssertExpectations(t)
// }

// func TestClose(t *testing.T) {
// 	mockEB := new(mockEventBus)
// 	sub, _ := NewSubscriber(mockEB, "test_topic")

// 	mockEB.On("Unsubscribe", "test_topic", sub.sendCh).Return()

// 	sub.Close()

// 	mockEB.AssertExpectations(t)
// }
