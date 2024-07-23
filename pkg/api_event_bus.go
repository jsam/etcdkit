package etcdkit

import (
	"context"
)

type EventBusInterface interface {
	Subscribe(topic string, ch chan<- *Event) error
	Unsubscribe(topic string, ch chan<- *Event)
	GetHistory(ctx context.Context, topic string, limit int64) ([]*Event, error)
	Publish(ctx context.Context, event *Event) error
	Prefix() string
	Client() EtcdClientInterface
}
