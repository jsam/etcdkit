package etcstruct

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Stack struct {
	client *clientv3.Client
	prefix string
}

func (s *Stack) Push(ctx context.Context, value string) error {
	lease, _ := s.client.Grant(ctx, 3600) // 1 hour TTL
	_, err := s.client.Put(ctx, fmt.Sprintf("%s/%d", s.prefix, time.Now().UnixNano()), value, clientv3.WithLease(lease.ID))
	return err
}

func (s *Stack) Pop(ctx context.Context) (string, error) {
	resp, err := s.client.Get(ctx, s.prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend), clientv3.WithLimit(1))
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("queue is empty")
	}
	_, err = s.client.Delete(ctx, string(resp.Kvs[0].Key))
	return string(resp.Kvs[0].Value), err
}
