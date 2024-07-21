package etcstruct

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Queue struct {
	client *clientv3.Client
	prefix string
}

func (q *Queue) Enqueue(ctx context.Context, value string) error {
	// Use Put with a lease to automatically delete old entries
	lease, _ := q.client.Grant(ctx, 3600) // 1 hour TTL
	_, err := q.client.Put(ctx, fmt.Sprintf("%s/%d", q.prefix, time.Now().UnixNano()), value, clientv3.WithLease(lease.ID))
	return err
}

func (q *Queue) Dequeue(ctx context.Context) (string, error) {
	resp, err := q.client.Get(ctx, q.prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(1))
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("queue is empty")
	}
	_, err = q.client.Delete(ctx, string(resp.Kvs[0].Key))
	return string(resp.Kvs[0].Value), err
}
