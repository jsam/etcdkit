package etcdkit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrQueueEmpty = fmt.Errorf("queue is empty")

type Queue struct {
	client  *clientv3.Client
	prefix  string
	mu      sync.Mutex
	counter uint64
}

func NewQueue(client *clientv3.Client, prefix string) *Queue {
	return &Queue{
		client: client,
		prefix: prefix,
	}
}

func (q *Queue) Enqueue(ctx context.Context, value string) error {
	key := q.generateUniqueKey()
	_, err := q.client.Put(ctx, key, value)
	return err
}

func (q *Queue) EnqueueBatch(ctx context.Context, values []string) error {
	ops := make([]clientv3.Op, len(values))
	for i, value := range values {
		key := q.generateUniqueKey()
		ops[i] = clientv3.OpPut(key, value)
	}
	_, err := q.client.Txn(ctx).Then(ops...).Commit()
	return err
}

func (q *Queue) EnqueueWithTTL(ctx context.Context, value string, ttl time.Duration) error {
	lease, err := q.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return err
	}

	key := q.generateUniqueKey()
	_, err = q.client.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	return err
}

func (q *Queue) EnqueueBatchWithTTL(ctx context.Context, values []string, ttl time.Duration) error {
	lease, err := q.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return err
	}

	ops := make([]clientv3.Op, len(values))
	for i, value := range values {
		key := q.generateUniqueKey()
		ops[i] = clientv3.OpPut(key, value, clientv3.WithLease(lease.ID))
	}
	_, err = q.client.Txn(ctx).Then(ops...).Commit()
	return err
}

func (q *Queue) generateUniqueKey() string {
	return fmt.Sprintf("%s/%020d-%010d", q.prefix, time.Now().UnixNano(), atomic.AddUint64(&q.counter, 1))
}

func (q *Queue) Dequeue(ctx context.Context) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.dequeueFromEtcd(ctx)
}

func (q *Queue) dequeueFromEtcd(ctx context.Context) (string, error) {
	for i := 0; i < 5; i++ {
		resp, err := q.client.Get(ctx, q.prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(1))
		if err != nil {
			time.Sleep(time.Duration(i*50) * time.Millisecond) // Exponential backoff
			continue
		}
		if len(resp.Kvs) == 0 {
			return "", ErrQueueEmpty
		}

		key := string(resp.Kvs[0].Key)
		value := string(resp.Kvs[0].Value)

		txn := q.client.Txn(ctx)
		txn = txn.If(clientv3.Compare(clientv3.ModRevision(key), "=", resp.Kvs[0].ModRevision))
		txn = txn.Then(clientv3.OpDelete(key))

		txnResp, err := txn.Commit()
		if err != nil {
			time.Sleep(time.Duration(i*50) * time.Millisecond) // Exponential backoff
			continue
		}

		if txnResp.Succeeded {
			return value, nil
		}

		time.Sleep(time.Duration(i*50) * time.Millisecond) // Exponential backoff
	}
	return "", fmt.Errorf("failed to dequeue after retries")
}

func (q *Queue) IsEmpty(ctx context.Context) (bool, error) {
	resp, err := q.client.Get(ctx, q.prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return false, err
	}
	return resp.Count == 0, nil
}
