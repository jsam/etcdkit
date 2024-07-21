package etcdkit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrStackEmpty = fmt.Errorf("stack is empty")

type Stack struct {
	client  *clientv3.Client
	prefix  string
	mu      sync.Mutex
	counter uint64
}

func NewStack(client *clientv3.Client, prefix string) *Stack {
	return &Stack{
		client: client,
		prefix: prefix,
	}
}

func (s *Stack) Push(ctx context.Context, value string) error {
	key := s.generateUniqueKey()
	_, err := s.client.Put(ctx, key, value)
	return err
}

func (s *Stack) PushBatch(ctx context.Context, values []string) error {
	ops := make([]clientv3.Op, len(values))
	for i, value := range values {
		key := s.generateUniqueKey()
		ops[i] = clientv3.OpPut(key, value)
	}
	_, err := s.client.Txn(ctx).Then(ops...).Commit()
	return err
}

func (s *Stack) PushWithTTL(ctx context.Context, value string, ttl time.Duration) error {
	lease, err := s.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return err
	}

	key := s.generateUniqueKey()
	_, err = s.client.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	return err
}

func (s *Stack) PushBatchWithTTL(ctx context.Context, values []string, ttl time.Duration) error {
	lease, err := s.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return err
	}

	ops := make([]clientv3.Op, len(values))
	for i, value := range values {
		key := s.generateUniqueKey()
		ops[i] = clientv3.OpPut(key, value, clientv3.WithLease(lease.ID))
	}
	_, err = s.client.Txn(ctx).Then(ops...).Commit()
	return err
}

func (s *Stack) generateUniqueKey() string {
	return fmt.Sprintf("%s/%020d-%010d", s.prefix, time.Now().UnixNano(), atomic.AddUint64(&s.counter, 1))
}

func (s *Stack) Pop(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.popFromEtcd(ctx)
}

func (s *Stack) popFromEtcd(ctx context.Context) (string, error) {
	for i := 0; i < 5; i++ {
		resp, err := s.client.Get(ctx, s.prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend), clientv3.WithLimit(1))
		if err != nil {
			time.Sleep(time.Duration(i*50) * time.Millisecond) // Exponential backoff
			continue
		}
		if len(resp.Kvs) == 0 {
			return "", ErrStackEmpty
		}

		key := string(resp.Kvs[0].Key)
		value := string(resp.Kvs[0].Value)

		txn := s.client.Txn(ctx)
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
	return "", fmt.Errorf("failed to pop after retries")
}

func (s *Stack) IsEmpty(ctx context.Context) (bool, error) {
	resp, err := s.client.Get(ctx, s.prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return false, err
	}
	return resp.Count == 0, nil
}
