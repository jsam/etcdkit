package etcdkit

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type DistributedLock struct {
	client     EtcdClientInterface
	prefix     string
	lockName   string
	leaseID    clientv3.LeaseID
	cancelFunc context.CancelFunc
}

func NewDistributedLock(client EtcdClientInterface, prefix, lockName string) *DistributedLock {
	return &DistributedLock{
		client:   client,
		prefix:   prefix,
		lockName: lockName,
	}
}

func (dl *DistributedLock) Lock(ctx context.Context) error {
	key := fmt.Sprintf("%s/%s", dl.prefix, dl.lockName)

	// Create a lease
	lease, err := dl.client.Grant(ctx, 10) // 10 second TTL
	if err != nil {
		return fmt.Errorf("failed to create lease: %v", err)
	}

	// Try to acquire the lock
	txn := dl.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "", clientv3.WithLease(lease.ID))).
		Else(clientv3.OpGet(key))

	resp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	if !resp.Succeeded {
		return fmt.Errorf("failed to acquire lock: already held by another process")
	}

	dl.leaseID = lease.ID

	// Start a goroutine to keep the lease alive
	keepAliveCtx, cancel := context.WithCancel(context.Background())
	dl.cancelFunc = cancel
	go dl.keepAlive(keepAliveCtx)

	return nil
}

func (dl *DistributedLock) Unlock(ctx context.Context) error {
	if dl.cancelFunc != nil {
		dl.cancelFunc()
	}

	key := fmt.Sprintf("%s/%s", dl.prefix, dl.lockName)

	// Revoke the lease and delete the key
	_, err := dl.client.Revoke(ctx, dl.leaseID)
	if err != nil {
		return fmt.Errorf("failed to revoke lease: %v", err)
	}

	_, err = dl.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete lock key: %v", err)
	}

	return nil
}

func (dl *DistributedLock) keepAlive(ctx context.Context) {
	keepAliveChan, err := dl.client.KeepAlive(ctx, dl.leaseID)
	if err != nil {
		// Handle error (log it, etc.)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-keepAliveChan:
			// Lease renewed successfully
		}
	}
}
