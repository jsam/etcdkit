package etcdkit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestNewDistributedLock(t *testing.T) {
	mockClient := new(MockEtcdClient)
	lock := NewDistributedLock(mockClient, "test-prefix", "test-lock")

	assert.NotNil(t, lock)
	assert.Equal(t, mockClient, lock.client)
	assert.Equal(t, "test-prefix", lock.prefix)
	assert.Equal(t, "test-lock", lock.lockName)
}

func TestDistributedLock_Lock(t *testing.T) {
	mockClient := new(MockEtcdClient)
	lock := NewDistributedLock(mockClient, "test-prefix", "test-lock")

	// Setup expectations
	mockLease := &clientv3.LeaseGrantResponse{ID: 1234}
	mockClient.On("Grant", mock.Anything, int64(10)).Return(mockLease, nil)

	mockTxn := &MockTxn{}
	mockTxn.On("If", mock.Anything).Return(mockTxn)
	mockTxn.On("Then", mock.Anything).Return(mockTxn)
	mockTxn.On("Else", mock.Anything).Return(mockTxn)
	mockTxn.On("Commit").Return(&clientv3.TxnResponse{Succeeded: true}, nil)

	mockClient.On("Txn", mock.Anything).Return(mockTxn)

	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)
	keepAliveCallChan := make(chan struct{})
	mockClient.On("KeepAlive", mock.Anything, clientv3.LeaseID(1234)).Return((<-chan *clientv3.LeaseKeepAliveResponse)(keepAliveChan), nil).Run(func(args mock.Arguments) {
		close(keepAliveCallChan)
	})

	// Call Lock
	err := lock.Lock(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, clientv3.LeaseID(1234), lock.leaseID)
	assert.NotNil(t, lock.cancelFunc)

	// Wait for KeepAlive to be called or timeout
	select {
	case <-keepAliveCallChan:
		// KeepAlive was called
	case <-time.After(100 * time.Millisecond):
		t.Fatal("KeepAlive was not called within the expected timeframe")
	}

	// Verify expectations
	mockClient.AssertExpectations(t)
	mockTxn.AssertExpectations(t)

	// Clean up
	if lock.cancelFunc != nil {
		lock.cancelFunc()
	}
}
func TestDistributedLock_Unlock(t *testing.T) {
	mockClient := new(MockEtcdClient)
	lock := NewDistributedLock(mockClient, "test-prefix", "test-lock")
	lock.leaseID = clientv3.LeaseID(1234)
	lock.cancelFunc = func() {} // Mock cancelFunc

	// Setup expectations
	mockClient.On("Revoke", mock.Anything, clientv3.LeaseID(1234)).Return(&clientv3.LeaseRevokeResponse{}, nil)
	mockClient.On("Delete", mock.Anything, "test-prefix/test-lock", mock.Anything).Return(&clientv3.DeleteResponse{}, nil)

	// Call Unlock
	err := lock.Unlock(context.Background())

	assert.NoError(t, err)

	// Verify expectations
	mockClient.AssertExpectations(t)
}

func TestDistributedLock_keepAlive(t *testing.T) {
	mockClient := new(MockEtcdClient)
	lock := NewDistributedLock(mockClient, "test-prefix", "test-lock")
	lock.leaseID = clientv3.LeaseID(1234)

	// Setup expectations
	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)
	keepAliveCallChan := make(chan struct{})
	mockClient.On("KeepAlive", mock.Anything, clientv3.LeaseID(1234)).Return((<-chan *clientv3.LeaseKeepAliveResponse)(keepAliveChan), nil).Run(func(args mock.Arguments) {
		close(keepAliveCallChan)
	})

	// Start keepAlive in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lock.keepAlive(ctx)

	// Wait for KeepAlive to be called or timeout
	select {
	case <-keepAliveCallChan:
		// KeepAlive was called
	case <-time.After(100 * time.Millisecond):
		t.Fatal("KeepAlive was not called within the expected timeframe")
	}

	// Simulate a few keepalive responses
	for i := 0; i < 3; i++ {
		keepAliveChan <- &clientv3.LeaseKeepAliveResponse{}
		time.Sleep(10 * time.Millisecond)
	}

	// Cancel the context to stop keepAlive
	cancel()

	// Wait a bit to ensure keepAlive has stopped
	time.Sleep(50 * time.Millisecond)

	// Verify expectations
	mockClient.AssertExpectations(t)
}
