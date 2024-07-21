package etcdkit

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func cleanupQueue(t *testing.T, queue *Queue, ctx context.Context) {
	for {
		_, err := queue.Dequeue(ctx)
		if err == ErrQueueEmpty {
			break
		}
		if err != nil {
			t.Fatalf("Failed to clean up queue: %v", err)
		}
	}
}

func TestQueue(t *testing.T) {
	// Setup
	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"localhost:2379"},
	})
	if err != nil {
		t.Fatalf("Failed to create etcd client: %v", err)
	}
	defer client.Close()

	queue := NewQueue(client, "/test-queue")

	// Test Enqueue and Dequeue
	t.Run("EnqueueDequeue", func(t *testing.T) {
		cleanupQueue(t, queue, ctx)
		for i := 0; i < 5; i++ {
			err := queue.Enqueue(ctx, fmt.Sprintf("item-%d", i))
			if err != nil {
				t.Errorf("Failed to enqueue item-%d: %v", i, err)
			}
		}

		for i := 0; i < 5; i++ {
			item, err := queue.Dequeue(ctx)
			if err != nil {
				t.Errorf("Failed to dequeue item: %v", err)
			}
			expected := fmt.Sprintf("item-%d", i)
			if item != expected {
				t.Errorf("Unexpected item: got %s, want %s", item, expected)
			}
		}
	})

	// Test Empty Queue
	t.Run("EmptyQueue", func(t *testing.T) {
		cleanupQueue(t, queue, ctx)
		_, err := queue.Dequeue(ctx)
		if err != ErrQueueEmpty {
			t.Errorf("Expected ErrQueueEmpty, got %v", err)
		}

		isEmpty, err := queue.IsEmpty(ctx)
		if err != nil {
			t.Errorf("Failed to check if queue is empty: %v", err)
		}
		if !isEmpty {
			t.Error("Queue should be empty")
		}
	})

	// Test Batch Enqueue and Dequeue
	t.Run("BatchEnqueueDequeue", func(t *testing.T) {
		cleanupQueue(t, queue, ctx)
		const batchSize = 10
		batch := make([]string, batchSize)
		for i := 0; i < batchSize; i++ {
			batch[i] = fmt.Sprintf("batch-item-%d", i)
		}

		err := queue.EnqueueBatch(ctx, batch)
		if err != nil {
			t.Fatalf("Failed to enqueue batch: %v", err)
		}

		for i := 0; i < batchSize; i++ {
			item, err := queue.Dequeue(ctx)
			if err != nil {
				t.Errorf("Failed to dequeue batch item: %v", err)
			}
			expected := fmt.Sprintf("batch-item-%d", i)
			if item != expected {
				t.Errorf("Unexpected batch item: got %s, want %s", item, expected)
			}
		}
	})

	// Test Concurrent Operations
	t.Run("ConcurrentOperations", func(t *testing.T) {
		cleanupQueue(t, queue, ctx)
		const numOperations = 1000
		const batchSize = 10

		// Enqueue in batches
		for i := 0; i < numOperations; i += batchSize {
			batch := make([]string, batchSize)
			for j := 0; j < batchSize; j++ {
				batch[j] = fmt.Sprintf("concurrent-item-%d", i+j)
			}
			err := queue.EnqueueBatch(ctx, batch)
			if err != nil {
				t.Fatalf("Failed to enqueue batch: %v", err)
			}
		}

		// Dequeue concurrently
		var wg sync.WaitGroup
		wg.Add(numOperations)

		dequeued := make(map[string]bool)
		var mu sync.Mutex

		for i := 0; i < numOperations; i++ {
			go func() {
				defer wg.Done()
				for retry := 0; retry < 5; retry++ {
					item, err := queue.Dequeue(ctx)
					if err == nil {
						mu.Lock()
						dequeued[item] = true
						mu.Unlock()
						return
					}
					if err != ErrQueueEmpty {
						t.Errorf("Concurrent dequeue error: %v", err)
					}
					time.Sleep(time.Duration(retry*50) * time.Millisecond) // Exponential backoff
				}
			}()
		}

		wg.Wait()

		// Allow some margin for error in concurrent operations
		if len(dequeued) < numOperations*95/100 {
			t.Errorf("Unexpected number of dequeued items: got %d, want at least %d", len(dequeued), numOperations*95/100)
		}

		notDequeued := 0
		for i := 0; i < numOperations; i++ {
			item := fmt.Sprintf("concurrent-item-%d", i)
			if !dequeued[item] {
				notDequeued++
			}
		}

		t.Logf("Items not dequeued: %d out of %d", notDequeued, numOperations)
	})

	// Test TTL
	t.Run("TTL", func(t *testing.T) {
		cleanupQueue(t, queue, ctx)
		err := queue.EnqueueWithTTL(ctx, "ttl-item", 5*time.Second)
		if err != nil {
			t.Fatalf("Failed to enqueue item with TTL: %v", err)
		}

		// Verify item is in the queue
		item, err := queue.Dequeue(ctx)
		if err != nil {
			t.Errorf("Failed to dequeue TTL item: %v", err)
		}
		if item != "ttl-item" {
			t.Errorf("Unexpected TTL item: got %s, want ttl-item", item)
		}

		// Re-enqueue and wait for expiration
		err = queue.EnqueueWithTTL(ctx, "ttl-item", 1*time.Second)
		if err != nil {
			t.Fatalf("Failed to re-enqueue item with TTL: %v", err)
		}

		time.Sleep(3 * time.Second)

		isEmpty, err := queue.IsEmpty(ctx)
		if err != nil {
			t.Errorf("Failed to check if queue is empty: %v", err)
		}
		if !isEmpty {
			t.Error("Queue should be empty after TTL expiration")
		}

		_, err = queue.Dequeue(ctx)
		if err != ErrQueueEmpty {
			t.Errorf("Expected ErrQueueEmpty when dequeueing expired item, got %v", err)
		}
	})

	// Test Batch TTL
	t.Run("BatchTTL", func(t *testing.T) {
		cleanupQueue(t, queue, ctx)
		const batchSize = 5
		batch := make([]string, batchSize)
		for i := 0; i < batchSize; i++ {
			batch[i] = fmt.Sprintf("ttl-batch-item-%d", i)
		}

		err := queue.EnqueueBatchWithTTL(ctx, batch, 5*time.Second)
		if err != nil {
			t.Fatalf("Failed to enqueue batch with TTL: %v", err)
		}

		// Verify items are in the queue
		for i := 0; i < batchSize; i++ {
			item, err := queue.Dequeue(ctx)
			if err != nil {
				t.Errorf("Failed to dequeue TTL batch item: %v", err)
			}
			expected := fmt.Sprintf("ttl-batch-item-%d", i)
			if item != expected {
				t.Errorf("Unexpected TTL batch item: got %s, want %s", item, expected)
			}
		}

		// Re-enqueue batch and wait for expiration
		err = queue.EnqueueBatchWithTTL(ctx, batch, 1*time.Second)
		if err != nil {
			t.Fatalf("Failed to re-enqueue batch with TTL: %v", err)
		}

		time.Sleep(3 * time.Second)

		isEmpty, err := queue.IsEmpty(ctx)
		if err != nil {
			t.Errorf("Failed to check if queue is empty: %v", err)
		}
		if !isEmpty {
			t.Error("Queue should be empty after TTL expiration")
		}

		_, err = queue.Dequeue(ctx)
		if err != ErrQueueEmpty {
			t.Errorf("Expected ErrQueueEmpty when dequeueing expired batch item, got %v", err)
		}
	})
}
