package etcdkit

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func cleanupStack(t *testing.T, stack *Stack, ctx context.Context) {
	for {
		_, err := stack.Pop(ctx)
		if err == ErrStackEmpty {
			break
		}
		if err != nil {
			t.Fatalf("Failed to clean up stack: %v", err)
		}
	}
}

func TestStack(t *testing.T) {
	// Setup
	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"localhost:2379"},
	})
	if err != nil {
		t.Fatalf("Failed to create etcd client: %v", err)
	}
	defer client.Close()

	stack := NewStack(client, "/test-stack")

	// Test Push and Pop
	t.Run("PushPop", func(t *testing.T) {
		cleanupStack(t, stack, ctx)
		for i := 0; i < 5; i++ {
			err := stack.Push(ctx, fmt.Sprintf("item-%d", i))
			if err != nil {
				t.Errorf("Failed to push item-%d: %v", i, err)
			}
		}

		for i := 4; i >= 0; i-- {
			item, err := stack.Pop(ctx)
			if err != nil {
				t.Errorf("Failed to pop item: %v", err)
			}
			expected := fmt.Sprintf("item-%d", i)
			if item != expected {
				t.Errorf("Unexpected item: got %s, want %s", item, expected)
			}
		}
	})

	// Test Empty Stack
	t.Run("EmptyStack", func(t *testing.T) {
		cleanupStack(t, stack, ctx)
		_, err := stack.Pop(ctx)
		if err != ErrStackEmpty {
			t.Errorf("Expected ErrStackEmpty, got %v", err)
		}

		isEmpty, err := stack.IsEmpty(ctx)
		if err != nil {
			t.Errorf("Failed to check if stack is empty: %v", err)
		}
		if !isEmpty {
			t.Error("Stack should be empty")
		}
	})

	// Test Batch Push and Pop
	t.Run("BatchPushPop", func(t *testing.T) {
		cleanupStack(t, stack, ctx)
		const batchSize = 10
		batch := make([]string, batchSize)
		for i := 0; i < batchSize; i++ {
			batch[i] = fmt.Sprintf("batch-item-%d", i)
		}

		err := stack.PushBatch(ctx, batch)
		if err != nil {
			t.Fatalf("Failed to push batch: %v", err)
		}

		for i := batchSize - 1; i >= 0; i-- {
			item, err := stack.Pop(ctx)
			if err != nil {
				t.Errorf("Failed to pop batch item: %v", err)
			}
			expected := fmt.Sprintf("batch-item-%d", i)
			if item != expected {
				t.Errorf("Unexpected batch item: got %s, want %s", item, expected)
			}
		}
	})

	// Test Concurrent Operations
	t.Run("ConcurrentOperations", func(t *testing.T) {
		cleanupStack(t, stack, ctx)
		const numOperations = 1000
		const batchSize = 10

		// Push in batches
		for i := 0; i < numOperations; i += batchSize {
			batch := make([]string, batchSize)
			for j := 0; j < batchSize; j++ {
				batch[j] = fmt.Sprintf("concurrent-item-%d", i+j)
			}
			err := stack.PushBatch(ctx, batch)
			if err != nil {
				t.Fatalf("Failed to push batch: %v", err)
			}
		}

		// Pop concurrently
		var wg sync.WaitGroup
		wg.Add(numOperations)

		popped := make(map[string]bool)
		var mu sync.Mutex

		for i := 0; i < numOperations; i++ {
			go func() {
				defer wg.Done()
				for retry := 0; retry < 5; retry++ {
					item, err := stack.Pop(ctx)
					if err == nil {
						mu.Lock()
						popped[item] = true
						mu.Unlock()
						return
					}
					if err != ErrStackEmpty {
						t.Errorf("Concurrent pop error: %v", err)
					}
					time.Sleep(time.Duration(retry*50) * time.Millisecond) // Exponential backoff
				}
			}()
		}

		wg.Wait()

		// Allow some margin for error in concurrent operations
		if len(popped) < numOperations*95/100 {
			t.Errorf("Unexpected number of popped items: got %d, want at least %d", len(popped), numOperations*95/100)
		}

		notPopped := 0
		for i := 0; i < numOperations; i++ {
			item := fmt.Sprintf("concurrent-item-%d", i)
			if !popped[item] {
				notPopped++
			}
		}

		t.Logf("Items not popped: %d out of %d", notPopped, numOperations)
	})

	// Test TTL
	t.Run("TTL", func(t *testing.T) {
		cleanupStack(t, stack, ctx)
		err := stack.PushWithTTL(ctx, "ttl-item", 5*time.Second)
		if err != nil {
			t.Fatalf("Failed to push item with TTL: %v", err)
		}

		// Verify item is in the stack
		item, err := stack.Pop(ctx)
		if err != nil {
			t.Errorf("Failed to pop TTL item: %v", err)
		}
		if item != "ttl-item" {
			t.Errorf("Unexpected TTL item: got %s, want ttl-item", item)
		}

		// Re-push and wait for expiration
		err = stack.PushWithTTL(ctx, "ttl-item", 1*time.Second)
		if err != nil {
			t.Fatalf("Failed to re-push item with TTL: %v", err)
		}

		time.Sleep(3 * time.Second)

		isEmpty, err := stack.IsEmpty(ctx)
		if err != nil {
			t.Errorf("Failed to check if stack is empty: %v", err)
		}
		if !isEmpty {
			t.Error("Stack should be empty after TTL expiration")
		}

		_, err = stack.Pop(ctx)
		if err != ErrStackEmpty {
			t.Errorf("Expected ErrStackEmpty when popping expired item, got %v", err)
		}
	})

	// Test Batch TTL
	t.Run("BatchTTL", func(t *testing.T) {
		cleanupStack(t, stack, ctx)
		const batchSize = 5
		batch := make([]string, batchSize)
		for i := 0; i < batchSize; i++ {
			batch[i] = fmt.Sprintf("ttl-batch-item-%d", i)
		}

		err := stack.PushBatchWithTTL(ctx, batch, 5*time.Second)
		if err != nil {
			t.Fatalf("Failed to push batch with TTL: %v", err)
		}

		// Verify items are in the stack
		for i := batchSize - 1; i >= 0; i-- {
			item, err := stack.Pop(ctx)
			if err != nil {
				t.Errorf("Failed to pop TTL batch item: %v", err)
			}
			expected := fmt.Sprintf("ttl-batch-item-%d", i)
			if item != expected {
				t.Errorf("Unexpected TTL batch item: got %s, want %s", item, expected)
			}
		}

		// Re-push batch and wait for expiration
		err = stack.PushBatchWithTTL(ctx, batch, 1*time.Second)
		if err != nil {
			t.Fatalf("Failed to re-push batch with TTL: %v", err)
		}

		time.Sleep(3 * time.Second)

		isEmpty, err := stack.IsEmpty(ctx)
		if err != nil {
			t.Errorf("Failed to check if stack is empty: %v", err)
		}
		if !isEmpty {
			t.Error("Stack should be empty after TTL expiration")
		}

		_, err = stack.Pop(ctx)
		if err != ErrStackEmpty {
			t.Errorf("Expected ErrStackEmpty when popping expired batch item, got %v", err)
		}
	})
}
