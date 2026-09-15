package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

type cleanupFunc func(context.Context, int) (bool, error)

func (f cleanupFunc) CleanupBatch(ctx context.Context, batch int) (bool, error) {
	return f(ctx, batch)
}

func TestCleanupDrainsBacklogFairly(t *testing.T) {
	remaining := []int{800, 600}
	var order []int
	cleaners := make([]coordination.CleanupBatcher, len(remaining))
	for i := range remaining {
		cleaners[i] = cleanupFunc(func(_ context.Context, batch int) (bool, error) {
			order = append(order, i)
			remaining[i] -= min(batch, remaining[i])
			return remaining[i] > 0, nil
		})
	}
	drainCoordinationCleanup(context.Background(), cleaners, 256)
	if remaining[0] != 0 || remaining[1] != 0 {
		t.Fatalf("backlog not drained: %v", remaining)
	}
	if len(order) != 7 || order[0] != 0 || order[1] != 1 || order[2] != 0 || order[3] != 1 {
		t.Fatalf("expected alternating bounded batches: %v", order)
	}
}

func TestCleanupFailureDoesNotPreventOtherCoordinator(t *testing.T) {
	failedCalls, remaining := 0, 700
	drainCoordinationCleanup(context.Background(), []coordination.CleanupBatcher{
		cleanupFunc(func(context.Context, int) (bool, error) {
			failedCalls++
			return true, errors.New("database unavailable")
		}),
		cleanupFunc(func(_ context.Context, batch int) (bool, error) {
			remaining -= min(batch, remaining)
			return remaining > 0, nil
		}),
	}, 256)
	if failedCalls != 1 || remaining != 0 {
		t.Fatalf("failure was retried or healthy backlog stalled: calls=%d remaining=%d", failedCalls, remaining)
	}
}

func TestCleanupStopsAtPassDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	calls := 0
	drainCoordinationCleanup(ctx, []coordination.CleanupBatcher{
		cleanupFunc(func(ctx context.Context, _ int) (bool, error) {
			calls++
			<-ctx.Done()
			return true, ctx.Err()
		}),
		cleanupFunc(func(context.Context, int) (bool, error) {
			t.Fatal("started cleanup after pass deadline")
			return false, nil
		}),
	}, 256)
	if calls != 1 || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("deadline was ignored: calls=%d err=%v", calls, ctx.Err())
	}
}
