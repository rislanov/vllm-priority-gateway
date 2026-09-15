package main

import (
	"context"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

// Maintenance has its own worker so draining retention backlogs cannot delay
// recovery or readiness publication. Each pass has a fixed total time budget.
func runCoordinationCleanup(ctx context.Context, cleaners ...coordination.CleanupBatcher) {
	cleaners = append([]coordination.CleanupBatcher(nil), cleaners...)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		passCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		drainCoordinationCleanup(passCtx, cleaners, 256)
		cancel()
		// A slow category may consume the entire budget; rotate the first turn
		// so the other coordinator still gets service on the next pass.
		if len(cleaners) > 1 {
			first := cleaners[0]
			copy(cleaners, cleaners[1:])
			cleaners[len(cleaners)-1] = first
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func drainCoordinationCleanup(ctx context.Context, cleaners []coordination.CleanupBatcher, batch int) {
	pending := append([]coordination.CleanupBatcher(nil), cleaners...)
	for len(pending) > 0 {
		next := make([]coordination.CleanupBatcher, 0, len(pending))
		for _, cleaner := range pending {
			if ctx.Err() != nil {
				return
			}
			more, err := cleaner.CleanupBatch(ctx, batch)
			if err == nil && more {
				next = append(next, cleaner)
			}
		}
		pending = next
	}
}
