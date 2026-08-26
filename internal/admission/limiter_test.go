package admission_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
)

func TestLimiterNeverAdmitsAboveLimitAndReleaseIsIdempotent(t *testing.T) {
	limiter := admission.NewLimiter()
	const clientID = int64(42)
	const limit = 7
	start := make(chan struct{})
	release := make(chan struct{})
	results := make(chan bool, 100)
	var acquired atomic.Int64
	var group sync.WaitGroup
	group.Add(100)
	for range 100 {
		go func() {
			defer group.Done()
			<-start
			lease, ok := limiter.Acquire(clientID, limit)
			results <- ok
			if !ok {
				return
			}
			acquired.Add(1)
			<-release
			lease.Release()
			lease.Release()
		}()
	}
	close(start)
	for range 100 {
		<-results
	}
	if got := limiter.InFlight(clientID); got != limit {
		t.Fatalf("InFlight() = %d, want %d", got, limit)
	}
	close(release)
	group.Wait()
	if got := acquired.Load(); got != limit {
		t.Fatalf("acquired = %d, want %d", got, limit)
	}
	if got := limiter.InFlight(clientID); got != 0 {
		t.Fatalf("InFlight() after release = %d", got)
	}
}

func TestLimiterRejectsZeroLimit(t *testing.T) {
	limiter := admission.NewLimiter()
	if lease, ok := limiter.Acquire(1, 0); ok || lease != nil {
		t.Fatalf("Acquire() = %v, %v", lease, ok)
	}
}
