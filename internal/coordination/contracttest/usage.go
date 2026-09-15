package contracttest

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

// LargeTokenCompletion uses a TPM-limited fixture to verify that valid int64
// usage fields cannot overflow into a token credit or a retry date in the past.
func LargeTokenCompletion(t *testing.T, coordinator coordination.AdmissionCoordinator, request func() coordination.AdmissionRequest) {
	t.Helper()
	ctx := context.Background()
	first, err := coordinator.Acquire(ctx, request())
	if err != nil || !first.Admitted() {
		t.Fatalf("initial admission=%+v err=%v", first, err)
	}
	usage := &coordination.TokenUsage{InputTokens: math.MaxInt64, OutputTokens: 1}
	if _, err := coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: first.Lease.Identity(), Usage: usage}}); err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	blocked, err := coordinator.Acquire(ctx, request())
	if err != nil || blocked.Reason != coordination.ReasonTPMExhausted {
		t.Fatalf("large token debit became credit: decision=%+v err=%v", blocked, err)
	}
	if blocked.RetryAt == nil || !blocked.RetryAt.After(before) {
		t.Fatalf("large debt overflowed retry time: %v", blocked.RetryAt)
	}
}
