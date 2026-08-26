package loadgen_test

import (
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/loadgen"
)

func TestPercentilesUseNearestRank(t *testing.T) {
	got := loadgen.Summarize([]time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 100 * time.Millisecond})
	if got.P50 != 2*time.Millisecond || got.P95 != 100*time.Millisecond || got.P99 != 100*time.Millisecond {
		t.Fatalf("summary=%+v", got)
	}
}
