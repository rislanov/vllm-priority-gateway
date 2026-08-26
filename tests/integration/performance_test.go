package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"github.com/rislanov/vllm-priority-gateway/internal/loadgen"
)

func TestPerformanceSmoke(t *testing.T) {
	h := newHarness(t)
	poolID := h.createPool("qwen")
	_, backendID := h.addFake(poolID, "gpu-a", fakevllm.State{})
	h.waitBackend(backendID, eligible)
	_, key := h.createClient("load-client", domain.PriorityHigh, -10, 64, poolID)

	requestCount := 50
	if os.Getenv("LLMGW_RUN_PERF") == "1" {
		requestCount = 500
	}
	result, err := loadgen.Run(context.Background(), loadgen.Config{
		URL: h.server.URL, Key: key, Model: "qwen", Requests: requestCount, Parallelism: 16,
		PromptSize: 32, MaxTokens: 1, Seed: 42, HTTPClient: h.client,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Successes != requestCount || result.Overloaded != 0 || result.ServerErrors != 0 || result.Failures != 0 {
		t.Fatalf("load smoke = %+v", result)
	}
	t.Logf("gateway latency p50=%s p99=%s", result.Latency.P50, result.Latency.P99)
	if os.Getenv("LLMGW_RUN_PERF") == "1" && (result.Latency.P50 > 5*time.Millisecond || result.Latency.P99 > 20*time.Millisecond) {
		t.Fatalf("opt-in quiet-host latency budget exceeded: p50=%s p99=%s", result.Latency.P50, result.Latency.P99)
	}
}
