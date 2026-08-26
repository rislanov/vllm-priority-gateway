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
	gatewayConfig := loadgen.Config{
		URL: h.server.URL, Key: key, Model: "qwen", Requests: requestCount, Parallelism: 16,
		PromptSize: 32, MaxTokens: 1, Seed: 42, HTTPClient: h.client,
	}
	directConfig := gatewayConfig
	directConfig.URL = h.upstreams[0].URL
	directConfig.Key = "direct-fake"
	directConfig.Model = "fake-model"
	directConfig.HTTPClient = h.client
	warmupGateway, warmupDirect := gatewayConfig, directConfig
	warmupGateway.Requests, warmupDirect.Requests = 20, 20
	if _, err := loadgen.Run(context.Background(), warmupGateway); err != nil {
		t.Fatal(err)
	}
	if _, err := loadgen.Run(context.Background(), warmupDirect); err != nil {
		t.Fatal(err)
	}
	direct, err := loadgen.Run(context.Background(), directConfig)
	if err != nil {
		t.Fatal(err)
	}
	result, err := loadgen.Run(context.Background(), gatewayConfig)
	if err != nil {
		t.Fatal(err)
	}
	if result.Successes != requestCount || result.Overloaded != 0 || result.ServerErrors != 0 || result.Failures != 0 {
		t.Fatalf("load smoke = %+v", result)
	}
	p50 := max(result.Latency.P50-direct.Latency.P50, 0)
	p99 := max(result.Latency.P99-direct.Latency.P99, 0)
	t.Logf("gateway-added latency p50=%s p99=%s (gateway=%s/%s direct=%s/%s)", p50, p99, result.Latency.P50, result.Latency.P99, direct.Latency.P50, direct.Latency.P99)
	if os.Getenv("LLMGW_RUN_PERF") == "1" && (p50 > 5*time.Millisecond || p99 > 20*time.Millisecond) {
		t.Fatalf("opt-in quiet-host gateway-added latency budget exceeded: p50=%s p99=%s", p50, p99)
	}
}
