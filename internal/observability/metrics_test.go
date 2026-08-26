package observability_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/observability"
)

func TestMetricsExposeRequiredFamiliesWithoutHighCardinalityLabels(t *testing.T) {
	metrics := observability.NewMetrics()
	inflight := gateway.InflightEvent{Client: "client-a", Model: "qwen", PriorityClass: domain.PriorityHigh, Backend: "gpu-1"}
	metrics.ClientInflight(inflight, 1)
	metrics.BackendInflight(inflight, 1)
	metrics.ClientInflight(inflight, -1)
	metrics.BackendInflight(inflight, -1)
	metrics.Complete(gateway.RequestEvent{
		RequestID: "request-id-must-not-be-a-label", Client: "client-a", Model: "qwen",
		Backend: "gpu-1", PriorityClass: domain.PriorityHigh, Status: 200,
		Duration: 250 * time.Millisecond, TTFT: 50 * time.Millisecond, RetryCount: 1,
	})
	metrics.Complete(gateway.RequestEvent{
		RequestID: "other-request-id", Client: "client-a", Model: "qwen",
		PriorityClass: domain.PriorityHigh, Status: 429, Reason: "gateway_overloaded",
		Duration: time.Millisecond,
	})
	metrics.Complete(gateway.RequestEvent{
		RequestID: "disconnected-request-id", Client: "client-a", Model: "qwen",
		Backend: "gpu-1", PriorityClass: domain.PriorityHigh, Status: 502,
		Reason: "upstream_error", Duration: 10 * time.Millisecond, Disconnect: true,
	})
	metrics.SetBackend("qwen", "gpu-1", domain.BackendRuntime{
		Pressure: .4, Running: 3, Waiting: 1, KVCacheUsage: .7,
	})

	server := httptest.NewServer(metrics.Handler())
	defer server.Close()
	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	text := string(body)
	for _, family := range []string{
		"llmgw_requests_total", "llmgw_requests_inflight", "llmgw_requests_rejected_total",
		"llmgw_client_inflight", "llmgw_backend_requests_inflight", "llmgw_backend_pressure",
		"llmgw_backend_running_requests", "llmgw_backend_waiting_requests", "llmgw_backend_kv_cache_usage",
		"llmgw_request_duration_seconds", "llmgw_ttft_seconds", "llmgw_stream_disconnects_total",
		"llmgw_backend_failures_total", "llmgw_retries_total",
	} {
		if !strings.Contains(text, family) {
			t.Fatalf("metrics missing %s:\n%s", family, text)
		}
	}
	if strings.Contains(text, "request-id-must-not-be-a-label") || strings.Contains(text, "llmgw_abcd") {
		t.Fatalf("high-cardinality or secret label leaked:\n%s", text)
	}
}
