package observability_test

import (
	"io"
	"net/http"
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
	cacheReadTokens := int64(3)
	inflight := gateway.InflightEvent{Client: "client-a", Model: "qwen", PriorityClass: domain.PriorityHigh, Backend: "gpu-1"}
	metrics.ClientInflight(inflight, 1)
	metrics.BackendInflight(inflight, 1)
	metrics.ClientInflight(inflight, -1)
	metrics.BackendInflight(inflight, -1)
	metrics.BackendInflight(inflight, 1)
	metrics.BackendInflight(inflight, -1)
	metrics.Complete(gateway.RequestEvent{
		RequestID: "request-id-must-not-be-a-label", Client: "client-a", Model: "qwen",
		Backend: "gpu-1", PriorityClass: domain.PriorityHigh, Status: 200,
		Duration: 250 * time.Millisecond, TTFT: 50 * time.Millisecond, RetryCount: 1,
		QueueWait: 4 * time.Millisecond, QueueOutcome: gateway.QueueSelected,
		Usage: &domain.TokenUsage{InputTokens: 12, OutputTokens: 7, CacheReadTokens: &cacheReadTokens},
	})
	metrics.Complete(gateway.RequestEvent{
		RequestID: "other-request-id", Client: "client-a", Model: "qwen",
		PriorityClass: domain.PriorityHigh, Status: 429, Reason: "gateway_overloaded",
		DecisionReason: gateway.DecisionPoolWaitingLimit,
		QueueWait:      12 * time.Millisecond, QueueOutcome: gateway.QueueRejected,
		Duration: time.Millisecond, UsageParseFailure: "json",
	})
	metrics.Complete(gateway.RequestEvent{
		RequestID: "disconnected-request-id", Client: "client-a", Model: "qwen",
		Backend: "gpu-1", PriorityClass: domain.PriorityHigh, Status: 502,
		Reason: "upstream_error", Duration: 10 * time.Millisecond, Disconnect: true,
		UsageParseFailure: "sse",
	})
	metrics.Complete(gateway.RequestEvent{
		RequestID: "usage-without-cache-request-id", Client: "client-a", Model: "qwen",
		PriorityClass: domain.PriorityHigh, Status: 200, Duration: time.Millisecond,
		Usage: &domain.TokenUsage{InputTokens: 4, OutputTokens: 2},
	})
	metrics.Complete(gateway.RequestEvent{
		RequestID: "unexpected-format-request-id", Client: "client-a", Model: "qwen",
		PriorityClass: domain.PriorityHigh, Status: 200, Duration: time.Millisecond,
		UsageParseFailure: "request-body-must-not-be-a-label",
	})
	metrics.UsagePersistenceFailure()
	metrics.UsagePersistenceFailure()
	metrics.SetBackend("qwen", "gpu-1", domain.BackendRuntime{
		Pressure: .4, Running: 3, Waiting: 1, KVCacheUsage: .7,
		CircuitState: domain.CircuitClosed, CircuitFailures: 4,
	})
	metrics.SetBackend("qwen", "gpu-open", domain.BackendRuntime{CircuitState: domain.CircuitOpen})
	metrics.SetBackend("qwen", "gpu-half-open", domain.BackendRuntime{CircuitState: domain.CircuitHalfOpen})
	metrics.SetPool("qwen", domain.PoolRuntime{
		State: domain.PoolSaturated, BestBackendPressure: 1.25,
		GatewayInflight: 3, TotalWaiting: 2.5, AvailableBackends: 2,
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
		"llmgw_backend_failures_total", "llmgw_retries_total", "llmgw_backend_circuit_state",
		"llmgw_backend_circuit_failures", "llmgw_pool_gateway_inflight",
		"llmgw_pool_waiting_requests", "llmgw_pool_available_backends", "llmgw_pool_pressure",
		"llmgw_pool_state", "llmgw_backend_selected_total", "llmgw_queue_wait_seconds",
	} {
		if !strings.Contains(text, family) {
			t.Fatalf("metrics missing %s:\n%s", family, text)
		}
	}
	for _, sample := range []string{
		`llmgw_input_tokens_total{client="client-a",model="qwen"} 16`,
		`llmgw_output_tokens_total{client="client-a",model="qwen"} 9`,
		`llmgw_cache_read_tokens_total{client="client-a",model="qwen"} 3`,
		`llmgw_usage_parse_failures_total{format="json"} 1`,
		`llmgw_usage_parse_failures_total{format="sse"} 1`,
		`llmgw_usage_persistence_failures_total 2`,
		`llmgw_backend_selected_total{backend="gpu-1",model="qwen"} 2`,
		`llmgw_pool_pressure{model="qwen"} 1.25`,
		`llmgw_pool_state{model="qwen",state="saturated"} 1`,
		`llmgw_pool_state{model="qwen",state="normal"} 0`,
		`llmgw_requests_rejected_total{client="client-a",model="qwen",priority_class="high",reason="pool_waiting_limit"} 1`,
		`llmgw_queue_wait_seconds_count{model="qwen",outcome="rejected",priority_class="high"} 1`,
		`llmgw_queue_wait_seconds_count{model="qwen",outcome="selected",priority_class="high"} 1`,
	} {
		if !strings.Contains(text, sample+"\n") {
			t.Fatalf("metrics missing exact sample %q:\n%s", sample, text)
		}
	}
	if strings.Contains(text, `llmgw_usage_parse_failures_total{format="unknown"}`) {
		t.Fatalf("unsupported usage parse format should not increment a counter:\n%s", text)
	}
	for _, requestID := range []string{
		"request-id-must-not-be-a-label", "other-request-id", "disconnected-request-id",
		"usage-without-cache-request-id", "unexpected-format-request-id", "request-body-must-not-be-a-label",
	} {
		if strings.Contains(text, requestID) {
			t.Fatalf("high-cardinality or secret label leaked %q:\n%s", requestID, text)
		}
	}
	if strings.Contains(text, "llmgw_abcd") {
		t.Fatalf("high-cardinality or secret label leaked:\n%s", text)
	}
	for _, sample := range []string{
		`llmgw_backend_circuit_state{backend="gpu-1",model="qwen"} 0`,
		`llmgw_backend_circuit_state{backend="gpu-open",model="qwen"} 1`,
		`llmgw_backend_circuit_state{backend="gpu-half-open",model="qwen"} 2`,
		`llmgw_backend_circuit_failures{backend="gpu-1",model="qwen"} 4`,
		`llmgw_pool_gateway_inflight{model="qwen"} 3`,
		`llmgw_pool_waiting_requests{model="qwen"} 2.5`,
		`llmgw_pool_available_backends{model="qwen"} 2`,
	} {
		if !strings.Contains(text, sample) {
			t.Fatalf("metrics missing sample %q:\n%s", sample, text)
		}
	}
}

func TestMetricsUnmanagedCircuitEncodingFollowsTopologyLifecycle(t *testing.T) {
	metrics := observability.NewMetrics()
	backend := observability.BackendRuntimeMetric{
		Model: "qwen", Backend: "gpu-disabled", Runtime: domain.BackendRuntime{BackendID: 7},
	}
	metrics.PublishRuntime(nil, []observability.BackendRuntimeMetric{backend}, nil)
	text := scrapeRuntimeMetrics(t, metrics)
	for _, sample := range []string{
		`# HELP llmgw_backend_circuit_state Backend circuit state (unmanaged/unknown=-1, closed=0, open=1, half_open=2).`,
		`llmgw_backend_circuit_state{backend="gpu-disabled",model="qwen"} -1`,
	} {
		if !strings.Contains(text, sample) {
			t.Fatalf("unmanaged circuit metrics missing %q:\n%s", sample, text)
		}
	}

	metrics.PublishRuntime(nil, nil, nil)
	text = scrapeRuntimeMetrics(t, metrics)
	if strings.Contains(text, `llmgw_backend_circuit_state{backend="gpu-disabled",model="qwen"}`) {
		t.Fatalf("removed unmanaged circuit series survived topology cleanup:\n%s", text)
	}

	backend.Runtime.CircuitState = domain.CircuitClosed
	metrics.PublishRuntime(nil, []observability.BackendRuntimeMetric{backend}, nil)
	text = scrapeRuntimeMetrics(t, metrics)
	if sample := `llmgw_backend_circuit_state{backend="gpu-disabled",model="qwen"} 0`; !strings.Contains(text, sample) {
		t.Fatalf("recreated managed closed circuit sample missing %q:\n%s", sample, text)
	}
}

func scrapeRuntimeMetrics(t *testing.T, metrics *observability.Metrics) string {
	t.Helper()
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return response.Body.String()
}
