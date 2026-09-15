package integration_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
)

func TestAdmissionPriorityAndHysteresisAcceptance(t *testing.T) {
	h := newHarness(t)
	poolID := h.createPool("qwen")
	fake, backendID := h.addFake(poolID, "gpu-a", fakevllm.State{})
	h.waitBackend(backendID, eligible)
	_, criticalKey := h.createClient("critical-client", domain.PriorityCritical, -100, 4, poolID)
	_, highKey := h.createClient("high-client", domain.PriorityHigh, -10, 4, poolID)
	_, normalKey := h.createClient("normal-client", domain.PriorityNormal, 0, 4, poolID)
	_, backgroundKey := h.createClient("background-client", domain.PriorityBackground, 100, 4, poolID)

	fake.SetState(fakevllm.State{Running: 32, Waiting: 4, KVCacheUsage: 1})
	h.waitBackend(backendID, func(runtime domain.BackendRuntime) bool { return runtime.Pressure >= 1.4 })
	first := h.manager.PoolSnapshot(poolID, time.Now())
	if first.State != domain.PoolNormal {
		t.Fatalf("first spike observation changed state to %s", first.State)
	}
	time.Sleep(25 * time.Millisecond)
	if state := h.manager.PoolSnapshot(poolID, time.Now()).State; state != domain.PoolNormal {
		t.Fatalf("sub-enter-window spike changed state to %s", state)
	}
	h.waitPool(poolID, domain.PoolEmergency)
	_, beforeMetrics := h.public(http.MethodGet, "/metrics", "", "")

	for _, check := range []struct {
		name       string
		key        string
		wantStatus int
	}{
		{name: "critical", key: criticalKey, wantStatus: http.StatusOK},
		{name: "high", key: highKey, wantStatus: http.StatusOK},
		{name: "normal", key: normalKey, wantStatus: http.StatusTooManyRequests},
		{name: "background", key: backgroundKey, wantStatus: http.StatusTooManyRequests},
	} {
		t.Run(check.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodPost, h.server.URL+"/v1/completions", strings.NewReader(postBody("qwen", false)))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer "+check.key)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Vllm-Priority", "-999")
			response, err := h.client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != check.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, check.wantStatus)
			}
		})
	}
	records := fake.Snapshot().Requests
	if len(records) != 2 || records[0].Priority != "-100" || records[1].Priority != "-10" {
		t.Fatalf("upstream priorities = %+v", records)
	}
	_, afterMetrics := h.public(http.MethodGet, "/metrics", "", "")
	if got := metricCounterDelta(beforeMetrics, afterMetrics, "llmgw_requests_rejected_total", map[string]string{"reason": "priority_concurrency_limit"}); got != 2 {
		t.Fatalf("priority rejection delta = %v, want 2", got)
	}
	if got := metricCounterDelta(beforeMetrics, afterMetrics, "llmgw_backend_selected_total", map[string]string{"backend": "gpu-a"}); got != 2 {
		t.Fatalf("backend selection delta = %v, want 2", got)
	}

	fake.SetState(fakevllm.State{})
	h.waitBackend(backendID, func(runtime domain.BackendRuntime) bool { return runtime.Pressure < .55 })
	if state := h.manager.PoolSnapshot(poolID, time.Now()).State; state != domain.PoolEmergency {
		t.Fatalf("recovery started from %s, want emergency", state)
	}
	time.Sleep(40 * time.Millisecond)
	if state := h.manager.PoolSnapshot(poolID, time.Now()).State; state != domain.PoolEmergency {
		t.Fatalf("pool recovered before exit window: %s", state)
	}
	h.waitPool(poolID, domain.PoolSaturated)
	h.waitPool(poolID, domain.PoolBusy)
	h.waitPool(poolID, domain.PoolNormal)

	response, payload := h.public(http.MethodPost, "/v1/completions", backgroundKey, postBody("qwen", false))
	if response.StatusCode != http.StatusOK {
		t.Fatalf("background after recovery = %d %s", response.StatusCode, payload)
	}
	records = fake.Snapshot().Requests
	if records[len(records)-1].Priority != "100" {
		t.Fatalf("background upstream priority = %q", records[len(records)-1].Priority)
	}
}
