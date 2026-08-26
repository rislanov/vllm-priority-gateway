package integration_test

import (
	"net/http"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
)

func TestRoutingUsesLeastPressureAndHealthRecovery(t *testing.T) {
	h := newHarness(t)
	poolID := h.createPool("qwen")
	fakeA, backendA := h.addFake(poolID, "gpu-a", fakevllm.State{})
	fakeB, backendB := h.addFake(poolID, "gpu-b", fakevllm.State{Waiting: 4})
	h.waitBackend(backendA, eligible)
	h.waitBackend(backendB, eligible)
	_, key := h.createClient("router-client", domain.PriorityHigh, -10, 16, poolID)

	for index := 0; index < 100; index++ {
		response, payload := h.public(http.MethodPost, "/v1/completions", key, postBody("qwen", false))
		if response.StatusCode != http.StatusOK {
			t.Fatalf("request %d = %d %s", index, response.StatusCode, payload)
		}
	}
	if got := len(fakeA.Snapshot().Requests); got != 100 {
		t.Fatalf("backend A requests = %d", got)
	}
	if got := len(fakeB.Snapshot().Requests); got != 0 {
		t.Fatalf("higher-pressure backend B received %d requests", got)
	}

	fakeA.SetState(fakevllm.State{HealthFailures: 100})
	h.waitBackend(backendA, func(runtime domain.BackendRuntime) bool { return !runtime.Healthy })
	response, payload := h.public(http.MethodPost, "/v1/completions", key, postBody("qwen", false))
	if response.StatusCode != http.StatusOK || len(fakeB.Snapshot().Requests) != 1 {
		t.Fatalf("unhealthy exclusion = %d %s, B requests=%d", response.StatusCode, payload, len(fakeB.Snapshot().Requests))
	}

	fakeA.SetState(fakevllm.State{})
	h.waitBackend(backendA, eligible)
	response, payload = h.public(http.MethodPost, "/v1/completions", key, postBody("qwen", false))
	if response.StatusCode != http.StatusOK || len(fakeA.Snapshot().Requests) != 101 {
		t.Fatalf("recovered routing = %d %s, A requests=%d", response.StatusCode, payload, len(fakeA.Snapshot().Requests))
	}
}

func eligible(runtime domain.BackendRuntime) bool {
	return runtime.Healthy && runtime.MetricsFresh
}
