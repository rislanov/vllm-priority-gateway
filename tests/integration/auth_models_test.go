package integration_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
)

func TestAuthenticationAndModelAccessAcceptance(t *testing.T) {
	h := newHarness(t)
	poolID := h.createPool("qwen")
	_, backendID := h.addFake(poolID, "gpu-a", fakevllm.State{})
	h.waitBackend(backendID, func(runtime domain.BackendRuntime) bool { return runtime.Healthy && runtime.MetricsFresh })
	clientID, validKey := h.createClient("allowed-client", domain.PriorityHigh, -10, 4, poolID)
	_, noAccessKey := h.createClient("no-access-client", domain.PriorityNormal, 0, 4)

	response, payload := h.public(http.MethodGet, "/v1/models", "llmgw_unknown", "")
	if response.StatusCode != http.StatusUnauthorized || !strings.Contains(string(payload), `"code":"invalid_api_key"`) {
		t.Fatalf("unknown key response = %d %s", response.StatusCode, payload)
	}
	response, payload = h.public(http.MethodGet, "/v1/models", validKey, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("models response = %d %s", response.StatusCode, payload)
	}
	var models struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &models); err != nil {
		t.Fatal(err)
	}
	if len(models.Data) != 1 || models.Data[0].ID != "qwen" {
		t.Fatalf("models = %+v", models.Data)
	}
	response, payload = h.public(http.MethodPost, "/v1/completions", noAccessKey, postBody("qwen", false))
	if response.StatusCode != http.StatusForbidden || !strings.Contains(string(payload), `"code":"model_not_allowed"`) {
		t.Fatalf("model access response = %d %s", response.StatusCode, payload)
	}

	keyList := h.registry.Snapshot().KeyCandidates[validKey[:12]]
	if len(keyList) != 1 || keyList[0].ClientID != clientID {
		t.Fatalf("key candidates = %+v", keyList)
	}
	h.adminObject(http.MethodDelete, "/admin/api/keys/"+strconv.FormatInt(keyList[0].ID, 10), nil, http.StatusNoContent)
	response, payload = h.public(http.MethodGet, "/v1/models", validKey, "")
	if response.StatusCode != http.StatusUnauthorized || !strings.Contains(string(payload), `"code":"invalid_api_key"`) {
		t.Fatalf("revoked key response = %d %s", response.StatusCode, payload)
	}
}

func TestSupportedOpenAIRoutesAcceptance(t *testing.T) {
	h := newHarness(t)
	poolID := h.createPool("qwen")
	fake, backendID := h.addFake(poolID, "gpu-a", fakevllm.State{})
	h.waitBackend(backendID, eligible)
	_, key := h.createClient("route-client", domain.PriorityHigh, -10, 4, poolID)

	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			response, payload := h.public(http.MethodPost, path, key, postBody("qwen", false))
			if response.StatusCode != http.StatusOK {
				t.Fatalf("%s = %d %s", path, response.StatusCode, payload)
			}
		})
	}
	if requests := fake.Snapshot().Requests; len(requests) != 3 {
		t.Fatalf("upstream requests = %d, want 3", len(requests))
	}
}
