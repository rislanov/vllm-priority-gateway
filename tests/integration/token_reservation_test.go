package integration_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
)

func TestSQLiteTokenReservationInference(t *testing.T) {
	h := newHarness(t)
	pool := h.adminObject(http.MethodPost, "/admin/api/pools", map[string]any{
		"publicModelName": "bounded", "upstreamModelName": "fake-model", "enabled": true,
		"requestSizeAdmission": map[string]any{
			"enabled": true, "estimationProfile": "integration-fixture", "maxOutstandingTokens": 1000,
			"maxInputTokens": 1000, "maxOutputTokens": 500, "defaultOutputTokens": 500,
		},
	}, http.StatusCreated)
	poolID := numberID(t, pool["id"])
	var tokenCalls, generationCalls atomic.Int32
	fake := fakevllm.New().Handler()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tokenize":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload["model"] != "fake-model" {
				t.Errorf("tokenizer did not receive the upstream model: %#v %v", payload, err)
			}
			tokenCalls.Add(1)
			io.WriteString(w, `{"count":100,"max_model_len":4096}`)
		case "/v1/completions":
			generationCalls.Add(1)
			fake.ServeHTTP(w, r)
		default:
			fake.ServeHTTP(w, r)
		}
	}))
	h.upstreams = append(h.upstreams, upstream)
	backend := h.adminObject(http.MethodPost, "/admin/api/backends", map[string]any{
		"modelPoolId": poolID, "name": "gpu", "baseUrl": upstream.URL, "enabled": true,
		"capacityHint": 1, "runningSoftLimit": 16,
	}, http.StatusCreated)
	h.waitBackend(numberID(t, backend["id"]), eligible)
	_, key := h.createClient("normal", domain.PriorityNormal, 0, 4, poolID)
	response, body := h.public(http.MethodPost, "/v1/completions", key, `{"model":"bounded","prompt":"hi","n":2}`)
	if response.StatusCode != 429 || !strings.Contains(string(body), "token_budget_exhausted") || generationCalls.Load() != 0 {
		t.Fatalf("oversized reservation = %d %s, generation calls=%d", response.StatusCode, body, generationCalls.Load())
	}
	response, body = h.public(http.MethodPost, "/v1/completions", key, `{"model":"bounded","prompt":"hi"}`)
	if response.StatusCode != 200 || tokenCalls.Load() != 2 || generationCalls.Load() != 1 {
		t.Fatalf("valid reservation = %d %s, tokenizer=%d generation=%d", response.StatusCode, body, tokenCalls.Load(), generationCalls.Load())
	}
}
