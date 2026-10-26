package gateway_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordlocal "github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
)

type utilityRuntime struct{ value domain.BackendRuntime }

func (r utilityRuntime) Snapshot(int64, time.Time) domain.BackendRuntime { return r.value }
func (r utilityRuntime) PoolSnapshot(int64, time.Time) domain.PoolRuntime {
	panic("inference pool snapshot")
}
func (r utilityRuntime) AcquirePool(int64, int) (func(), bool) { panic("inference pool acquire") }
func (r utilityRuntime) AcquireBackend(domain.Backend, time.Time) (func(domain.InferenceOutcome), bool) {
	panic("inference backend/circuit acquire")
}
func TestTokenizationValidationAndIsolation(t *testing.T) {
	now := time.Now().UTC()
	secret := []byte(strings.Repeat("s", 32))
	raw := "llmgw_abcdefghijklmnopqrstuvwxyz012345"
	client := domain.Client{ID: 1, Name: "client", Enabled: true}
	pool := domain.ModelPool{ID: 2, Revision: 1, PublicModelName: "public", UpstreamModelName: "upstream", Enabled: true}
	key := domain.APIKey{ID: 3, ClientID: 1, Prefix: raw[:12], SecretHash: apikey.Digest(secret, raw)}
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var b map[string]json.RawMessage
		json.NewDecoder(r.Body).Decode(&b)
		if string(b["model"]) != `"upstream"` || b["priority"] != nil || string(b["return_token_strs"]) != "true" {
			t.Errorf("payload changed: %v", b)
		}
		io.WriteString(w, `{"count":2,"tokens":[1,2]}`)
	}))
	defer upstream.Close()
	backend := domain.Backend{ID: 4, Revision: 1, ModelPoolID: 2, Name: "gpu", BaseURL: upstream.URL, Enabled: true}
	for _, tc := range []struct {
		name, body string
		status     int
		change     func(*domain.BackendRuntime, *domain.Backend, *domain.APIKey)
	}{
		{name: "prompt saturated GPU", body: `{"model":"public","prompt":"hi","return_token_strs":true,"priority":-999}`, status: 200},
		{name: "messages", body: `{"model":"public","messages":[{"role":"user","content":"hi"}],"return_token_strs":true}`, status: 200},
		{name: "missing model", body: `{"prompt":"hi"}`, status: 400}, {name: "null", body: `null`, status: 400}, {name: "array", body: `[]`, status: 400}, {name: "unknown", body: `{"model":"unknown"}`, status: 403},
		{name: "revoked", body: `{"model":"public"}`, status: 401, change: func(_ *domain.BackendRuntime, _ *domain.Backend, k *domain.APIKey) { k.RevokedAt = &now }},
		{name: "stale health", body: `{"model":"public"}`, status: 503, change: func(r *domain.BackendRuntime, _ *domain.Backend, _ *domain.APIKey) {
			r.LastHealthAt = now.Add(-time.Minute)
		}},
		{name: "future health", body: `{"model":"public"}`, status: 503, change: func(r *domain.BackendRuntime, _ *domain.Backend, _ *domain.APIKey) {
			r.LastHealthAt = now.Add(time.Second)
		}},
		{name: "draining", body: `{"model":"public"}`, status: 503, change: func(_ *domain.BackendRuntime, b *domain.Backend, _ *domain.APIKey) { b.Draining = true }},
		{name: "secret absent", body: `{"model":"public"}`, status: 503, change: func(_ *domain.BackendRuntime, b *domain.Backend, _ *domain.APIKey) { b.UpstreamAPIKeyEnv = "MISSING" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, k := backend, key
			runtime := domain.BackendRuntime{Healthy: true, LastHealthAt: now, Pressure: 99, MetricsFresh: false, CircuitAvailable: false}
			if tc.change != nil {
				tc.change(&runtime, &b, &k)
			}
			provider := &mutableSnapshotProvider{}
			provider.Set(testSnapshot(client, k, pool, []domain.Backend{b}))
			p := coordination.DefaultTokenizationPolicy()
			s := gateway.New(gateway.Dependencies{Registry: provider, HMACSecret: secret, Runtime: utilityRuntime{runtime}, Now: func() time.Time { return now }, TokenizationPolicy: p, Tokenization: coordlocal.NewTokenizationCoordinator(p, func() time.Time { return now }), TokenizationForwarder: proxy.New(upstream.Client()), TokenizationHealthMaxAge: 10 * time.Second})
			before := calls
			w := httptest.NewRecorder()
			result, e := s.Tokenize(context.Background(), w, gateway.ForwardRequest{APIKey: raw, Body: []byte(tc.body), RequestID: "utility"})
			status := result.Status
			if e != nil {
				status = e.HTTPStatus
			}
			if status != tc.status {
				t.Fatalf("status %d error=%+v", status, e)
			}
			if calls-before != map[bool]int{true: 1, false: 0}[tc.status == 200] {
				t.Fatal("unexpected upstream call")
			}
		})
	}
}
