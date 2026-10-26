package gateway_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordlocal "github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/monitor"
	"github.com/rislanov/vllm-priority-gateway/internal/pressure"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
)

type utilityRuntime struct{ value domain.BackendRuntime }

func (r utilityRuntime) Snapshot(int64, time.Time) domain.BackendRuntime { return r.value }
func (r utilityRuntime) BackendHealth(domain.Backend, time.Time) (domain.BackendRuntime, bool) {
	return r.value, true
}
func (r utilityRuntime) PoolSnapshot(int64, time.Time) domain.PoolRuntime {
	panic("inference pool snapshot")
}

func TestTokenizationRejectsHealthFromPreviousBackendConfiguration(t *testing.T) {
	oldUpstream := httptest.NewServer(fakevllm.New().Handler())
	defer oldUpstream.Close()
	var calls atomic.Int32
	healthGate := make(chan struct{})
	defer func() {
		select {
		case <-healthGate:
		default:
			close(healthGate)
		}
	}()
	newUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			select {
			case <-healthGate:
			case <-r.Context().Done():
				return
			}
			w.WriteHeader(200)
			return
		}
		if r.URL.Path == "/tokenize" {
			calls.Add(1)
			io.WriteString(w, `{"count":1,"tokens":[1]}`)
			return
		}
		fakevllm.New().Handler().ServeHTTP(w, r)
	}))
	defer newUpstream.Close()
	manager := monitor.NewManager(context.Background(), monitor.Options{HTTPClient: oldUpstream.Client(), HealthInterval: time.Hour, HealthTimeout: time.Second, MetricsInterval: time.Hour, MetricsTimeout: time.Second, StaleAfter: time.Minute, UnhealthyAfter: 1, RecoveryAfter: 1, EWMAWindow: time.Second, BusyThreshold: .7, SaturatedThreshold: 1, Limits: pressure.Limits{QueueSoft: 2, KVSoft: .8, KVHard: .95}})
	defer manager.Shutdown()
	old := domain.Backend{ID: 4, Revision: 1, ModelPoolID: 3, Name: "gpu", Enabled: true, BaseURL: oldUpstream.URL, CapacityHint: 1, RunningSoftLimit: 1}
	if e := manager.Reconcile([]domain.Backend{old}); e != nil {
		t.Fatal(e)
	}
	waitHealthy := func() {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for !manager.Snapshot(4, time.Now()).Healthy {
			if time.Now().After(deadline) {
				t.Fatal("health check did not finish")
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitHealthy()
	secret := []byte(strings.Repeat("s", 32))
	raw := "llmgw_abcdefghijklmnopqrstuvwxyz012345"
	client := domain.Client{ID: 1, Enabled: true}
	key := domain.APIKey{ID: 2, ClientID: 1, Prefix: raw[:12], SecretHash: apikey.Digest(secret, raw)}
	pool := domain.ModelPool{ID: 3, PublicModelName: "public", UpstreamModelName: "upstream", Enabled: true}
	provider := &mutableSnapshotProvider{}
	updated := old
	updated.BaseURL = newUpstream.URL
	updated.Revision++
	provider.Set(testSnapshot(client, key, pool, []domain.Backend{updated}))
	p := coordination.DefaultTokenizationPolicy()
	s := gateway.New(gateway.Dependencies{Registry: provider, HMACSecret: secret, Runtime: manager, Tokenization: coordlocal.NewTokenizationCoordinator(p, time.Now), TokenizationPolicy: p, TokenizationForwarder: proxy.New(newUpstream.Client()), TokenizationHealthMaxAge: time.Minute})
	request := gateway.ForwardRequest{APIKey: raw, Body: []byte(`{"model":"public","prompt":"synthetic"}`)}
	urlOnly := updated
	urlOnly.Revision = old.Revision
	revisionOnly := old
	revisionOnly.Revision++
	for _, candidate := range []domain.Backend{updated, urlOnly, revisionOnly} {
		provider.Set(testSnapshot(client, key, pool, []domain.Backend{candidate}))
		_, e := s.Tokenize(context.Background(), httptest.NewRecorder(), request)
		if e == nil || e.HTTPStatus != 503 || calls.Load() != 0 {
			t.Fatalf("used health of old configuration: error=%+v calls=%d", e, calls.Load())
		}
	}
	provider.Set(testSnapshot(client, key, pool, []domain.Backend{updated}))
	if e := manager.Reconcile([]domain.Backend{updated}); e != nil {
		t.Fatal(e)
	}
	_, e := s.Tokenize(context.Background(), httptest.NewRecorder(), request)
	if e == nil || e.HTTPStatus != 503 || calls.Load() != 0 {
		t.Fatalf("dispatched before new health check: %+v", e)
	}
	close(healthGate)
	waitHealthy()
	result, e := s.Tokenize(context.Background(), httptest.NewRecorder(), request)
	if e != nil || result.Status != 200 || calls.Load() != 1 {
		t.Fatalf("fresh backend not dispatched: %+v %+v calls=%d", result, e, calls.Load())
	}
	// A worker replacement during admission must also fail the second check,
	// even if the registry snapshot itself has not changed again.
	hook := &tokenizationHook{TokenizationCoordinator: coordlocal.NewTokenizationCoordinator(p, time.Now)}
	hook.after = func() {
		if err := manager.Reconcile([]domain.Backend{old}); err != nil {
			t.Fatal(err)
		}
	}
	s = gateway.New(gateway.Dependencies{Registry: provider, HMACSecret: secret, Runtime: manager, Tokenization: hook, TokenizationPolicy: p, TokenizationForwarder: proxy.New(newUpstream.Client()), TokenizationHealthMaxAge: time.Minute})
	_, e = s.Tokenize(context.Background(), httptest.NewRecorder(), request)
	if e == nil || e.HTTPStatus != 503 || calls.Load() != 1 || hook.completions != 1 {
		t.Fatalf("worker change during admission dispatched or leaked: %+v calls=%d release=%d", e, calls.Load(), hook.completions)
	}
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
