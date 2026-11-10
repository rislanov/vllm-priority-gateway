package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
)

type estimateTestSnapshot struct{ value *registry.Snapshot }

func (s estimateTestSnapshot) Snapshot() *registry.Snapshot { return s.value }

type emergencyEstimateRuntime struct {
	Runtime
	now func() time.Time
}

func (r emergencyEstimateRuntime) BackendHealth(domain.Backend, time.Time) (domain.BackendRuntime, bool) {
	return domain.BackendRuntime{Healthy: true, LastHealthAt: r.now()}, true
}

func TestEmergencyEstimatorWithoutDatabase(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tokenize" {
			t.Error("emergency preflight called the wrong endpoint")
		}
		calls++
		fmt.Fprint(w, `{"count":100,"max_model_len":4096}`)
	}))
	defer upstream.Close()
	ctx := context.Background()
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	secret := []byte(strings.Repeat("s", 32))
	raw := "llmgw_abcdefghijklmnopqrstuvwxyz012345"
	client := domain.Client{ID: 1, Revision: 1, Enabled: true, PriorityClass: domain.PriorityHigh, MaxConcurrency: 2}
	key := domain.APIKey{ID: 2, ClientID: 1, Prefix: raw[:12], SecretHash: apikey.Digest(secret, raw)}
	pool := domain.ModelPool{ID: 3, Revision: 1, PublicModelName: "public", UpstreamModelName: "upstream", Enabled: true,
		RequestSizeAdmission: domain.RequestSizePolicy{
			Enabled: true, EstimationProfile: "qualified-v1", MaxOutstandingTokens: 1000,
			Limits: domain.RequestSizeLimits{MaxInputTokens: 1024, MaxOutputTokens: 512, DefaultOutputTokens: 500},
		},
	}
	backend := domain.Backend{ID: 4, Revision: 1, ModelPoolID: pool.ID, Enabled: true, BaseURL: upstream.URL}
	snapshot := &registry.Snapshot{
		Revision: 1, Clients: map[int64]domain.Client{1: client}, KeyCandidates: map[string][]domain.APIKey{key.Prefix: {key}},
		PoolsByName: map[string]domain.ModelPool{"public": pool}, PoolsByID: map[int64]domain.ModelPool{3: pool},
		Access: map[int64]map[int64]bool{1: {3: true}}, BackendsByID: map[int64]domain.Backend{4: backend},
		BackendsByPool: map[int64][]domain.Backend{3: {backend}},
	}
	emergencyPolicy := coordination.TokenizationPolicy{ClientMaxInflight: 1, BackendMaxInflight: 1, ClientRPM: 30, Timeout: 2 * time.Second}
	allowed, ready := true, false
	s := New(Dependencies{
		Registry: estimateTestSnapshot{snapshot}, HMACSecret: secret, Now: clock, Runtime: emergencyEstimateRuntime{now: clock},
		CoordinationAllowed: func() bool { return allowed }, CoordinationReady: func() bool { return ready },
		Emergency: coordination.NewEmergencyAdmission(1, 2), EmergencyTokenization: local.NewEmergencyTokenizationCoordinator(emergencyPolicy, clock),
		TokenizationForwarder: proxy.New(upstream.Client()), TokenizationHealthMaxAge: time.Minute,
	})
	policy, err := domain.ResolveRequestSizePolicy(client, pool)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/completions"
	prepared, err := admission.PrepareRequestSize(path, []byte(`{"model":"public","prompt":"hello"}`), policy)
	if err != nil {
		t.Fatal(err)
	}
	req := EstimationRequest{Path: path, Prepared: prepared, Snapshot: snapshot, Client: client, Pool: pool, APIKeyID: key.ID, apiKey: raw}
	estimate, apiErr := s.inputEstimator.Estimate(ctx, req)
	if apiErr != nil || estimate.Mode != EstimationEmergency || len(estimate.InputTokens) != 1 || estimate.InputTokens[0] != 100 {
		t.Fatalf("emergency estimate=%+v err=%+v", estimate, apiErr)
	}
	if _, apiErr := s.inputEstimator.Estimate(ctx, req); apiErr == nil || apiErr.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("emergency tokenizer allowed a burst above one: %+v", apiErr)
	}
	_, apiErr = s.Tokenize(ctx, httptest.NewRecorder(), ForwardRequest{APIKey: raw, Body: []byte(`{"model":"public","prompt":"hello"}`)})
	if apiErr == nil || apiErr.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("public tokenize gained emergency access: %+v", apiErr)
	}
	reserve := coordination.TokenReservation{Enabled: true, InputEstimate: 100, OutputAllowance: 500, PromptCount: 1, Samples: 1,
		ReservedTokens: 600, PoolBudget: 1000, PoolPolicyRevision: pool.Revision, EstimatorKind: estimate.Kind, EstimationProfile: estimate.Profile}
	release, reason, _ := s.emergency.AcquireTokens(client.PriorityClass, client.ID, pool.ID, client.MaxConcurrency, reserve, 1000)
	if release == nil || reason != "" {
		t.Fatalf("emergency admission rejected valid estimate: %q", reason)
	}
	ready = true
	if got := s.emergency.PoolOutstandingTokens(pool.ID); got != 600 {
		t.Fatalf("recovery reset an active emergency lease: %d", got)
	}
	ready = false
	if _, reason, _ := s.emergency.AcquireTokens(client.PriorityClass, client.ID, pool.ID, client.MaxConcurrency, reserve, 1000); reason != coordination.ReasonTokenBudgetExhausted {
		t.Fatalf("repeated outage lost the prior emergency reservation: %q", reason)
	}
	release()
	now = now.Add(2 * time.Second)
	allowed = false
	if _, apiErr := s.inputEstimator.Estimate(ctx, req); apiErr == nil {
		t.Fatal("permanent coordination fault fell back to emergency")
	}
	if calls != 1 {
		t.Fatalf("tokenizer calls=%d, want 1", calls)
	}
}
