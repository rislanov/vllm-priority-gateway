package gateway_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
)

type estimatorFunc func(context.Context, gateway.EstimationRequest) (gateway.TokenEstimate, *gateway.APIError)

func (f estimatorFunc) Estimate(ctx context.Context, req gateway.EstimationRequest) (gateway.TokenEstimate, *gateway.APIError) {
	return f(ctx, req)
}

type requestCaptureForwarder func(context.Context, http.ResponseWriter, proxy.Request) proxy.Result

func (f requestCaptureForwarder) Forward(ctx context.Context, w http.ResponseWriter, req proxy.Request) proxy.Result {
	return f(ctx, w, req)
}

func TestTokenAdmissionHTTPMatrix(t *testing.T) {
	policy := domain.RequestSizePolicy{
		Enabled: true, EstimationProfile: "qualified-v1", MaxOutstandingTokens: 1000,
		FallbackMode: domain.TokenFallbackUTF8Bytes, FallbackOverheadTokens: 32,
		Limits: domain.RequestSizeLimits{MaxInputTokens: 1000, MaxOutputTokens: 500, DefaultOutputTokens: 500},
	}
	for _, tc := range []struct {
		name, path, body, outputKey, reason string
		status                              int
		estimateCalls                       int
		disabled                            bool
	}{
		{"chat", "/v1/chat/completions", `{"model":"public-model","messages":[{"role":"user","content":"hi"}],"stream":true}`, "max_completion_tokens", "", 200, 1, false},
		{"completions", "/v1/completions", `{"model":"public-model","prompt":"hi"}`, "max_tokens", "", 200, 1, false},
		{"responses", "/v1/responses", `{"model":"public-model","input":"hi"}`, "max_output_tokens", "", 200, 1, false},
		{"disabled", "/v1/completions", `{"model":"public-model","prompt":"hi"}`, "", "", 200, 0, true},
		{"output limit", "/v1/completions", `{"model":"public-model","prompt":"hi","max_tokens":501}`, "", "output_token_limit_exceeded", 400, 0, false},
		{"budget", "/v1/completions", `{"model":"public-model","prompt":"hi","n":2}`, "", "token_budget_exhausted", 429, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coordinator := local.NewAdmissionCoordinator(func() time.Time { return poolTestNow })
			estimates, forwards := 0, 0
			poolPolicy := policy
			if tc.disabled {
				poolPolicy.Enabled = false
			}
			service, req, _ := newPoolService(t, poolServiceOptions{
				requestSize: poolPolicy, coordinator: coordinator,
				inputEstimator: estimatorFunc(func(_ context.Context, req gateway.EstimationRequest) (gateway.TokenEstimate, *gateway.APIError) {
					estimates++
					return gateway.TokenEstimate{InputTokens: []int64{100}, ContextLimits: []int64{4096}, Kind: "upstream", Profile: req.Pool.RequestSizeAdmission.EstimationProfile}, nil
				}),
				forwarder: requestCaptureForwarder(func(_ context.Context, w http.ResponseWriter, req proxy.Request) proxy.Result {
					forwards++
					var payload map[string]any
					if err := json.Unmarshal(req.Body, &payload); err != nil {
						t.Fatal(err)
					}
					if tc.outputKey != "" && payload[tc.outputKey] != float64(500) {
						t.Errorf("forwarded output cap = %#v, want 500", payload[tc.outputKey])
					}
					if tc.disabled && payload["max_tokens"] != nil {
						t.Errorf("disabled policy rewrote the output cap: %#v", payload)
					}
					wantOutstanding := int64(600)
					if tc.disabled {
						wantOutstanding = 0
					}
					if got := coordinator.PoolOutstandingTokens(10); got != wantOutstanding {
						t.Errorf("during upstream request, outstanding=%d want=%d", got, wantOutstanding)
					}
					fmt.Fprint(w, `{"ok":true}`)
					return proxy.Result{Status: 200, ResponseStarted: true}
				}),
			})
			handler := httpapi.NewPublicHandler(service, 1024*1024, nil)
			httpReq := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			httpReq.Header.Set("Authorization", "Bearer "+req.APIKey)
			httpReq.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httpReq)
			if recorder.Code != tc.status || tc.reason != "" && !strings.Contains(recorder.Body.String(), tc.reason) {
				t.Fatalf("response %d %s; want %d %q", recorder.Code, recorder.Body.String(), tc.status, tc.reason)
			}
			if tc.status == 429 && recorder.Header().Get("Retry-After") == "" {
				t.Fatal("budget denial omitted Retry-After")
			}
			wantForwards := 0
			if tc.status == 200 {
				wantForwards = 1
			}
			if estimates != tc.estimateCalls || forwards != wantForwards || coordinator.PoolOutstandingTokens(10) != 0 {
				t.Fatalf("estimates=%d forwards=%d outstanding=%d", estimates, forwards, coordinator.PoolOutstandingTokens(10))
			}
		})
	}
}

func TestTokenAdmissionPolicyChangedDuringEstimate(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		changes               int
		mutate                func(*domain.Client, *domain.ModelPool, *domain.APIKey)
		wantStatus, estimates int
	}{
		{"key revoked", 1, func(_ *domain.Client, _ *domain.ModelPool, k *domain.APIKey) { at := poolTestNow; k.RevokedAt = &at }, 401, 1},
		{"pool disabled", 1, func(_ *domain.Client, p *domain.ModelPool, _ *domain.APIKey) { p.Enabled = false }, 403, 1},
		{"profile changed", 1, func(_ *domain.Client, p *domain.ModelPool, _ *domain.APIKey) {
			p.RequestSizeAdmission.EstimationProfile += "-new"
		}, 503, 1},
		{"output limit reduced", 1, func(_ *domain.Client, p *domain.ModelPool, _ *domain.APIKey) {
			p.RequestSizeAdmission.Limits.MaxOutputTokens -= 10
			p.RequestSizeAdmission.Limits.DefaultOutputTokens -= 10
		}, 200, 2},
		{"policy changes twice", 2, func(_ *domain.Client, p *domain.ModelPool, _ *domain.APIKey) {
			p.RequestSizeAdmission.Limits.MaxOutputTokens -= 10
			p.RequestSizeAdmission.Limits.DefaultOutputTokens -= 10
		}, 503, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			secret := []byte(strings.Repeat("s", 32))
			raw := "llmgw_abcdefghijklmnopqrstuvwxyz012345"
			client := domain.Client{ID: 1, Revision: 1, Enabled: true, PriorityClass: domain.PriorityHigh, MaxConcurrency: 4}
			key := domain.APIKey{ID: 2, ClientID: client.ID, Prefix: raw[:12], SecretHash: apikey.Digest(secret, raw)}
			pool := domain.ModelPool{ID: 10, Revision: 1, Enabled: true, PublicModelName: "public-model", UpstreamModelName: "upstream",
				RequestSizeAdmission: domain.RequestSizePolicy{
					Enabled: true, EstimationProfile: "qualified-v1", MaxOutstandingTokens: 1000,
					Limits: domain.RequestSizeLimits{MaxInputTokens: 1000, MaxOutputTokens: 500, DefaultOutputTokens: 500},
				},
			}
			backend := retryBackend(20, "gpu", "http://gpu.invalid")
			provider := &mutableSnapshotProvider{}
			provider.Set(testSnapshot(client, key, pool, []domain.Backend{backend}))
			coordinator := local.NewAdmissionCoordinator(func() time.Time { return poolTestNow })
			estimates, forwarded := 0, 0
			service := gateway.New(gateway.Dependencies{
				Registry: provider, HMACSecret: secret, Admission: coordinator, Now: func() time.Time { return poolTestNow },
				Runtime: newPoolRuntime(domain.PoolRuntime{PoolID: pool.ID, State: domain.PoolNormal, AvailableBackends: 1}, []domain.BackendRuntime{{
					BackendID: backend.ID, Healthy: true, MetricsFresh: true, CircuitAvailable: true,
				}}),
				Router: routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0)),
				InputEstimator: estimatorFunc(func(_ context.Context, req gateway.EstimationRequest) (gateway.TokenEstimate, *gateway.APIError) {
					estimates++
					if estimates <= tc.changes {
						tc.mutate(&client, &pool, &key)
						pool.Revision++
						updated := testSnapshot(client, key, pool, []domain.Backend{backend})
						updated.Revision = int64(estimates + 1)
						provider.Set(updated)
					}
					return gateway.TokenEstimate{InputTokens: []int64{100}, Kind: "upstream", Profile: req.Pool.RequestSizeAdmission.EstimationProfile}, nil
				}),
				Forwarder: requestCaptureForwarder(func(_ context.Context, w http.ResponseWriter, request proxy.Request) proxy.Result {
					forwarded++
					var payload map[string]any
					if err := json.Unmarshal(request.Body, &payload); err != nil || payload["max_tokens"] != float64(490) {
						t.Fatalf("did not rebuild payload from original request: %#v %v", payload, err)
					}
					return proxy.Result{Status: 200}
				}),
			})
			_, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), gateway.ForwardRequest{
				Method: http.MethodPost, Path: "/v1/completions", APIKey: raw,
				Body: []byte(`{"model":"public-model","prompt":"hi"}`), Headers: make(http.Header),
			})
			if tc.wantStatus == 200 {
				if apiErr != nil || forwarded != 1 {
					t.Fatalf("current policy was not admitted: %+v forwarded=%d", apiErr, forwarded)
				}
			} else if apiErr == nil || apiErr.HTTPStatus != tc.wantStatus || forwarded != 0 {
				t.Fatalf("API error=%+v forwarded=%d, want %d and no dispatch", apiErr, forwarded, tc.wantStatus)
			}
			if estimates != tc.estimates {
				t.Fatalf("estimates=%d want %d", estimates, tc.estimates)
			}
		})
	}
}

type uncertainAdmission struct {
	coordination.AdmissionCoordinator
	calls int
}

func (c *uncertainAdmission) Acquire(ctx context.Context, req coordination.AdmissionRequest) (coordination.AdmissionDecision, error) {
	c.calls++
	d, err := c.AdmissionCoordinator.Acquire(ctx, req)
	if err != nil || !d.Admitted() {
		return d, err
	}
	return coordination.AdmissionDecision{Reason: coordination.ReasonCoordinationUnavailable}, fmt.Errorf("lost commit response")
}

func TestTokenAdmissionAmbiguousFallback(t *testing.T) {
	now := poolTestNow
	shared := local.NewAdmissionCoordinator(func() time.Time { return now })
	coordinator := &uncertainAdmission{AdmissionCoordinator: shared}
	emergency := coordination.NewEmergencyAdmission(1, 2)
	forwards := 0
	service, request, _ := newPoolService(t, poolServiceOptions{
		requestSize: domain.RequestSizePolicy{Enabled: true, EstimationProfile: "qualified-v1", MaxOutstandingTokens: 1000,
			Limits: domain.RequestSizeLimits{MaxInputTokens: 1000, MaxOutputTokens: 500, DefaultOutputTokens: 500}},
		coordinator: coordinator, emergency: emergency,
		inputEstimator: estimatorFunc(func(_ context.Context, req gateway.EstimationRequest) (gateway.TokenEstimate, *gateway.APIError) {
			return gateway.TokenEstimate{InputTokens: []int64{100}, Kind: "upstream", Profile: req.Pool.RequestSizeAdmission.EstimationProfile}, nil
		}),
		forwarder: requestCaptureForwarder(func(_ context.Context, _ http.ResponseWriter, _ proxy.Request) proxy.Result {
			forwards++
			if emergency.PoolOutstandingTokens(10) != 600 || shared.PoolOutstandingTokens(10) != 600 {
				t.Fatal("emergency and unresolved shared lease did not retain independent accounting")
			}
			return proxy.Result{Status: 200}
		}),
	})
	request.Body = []byte(`{"model":"public-model","prompt":"hi"}`)
	if _, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request); apiErr != nil {
		t.Fatal(apiErr)
	}
	if coordinator.calls != 1 || forwards != 1 || emergency.PoolOutstandingTokens(10) != 0 || shared.PoolOutstandingTokens(10) != 600 {
		t.Fatal("ambiguous admission repeated allocation or completed an unknown shared lease")
	}
	now = now.Add(time.Minute)
	if shared.PoolOutstandingTokens(10) != 0 {
		t.Fatal("unknown shared lease did not expire")
	}
}

func TestTokenAdmissionRetryAndCancellation(t *testing.T) {
	coordinator := local.NewAdmissionCoordinator(func() time.Time { return poolTestNow })
	const poolID = 10
	probe := &retryProbeForwarder{
		beforeAlternate: func() {
			if got := coordinator.PoolOutstandingTokens(poolID); got != 600 {
				t.Fatalf("outstanding during retry selection = %d, want one reservation", got)
			}
		},
	}
	options := poolServiceOptions{
		requestSize: domain.RequestSizePolicy{Enabled: true, EstimationProfile: "qualified-v1", MaxOutstandingTokens: 1000,
			Limits: domain.RequestSizeLimits{MaxInputTokens: 1000, MaxOutputTokens: 500, DefaultOutputTokens: 500}},
		coordinator: coordinator, forwarder: probe,
		backends: []domain.Backend{retryBackend(20, "first", "http://first.invalid"), retryBackend(21, "alternate", "http://alternate.invalid")},
		inputEstimator: estimatorFunc(func(_ context.Context, req gateway.EstimationRequest) (gateway.TokenEstimate, *gateway.APIError) {
			return gateway.TokenEstimate{InputTokens: []int64{100}, Kind: "upstream", Profile: req.Pool.RequestSizeAdmission.EstimationProfile}, nil
		}),
	}
	service, request, _ := newPoolService(t, options)
	request.Body = []byte(`{"model":"public-model","prompt":"hi"}`)
	if _, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request); apiErr != nil {
		t.Fatal(apiErr)
	}
	if probe.alternateErr != nil || probe.alternateBackend == 0 || probe.alternateBackend == probe.initialBackend || coordinator.PoolOutstandingTokens(poolID) != 0 {
		t.Fatalf("retry did not select an alternate and release its one reservation: %+v", probe)
	}

	for _, outcome := range []string{"cancel", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			options.forwarder = requestCaptureForwarder(func(_ context.Context, _ http.ResponseWriter, _ proxy.Request) proxy.Result {
				if got := coordinator.PoolOutstandingTokens(poolID); got != 600 {
					t.Fatalf("outstanding before %s = %d", outcome, got)
				}
				if outcome == "panic" {
					panic("simulated forwarding failure")
				}
				cancel()
				return proxy.Result{Cancelled: true, Err: context.Canceled}
			})
			service, request, _ := newPoolService(t, options)
			request.Body = []byte(`{"model":"public-model","prompt":"hi"}`)
			func() {
				defer func() {
					p := recover()
					if outcome == "panic" && p != "simulated forwarding failure" || outcome != "panic" && p != nil {
						t.Fatalf("unexpected panic: %v", p)
					}
				}()
				_, _, apiErr := service.Forward(ctx, httptest.NewRecorder(), request)
				if apiErr != nil {
					t.Fatal(apiErr)
				}
			}()
			if got := coordinator.PoolOutstandingTokens(poolID); got != 0 {
				t.Fatalf("outstanding after %s = %d", outcome, got)
			}
		})
	}
}
