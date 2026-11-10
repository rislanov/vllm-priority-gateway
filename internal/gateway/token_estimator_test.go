package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
)

func TestEstimatorBoundsAndPrivacy(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body       string
		wantStatus int
		wantKind   string
	}{
		{"valid", 200, `{"count":2,"max_model_len":4096}`, 0, "upstream"},
		{"unsupported can fall back", 404, "private-tokenizer-detail", 0, "utf8_bytes"},
		{"rate limit cannot fall back", 429, "private-tokenizer-detail", 429, ""},
		{"temporary failure cannot fall back", 503, "private-tokenizer-detail", 503, ""},
		{"upstream authentication cannot fall back", 401, "private-tokenizer-detail", 503, ""},
		{"malformed count", 200, `{"count":null,"max_model_len":4096}`, 503, ""},
		{"context exceeded", 200, `{"count":200,"max_model_len":256}`, 400, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				data, _ := io.ReadAll(r.Body)
				if string(data) != `{"add_special_tokens":true,"model":"upstream","prompt":"hello"}` {
					t.Errorf("unexpected tokenizer request %s", data)
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			var released int
			estimator := tokenEstimator{
				timeout: time.Second, maxResponseBytes: 1024, transport: proxy.New(server.Client()),
				acquire: func(context.Context, EstimationRequest) (proxy.TokenizationRequest, func(), *APIError) {
					return proxy.TokenizationRequest{Backend: domain.Backend{BaseURL: server.URL}}, func() { released++ }, nil
				},
			}
			pool := domain.ModelPool{UpstreamModelName: "upstream", RequestSizeAdmission: domain.RequestSizePolicy{
				Enabled: true, EstimationProfile: "qualified-v1", MaxOutstandingTokens: 10000,
				FallbackMode: domain.TokenFallbackUTF8Bytes, FallbackOverheadTokens: 64,
				Limits: domain.RequestSizeLimits{MaxInputTokens: 1024, MaxOutputTokens: 512, DefaultOutputTokens: 128},
			}}
			client := domain.Client{PriorityClass: domain.PriorityNormal}
			policy, err := domain.ResolveRequestSizePolicy(client, pool)
			if err != nil {
				t.Fatal(err)
			}
			path := "/v1/completions"
			prepared, err := admission.PrepareRequestSize(path, []byte(`{"prompt":"hello"}`), policy)
			if err != nil {
				t.Fatal(err)
			}
			estimate, apiErr := estimator.Estimate(context.Background(), EstimationRequest{Path: path, Prepared: prepared, Pool: pool, Client: client})
			if calls.Load() != 1 || released != 1 {
				t.Fatalf("calls=%d releases=%d, want 1 each", calls.Load(), released)
			}
			if tc.wantStatus != 0 {
				if apiErr == nil || apiErr.HTTPStatus != tc.wantStatus || strings.Contains(apiErr.Message, "private-tokenizer-detail") {
					t.Fatalf("error = %+v, want sanitized status %d", apiErr, tc.wantStatus)
				}
				return
			}
			if apiErr != nil || estimate.Kind != tc.wantKind || len(estimate.InputTokens) != 1 || estimate.InputTokens[0] <= 0 {
				t.Fatalf("estimate=%+v error=%+v", estimate, apiErr)
			}
		})
	}
}

func TestEstimatorSharesDeadlineAcrossBatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"count":2,"max_model_len":4096}`)
	}))
	defer server.Close()
	var deadlines []time.Time
	estimator := tokenEstimator{
		timeout: 2 * time.Second, maxResponseBytes: 1024, transport: proxy.New(server.Client()),
		acquire: func(ctx context.Context, _ EstimationRequest) (proxy.TokenizationRequest, func(), *APIError) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("utility lease acquired without deadline")
			}
			deadlines = append(deadlines, deadline)
			return proxy.TokenizationRequest{Backend: domain.Backend{BaseURL: server.URL}}, func() {}, nil
		},
	}
	client := domain.Client{PriorityClass: domain.PriorityNormal}
	pool := domain.ModelPool{UpstreamModelName: "upstream", RequestSizeAdmission: domain.RequestSizePolicy{
		Enabled: true, EstimationProfile: "qualified-v1", MaxOutstandingTokens: 10000,
		Limits: domain.RequestSizeLimits{MaxInputTokens: 1024, MaxOutputTokens: 512, DefaultOutputTokens: 128},
	}}
	policy, err := domain.ResolveRequestSizePolicy(client, pool)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/completions"
	prepared, err := admission.PrepareRequestSize(path, []byte(`{"prompt":["one","two"]}`), policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, apiErr := estimator.Estimate(context.Background(), EstimationRequest{Path: path, Prepared: prepared, Pool: pool, Client: client}); apiErr != nil {
		t.Fatal(apiErr)
	}
	if len(deadlines) != 2 || !deadlines[0].Equal(deadlines[1]) {
		t.Fatalf("preflight operations used different deadlines: %v", deadlines)
	}
}

func TestEstimatorCancelsUpstreamAndReleasesUtilityLease(t *testing.T) {
	started := make(chan struct{})
	upstreamCancelled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(upstreamCancelled)
	}))
	defer server.Close()
	parent, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		select {
		case <-started:
			cancel()
		case <-parent.Done():
		}
	}()

	var released bool
	estimator := tokenEstimator{
		timeout: 2 * time.Second, maxResponseBytes: 1024, transport: proxy.New(server.Client()),
		acquire: func(context.Context, EstimationRequest) (proxy.TokenizationRequest, func(), *APIError) {
			return proxy.TokenizationRequest{Backend: domain.Backend{BaseURL: server.URL}}, func() { released = true }, nil
		},
	}
	client := domain.Client{PriorityClass: domain.PriorityNormal}
	pool := domain.ModelPool{UpstreamModelName: "upstream", RequestSizeAdmission: domain.RequestSizePolicy{
		Enabled: true, EstimationProfile: "qualified-v1", MaxOutstandingTokens: 10000,
		Limits: domain.RequestSizeLimits{MaxInputTokens: 1024, MaxOutputTokens: 512, DefaultOutputTokens: 128},
	}}
	policy, err := domain.ResolveRequestSizePolicy(client, pool)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/completions"
	prepared, err := admission.PrepareRequestSize(path, []byte(`{"prompt":"hello"}`), policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, apiErr := estimator.Estimate(parent, EstimationRequest{Path: path, Prepared: prepared, Pool: pool, Client: client}); apiErr == nil || !released {
		t.Fatalf("cancelled preflight error=%+v released=%v", apiErr, released)
	}
	select {
	case <-upstreamCancelled:
	case <-time.After(time.Second):
		t.Fatal("tokenizer request did not observe cancellation")
	}
}

func TestEmergencyEstimatorTransitionMidBatch(t *testing.T) {
	for _, tc := range []struct {
		name          string
		code          string
		allow         bool
		wantEmergency bool
	}{
		{"recoverable outage", "tokenization_coordination_unavailable", true, true},
		{"permanent fault", "tokenization_coordination_unavailable", false, false},
		{"utility limit", "tokenization_client_concurrency", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var upstreamCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				upstreamCalls.Add(1)
				fmt.Fprint(w, `{"count":2,"max_model_len":4096}`)
			}))
			defer server.Close()
			var modes []EstimationMode
			var deadline time.Time
			estimator := tokenEstimator{
				timeout: 2 * time.Second, maxResponseBytes: 1024, transport: proxy.New(server.Client()),
				mayUseEmergency: func(EstimationRequest) bool { return tc.allow },
				acquire: func(ctx context.Context, req EstimationRequest) (proxy.TokenizationRequest, func(), *APIError) {
					modes = append(modes, req.Mode)
					d, _ := ctx.Deadline()
					if len(modes) == 1 {
						deadline = d
					} else if d.After(deadline) {
						t.Fatal("transition extended the preflight deadline")
					}
					if len(modes) == 2 {
						return proxy.TokenizationRequest{}, nil, &APIError{Code: tc.code, HTTPStatus: 503}
					}
					return proxy.TokenizationRequest{Backend: domain.Backend{BaseURL: server.URL}}, func() {}, nil
				},
			}
			client := domain.Client{PriorityClass: domain.PriorityHigh}
			pool := domain.ModelPool{UpstreamModelName: "upstream", RequestSizeAdmission: domain.RequestSizePolicy{
				Enabled: true, EstimationProfile: "qualified-v1", MaxOutstandingTokens: 10000,
				Limits: domain.RequestSizeLimits{MaxInputTokens: 1024, MaxOutputTokens: 512, DefaultOutputTokens: 128},
			}}
			policy, err := domain.ResolveRequestSizePolicy(client, pool)
			if err != nil {
				t.Fatal(err)
			}
			path := "/v1/completions"
			prepared, err := admission.PrepareRequestSize(path, []byte(`{"prompt":["one","two","three"]}`), policy)
			if err != nil {
				t.Fatal(err)
			}
			got, apiErr := estimator.Estimate(context.Background(), EstimationRequest{Path: path, Prepared: prepared, Pool: pool, Client: client, Mode: EstimationNormal})
			if !tc.wantEmergency {
				if apiErr == nil || len(modes) != 2 || upstreamCalls.Load() != 1 {
					t.Fatalf("unexpected retry after nonrecoverable failure: modes=%v err=%+v", modes, apiErr)
				}
				return
			}
			if apiErr != nil || got.Mode != EstimationEmergency || len(modes) != 4 ||
				modes[2] != EstimationEmergency || modes[3] != EstimationEmergency ||
				upstreamCalls.Load() != 3 || len(got.InputTokens) != 3 {
				t.Fatalf("mid-batch transition: modes=%v estimates=%+v error=%+v", modes, got, apiErr)
			}
		})
	}
}
