package gateway_test

import (
	"context"
	"errors"
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
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
)

type tokenizationHook struct {
	coordination.TokenizationCoordinator
	after       func()
	completions int
}

func (c *tokenizationHook) Acquire(ctx context.Context, r coordination.TokenizationAcquireRequest) (coordination.TokenizationDecision, error) {
	d, e := c.TokenizationCoordinator.Acquire(ctx, r)
	if c.after != nil {
		c.after()
	}
	return d, e
}
func (c *tokenizationHook) Complete(ctx context.Context, l coordination.TokenizationLease) error {
	c.completions++
	return c.TokenizationCoordinator.Complete(ctx, l)
}

type utilityForwardFunc func(context.Context, http.ResponseWriter, proxy.TokenizationRequest) proxy.Result

func (f utilityForwardFunc) ForwardTokenization(ctx context.Context, w http.ResponseWriter, r proxy.TokenizationRequest) proxy.Result {
	return f(ctx, w, r)
}

type utilityEvents struct{ events []gateway.TokenizationEvent }

func (o *utilityEvents) TokenizationComplete(e gateway.TokenizationEvent) {
	o.events = append(o.events, e)
}
func TestTokenizationRevalidatesAndReleasesAfterAdmission(t *testing.T) {
	for _, change := range []string{"revoked", "backend", "pool", "panic", "timeout"} {
		t.Run(change, func(t *testing.T) {
			now := time.Now().UTC()
			p := coordination.DefaultTokenizationPolicy()
			p.Timeout = 20 * time.Millisecond
			secret := []byte(strings.Repeat("s", 32))
			raw := "llmgw_abcdefghijklmnopqrstuvwxyz012345"
			client := domain.Client{ID: 1, Name: "client", Enabled: true}
			key := domain.APIKey{ID: 2, ClientID: 1, Prefix: raw[:12], SecretHash: apikey.Digest(secret, raw)}
			pool := domain.ModelPool{ID: 3, Revision: 1, PublicModelName: "public", UpstreamModelName: "upstream", Enabled: true}
			backend := domain.Backend{ID: 4, Revision: 1, Name: "gpu", ModelPoolID: 3, Enabled: true}
			provider := &mutableSnapshotProvider{}
			provider.Set(testSnapshot(client, key, pool, []domain.Backend{backend}))
			coordinator := &tokenizationHook{TokenizationCoordinator: coordlocal.NewTokenizationCoordinator(p, time.Now)}
			events := &utilityEvents{}
			calls := 0
			coordinator.after = func() {
				switch change {
				case "revoked":
					key.RevokedAt = &now
				case "backend":
					backend.Revision++
				case "pool":
					pool.UpstreamModelName = "changed"
				}
				provider.Set(testSnapshot(client, key, pool, []domain.Backend{backend}))
			}
			service := gateway.New(gateway.Dependencies{Registry: provider, HMACSecret: secret, Runtime: utilityRuntime{domain.BackendRuntime{Healthy: true, LastHealthAt: now}}, Tokenization: coordinator, TokenizationPolicy: p, TokenizationHealthMaxAge: time.Minute, TokenizationObserver: events, TokenizationForwarder: utilityForwardFunc(func(ctx context.Context, w http.ResponseWriter, r proxy.TokenizationRequest) proxy.Result {
				calls++
				if change == "panic" {
					panic("forward-panic")
				}
				<-ctx.Done()
				return proxy.Result{Err: ctx.Err(), Cancelled: true}
			})})
			var apiErr *gateway.APIError
			func() {
				defer func() {
					if recovered := recover(); recovered != nil && (change != "panic" || recovered != "forward-panic") {
						t.Fatalf("wrong panic: %v", recovered)
					}
				}()
				_, apiErr = service.Tokenize(context.Background(), httptest.NewRecorder(), gateway.ForwardRequest{APIKey: raw, Body: []byte(`{"model":"public","prompt":"synthetic"}`)})
			}()
			if coordinator.completions != 1 || len(events.events) != 1 {
				t.Fatalf("release/events: %d %d", coordinator.completions, len(events.events))
			}
			if change == "panic" {
				if calls != 1 || events.events[0].Reason != "panic" {
					t.Fatal(events.events)
				}
			} else if change == "timeout" {
				if apiErr == nil || apiErr.HTTPStatus != 504 {
					t.Fatal(apiErr)
				}
			} else if calls != 0 || apiErr == nil {
				t.Fatalf("stale request dispatched: %d %+v", calls, apiErr)
			}
		})
	}
}
func TestTokenizationHTTPEarlyFailureEmitsOneUtilityEvent(t *testing.T) {
	for _, body := range []string{`{}`, strings.Repeat("x", 2048)} {
		events := &utilityEvents{}
		s := gateway.New(gateway.Dependencies{TokenizationObserver: events})
		h := httpapi.NewPublicHandler(s, 1024, func() (string, error) { return "", errors.New("ID unavailable") })
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", "/tokenize", strings.NewReader(body)))
		if w.Code != 500 || len(events.events) != 1 {
			t.Fatalf("early failure: %d events=%d", w.Code, len(events.events))
		}
	}
}

func TestTokenizationHTTPEmitsExactlyOneEvent(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		auth       bool
		status     int
	}{{"missing auth", `{}`, false, 401}, {"invalid JSON", `[]`, true, 400}, {"oversize", strings.Repeat("x", 2048), true, 400}, {"success", `{"model":"public","prompt":"synthetic"}`, true, 200}} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC()
			secret := []byte(strings.Repeat("s", 32))
			raw := "llmgw_abcdefghijklmnopqrstuvwxyz012345"
			client := domain.Client{ID: 1, Enabled: true}
			key := domain.APIKey{ID: 2, ClientID: 1, Prefix: raw[:12], SecretHash: apikey.Digest(secret, raw)}
			pool := domain.ModelPool{ID: 3, PublicModelName: "public", UpstreamModelName: "upstream", Enabled: true}
			backend := domain.Backend{ID: 4, ModelPoolID: 3, Enabled: true}
			provider := &mutableSnapshotProvider{}
			provider.Set(testSnapshot(client, key, pool, []domain.Backend{backend}))
			events := &utilityEvents{}
			p := coordination.DefaultTokenizationPolicy()
			s := gateway.New(gateway.Dependencies{Registry: provider, HMACSecret: secret, Runtime: utilityRuntime{domain.BackendRuntime{Healthy: true, LastHealthAt: now}}, Tokenization: coordlocal.NewTokenizationCoordinator(p, time.Now), TokenizationPolicy: p, TokenizationHealthMaxAge: time.Minute, TokenizationObserver: events, TokenizationForwarder: utilityForwardFunc(func(ctx context.Context, w http.ResponseWriter, r proxy.TokenizationRequest) proxy.Result {
				w.WriteHeader(200)
				return proxy.Result{Status: 200, ResponseStarted: true}
			})})
			h := httpapi.NewPublicHandler(s, 1024, nil)
			r := httptest.NewRequest("POST", "/tokenize", strings.NewReader(tc.body))
			if tc.auth {
				r.Header.Set("Authorization", "Bearer "+raw)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status || len(events.events) != 1 || events.events[0].Status != tc.status {
				t.Fatalf("status/events mismatch: %d %+v", w.Code, events.events)
			}
		})
	}
}
