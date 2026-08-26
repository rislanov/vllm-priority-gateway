package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
)

var testNow = time.Unix(1_700_000_000, 0).UTC()

func TestPublicAuthenticationCasesUseSameUnauthorizedEnvelope(t *testing.T) {
	raw, key := testKey(t)
	cases := []struct {
		name   string
		raw    string
		client domain.Client
		key    domain.APIKey
	}{
		{name: "unknown", raw: "llmgw_unknown_unknown_unknown_unknown_unknown", client: enabledClient(), key: key},
		{name: "revoked", raw: raw, client: enabledClient(), key: withRevoked(key)},
		{name: "expired", raw: raw, client: enabledClient(), key: withExpiry(key, testNow.Add(-time.Second))},
		{name: "disabled client", raw: raw, client: disabledClient(), key: key},
	}
	var firstBody string
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			handler, _ := newFixture(t, fixtureOptions{client: tt.client, key: tt.key})
			request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
			request.Header.Set("Authorization", "Bearer "+tt.raw)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d", response.Code)
			}
			if firstBody == "" {
				firstBody = response.Body.String()
			} else if response.Body.String() != firstBody {
				t.Fatalf("unauthorized body differs: %s != %s", response.Body.String(), firstBody)
			}
			assertErrorCode(t, response.Body.Bytes(), "invalid_api_key")
		})
	}
}

func TestModelsListsOnlyExplicitEnabledAccess(t *testing.T) {
	raw, key := testKey(t)
	handler, _ := newFixture(t, fixtureOptions{client: enabledClient(), key: key, extraPools: true})
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer "+raw)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data) != 1 || body.Data[0].ID != "public-model" {
		t.Fatalf("models = %+v", body.Data)
	}
}

func TestForwardRewritesModelAndClientControlledPriority(t *testing.T) {
	raw, key := testKey(t)
	forwarder := &capturingForwarder{}
	handler, _ := newFixture(t, fixtureOptions{client: enabledClient(), key: key, forwarder: forwarder})
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"public-model","priority":-999,"input":"hello"}`))
	request.Header.Set("Authorization", "Bearer "+raw)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Vllm-Priority", "-999")
	request.Header.Set("X-Request-Id", "parent-request")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	captured := forwarder.Request()
	if captured.Priority != -10 || captured.Headers.Get("X-Vllm-Priority") != "" {
		t.Fatalf("captured priority = %+v", captured)
	}
	var upstream map[string]json.RawMessage
	if err := json.Unmarshal(captured.Body, &upstream); err != nil {
		t.Fatal(err)
	}
	var model string
	if err := json.Unmarshal(upstream["model"], &model); err != nil {
		t.Fatal(err)
	}
	if model != "upstream-model" {
		t.Fatalf("upstream model = %q", model)
	}
	if _, exists := upstream["priority"]; exists {
		t.Fatal("client JSON priority reached upstream")
	}
	if response.Header().Get("X-Request-Id") != "fixed-gateway-id" {
		t.Fatalf("response request ID = %q", response.Header().Get("X-Request-Id"))
	}
}

func TestConcurrencyLeaseSpansWholeForwardLifecycle(t *testing.T) {
	raw, key := testKey(t)
	client := enabledClient()
	client.MaxConcurrency = 1
	forwarder := newBlockingForwarder()
	handler, _ := newFixture(t, fixtureOptions{client: client, key: key, forwarder: forwarder})
	server := httptest.NewServer(handler)
	defer server.Close()
	firstDone := make(chan *http.Response, 1)
	go func() {
		firstDone <- post(t, server.URL, raw)
	}()
	<-forwarder.started
	second := post(t, server.URL, raw)
	secondBody, _ := io.ReadAll(second.Body)
	second.Body.Close()
	if second.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second status = %d body=%s", second.StatusCode, secondBody)
	}
	close(forwarder.release)
	first := <-firstDone
	first.Body.Close()
	third := post(t, server.URL, raw)
	third.Body.Close()
	if third.StatusCode != http.StatusOK {
		t.Fatalf("third status = %d", third.StatusCode)
	}
}

func TestPublicControlledErrors(t *testing.T) {
	raw, key := testKey(t)
	cases := []struct {
		name       string
		method     string
		path       string
		body       string
		options    fixtureOptions
		wantStatus int
		wantCode   string
	}{
		{name: "malformed JSON", method: http.MethodPost, path: "/v1/completions", body: `{`, options: fixtureOptions{client: enabledClient(), key: key}, wantStatus: 400, wantCode: "invalid_request_error"},
		{name: "forbidden model", method: http.MethodPost, path: "/v1/completions", body: `{"model":"forbidden"}`, options: fixtureOptions{client: enabledClient(), key: key}, wantStatus: 403, wantCode: "model_not_allowed"},
		{name: "unsupported endpoint", method: http.MethodPost, path: "/v1/embeddings", body: `{}`, options: fixtureOptions{client: enabledClient(), key: key}, wantStatus: 404, wantCode: "unsupported_endpoint"},
		{name: "no backend", method: http.MethodPost, path: "/v1/completions", body: `{"model":"public-model"}`, options: fixtureOptions{client: enabledClient(), key: key, unhealthy: true}, wantStatus: 503, wantCode: "backend_unavailable"},
		{name: "overloaded", method: http.MethodPost, path: "/v1/completions", body: `{"model":"public-model"}`, options: fixtureOptions{client: backgroundClient(), key: key, poolState: domain.PoolSaturated}, wantStatus: 429, wantCode: "gateway_overloaded"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			handler, _ := newFixture(t, tt.options)
			request := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			request.Header.Set("Authorization", "Bearer "+raw)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
			}
			assertErrorCode(t, response.Body.Bytes(), tt.wantCode)
			if tt.wantStatus == http.StatusTooManyRequests || tt.wantStatus == http.StatusServiceUnavailable {
				if response.Header().Get("Retry-After") != "2" {
					t.Fatalf("Retry-After = %q", response.Header().Get("Retry-After"))
				}
			}
		})
	}
}

type fixtureOptions struct {
	client     domain.Client
	key        domain.APIKey
	forwarder  gateway.Forwarder
	poolState  domain.PoolState
	unhealthy  bool
	extraPools bool
}

func newFixture(t *testing.T, options fixtureOptions) (http.Handler, *runtimeStub) {
	t.Helper()
	pool := domain.ModelPool{ID: 10, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: true}
	backend := domain.Backend{ID: 20, ModelPoolID: pool.ID, Name: "gpu", BaseURL: "http://backend.invalid", Enabled: true, CapacityHint: 1, RunningSoftLimit: 8}
	data := registry.Data{
		Revision: 1, Clients: []domain.Client{options.client}, Keys: []domain.APIKey{options.key},
		Pools: []domain.ModelPool{pool}, Access: []domain.ClientModelAccess{{ClientID: options.client.ID, ModelPoolID: pool.ID, Enabled: true}},
		Backends: []domain.Backend{backend},
	}
	if options.extraPools {
		data.Pools = append(data.Pools,
			domain.ModelPool{ID: 11, PublicModelName: "disabled-model", UpstreamModelName: "disabled", Enabled: false},
			domain.ModelPool{ID: 12, PublicModelName: "not-allowed", UpstreamModelName: "other", Enabled: true},
		)
	}
	loader := staticLoader{data: data}
	reg := registry.New(loader)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	state := options.poolState
	if state == "" {
		state = domain.PoolNormal
	}
	runtime := &runtimeStub{pool: domain.PoolRuntime{PoolID: pool.ID, State: state, AvailableBackends: 1}, runtime: domain.BackendRuntime{
		BackendID: backend.ID, Healthy: !options.unhealthy, MetricsFresh: !options.unhealthy, State: domain.BackendHealthy, Pressure: .3,
	}}
	forwarder := options.forwarder
	if forwarder == nil {
		forwarder = &capturingForwarder{}
	}
	service := gateway.New(gateway.Dependencies{
		Registry: reg, HMACSecret: []byte(strings.Repeat("s", 32)),
		Limiter: admission.NewLimiter(), Runtime: runtime,
		Router: routing.New(.02, routing.FixedSource(0)), Forwarder: forwarder,
		Now: func() time.Time { return testNow }, LookupEnv: func(string) (string, bool) { return "", false },
	})
	handler := httpapi.NewPublicHandler(service, 1024*1024, func() (string, error) { return "fixed-gateway-id", nil })
	return handler, runtime
}

type staticLoader struct{ data registry.Data }

func (l staticLoader) LoadSnapshot(context.Context) (registry.Data, error) { return l.data, nil }

type runtimeStub struct {
	mu      sync.Mutex
	pool    domain.PoolRuntime
	runtime domain.BackendRuntime
}

func (r *runtimeStub) PoolSnapshot(int64, time.Time) domain.PoolRuntime { return r.pool }
func (r *runtimeStub) Snapshot(int64, time.Time) domain.BackendRuntime  { return r.runtime }
func (r *runtimeStub) IncrementInflight(int64) (func(), bool) {
	r.mu.Lock()
	r.runtime.GatewayInflight++
	r.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { r.mu.Lock(); r.runtime.GatewayInflight--; r.mu.Unlock() }) }, true
}

type capturingForwarder struct {
	mu      sync.Mutex
	request proxy.Request
}

func (f *capturingForwarder) Forward(_ context.Context, writer http.ResponseWriter, request proxy.Request) proxy.Result {
	f.mu.Lock()
	f.request = request
	f.mu.Unlock()
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	written, _ := writer.Write([]byte(`{"ok":true}`))
	return proxy.Result{BackendID: request.Target.Backend.ID, Status: http.StatusOK, BytesSent: int64(written)}
}

func (f *capturingForwarder) Request() proxy.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.request
}

type blockingForwarder struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingForwarder() *blockingForwarder {
	return &blockingForwarder{started: make(chan struct{}), release: make(chan struct{})}
}

func (f *blockingForwarder) Forward(ctx context.Context, writer http.ResponseWriter, request proxy.Request) proxy.Result {
	f.once.Do(func() { close(f.started) })
	select {
	case <-f.release:
		writer.WriteHeader(http.StatusOK)
		written, _ := writer.Write([]byte(`{"ok":true}`))
		return proxy.Result{BackendID: request.Target.Backend.ID, Status: http.StatusOK, BytesSent: int64(written)}
	case <-ctx.Done():
		return proxy.Result{BackendID: request.Target.Backend.ID, Cancelled: true, Err: ctx.Err()}
	}
}

func testKey(t *testing.T) (string, domain.APIKey) {
	t.Helper()
	plaintext, err := apikey.Generate(bytes.NewReader(bytes.Repeat([]byte{0x42}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte(strings.Repeat("s", 32))
	return plaintext.Value, domain.APIKey{
		ID: 2, ClientID: 1, Prefix: plaintext.Prefix, SecretHash: apikey.Digest(secret, plaintext.Value), CreatedAt: testNow,
	}
}

func enabledClient() domain.Client {
	return domain.Client{ID: 1, Name: "client", Enabled: true, PriorityClass: domain.PriorityHigh, VLLMPriority: -10, MaxConcurrency: 2}
}

func disabledClient() domain.Client { value := enabledClient(); value.Enabled = false; return value }
func backgroundClient() domain.Client {
	value := enabledClient()
	value.PriorityClass = domain.PriorityBackground
	value.VLLMPriority = 100
	return value
}
func withRevoked(key domain.APIKey) domain.APIKey {
	value := testNow
	key.RevokedAt = &value
	return key
}
func withExpiry(key domain.APIKey, value time.Time) domain.APIKey { key.ExpiresAt = &value; return key }

func post(t *testing.T, url, rawKey string) *http.Response {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, url+"/v1/completions", strings.NewReader(`{"model":"public-model"}`))
	request.Header.Set("Authorization", "Bearer "+rawKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func assertErrorCode(t *testing.T, body []byte, want string) {
	t.Helper()
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("invalid error JSON %s: %v", body, err)
	}
	if envelope.Error.Code != want {
		t.Fatalf("error code = %q, want %q; body=%s", envelope.Error.Code, want, body)
	}
}
