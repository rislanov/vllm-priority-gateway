package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

func TestAdminSecurityRequiresBasicAuthAndMatchingCSRF(t *testing.T) {
	security, err := httpapi.NewAdminSecurity(httpapi.AdminSecurityConfig{
		Username: "operator", Password: "correct horse battery staple", Random: bytes.NewReader(bytes.Repeat([]byte{7}, 96)),
	})
	if err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		writer.WriteHeader(http.StatusOK)
	})
	handler := security.Wrap(next)

	request := httptest.NewRequest(http.MethodGet, "/admin", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || response.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("unauthenticated response = %d headers=%v", response.Code, response.Header())
	}

	request = httptest.NewRequest(http.MethodGet, "/admin", nil)
	request.SetBasicAuth("operator", "wrong")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong credentials status = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/admin", nil)
	request.SetBasicAuth("operator", "correct horse battery staple")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(response.Result().Cookies()) != 1 {
		t.Fatalf("authorized response = %d cookies=%v", response.Code, response.Result().Cookies())
	}
	csrf := response.Result().Cookies()[0]
	if !csrf.HttpOnly || csrf.SameSite != http.SameSiteStrictMode || csrf.Path != "/admin" {
		t.Fatalf("csrf cookie = %+v", csrf)
	}
	for name, want := range map[string]string{
		"Cache-Control": "no-store", "X-Frame-Options": "DENY", "X-Content-Type-Options": "nosniff",
		"Referrer-Policy": "no-referrer",
	} {
		if got := response.Header().Get(name); got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
	if csp := response.Header().Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, "frame-ancestors 'none'") {
		t.Fatalf("CSP = %q", csp)
	}

	for _, token := range []string{"", "mismatch"} {
		request = httptest.NewRequest(http.MethodPost, "/admin/api/clients", strings.NewReader(`{}`))
		request.SetBasicAuth("operator", "correct horse battery staple")
		request.AddCookie(csrf)
		if token != "" {
			request.Header.Set("X-CSRF-Token", token)
		}
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("csrf token %q status = %d", token, response.Code)
		}
	}

	request = httptest.NewRequest(http.MethodPost, "/admin/api/clients", strings.NewReader(`{}`))
	request.SetBasicAuth("operator", "correct horse battery staple")
	request.AddCookie(csrf)
	request.Header.Set("X-CSRF-Token", csrf.Value)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("matching csrf status = %d", response.Code)
	}
}

func TestAdminCRUDPublishesEveryRevisionAndDisclosesKeyOnce(t *testing.T) {
	handler, registryValue, runtime := newAdminFixture(t)
	csrf := fetchCSRF(t, handler)
	revision := int64(0)

	poolResponse := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/pools", map[string]any{
		"publicModelName": "qwen-72b", "upstreamModelName": "Qwen/Qwen2.5-72B-Instruct", "enabled": true,
	}, http.StatusCreated)
	poolID := jsonInt64(t, poolResponse, "id")
	assertRevision(t, registryValue, &revision)

	backendResponse := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/backends", map[string]any{
		"modelPoolId": poolID, "name": "gpu-a", "baseUrl": "http://127.0.0.1:9001", "enabled": true,
		"capacityHint": 1, "runningSoftLimit": 16,
	}, http.StatusCreated)
	backendID := jsonInt64(t, backendResponse, "id")
	assertRevision(t, registryValue, &revision)

	clientResponse := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/clients", map[string]any{
		"name": "payments", "enabled": true, "priorityClass": "critical", "vllmPriority": -100,
		"maxConcurrency": 24, "modelPoolIds": []int64{poolID},
	}, http.StatusCreated)
	clientID := jsonInt64(t, clientResponse, "id")
	assertRevision(t, registryValue, &revision)

	adminJSON(t, handler, csrf, http.MethodPut, "/admin/api/clients/"+strconv.FormatInt(clientID, 10), map[string]any{
		"name": "payments", "enabled": false, "priorityClass": "high", "vllmPriority": -50,
		"maxConcurrency": 12, "modelPoolIds": []int64{poolID},
	}, http.StatusOK)
	assertRevision(t, registryValue, &revision)

	keyResponse := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/clients/"+strconv.FormatInt(clientID, 10)+"/keys", map[string]any{}, http.StatusCreated)
	secret, ok := keyResponse["secret"].(string)
	if !ok || !strings.HasPrefix(secret, "llmgw_") {
		t.Fatalf("key response = %#v", keyResponse)
	}
	keyID := jsonInt64(t, keyResponse, "id")
	assertRevision(t, registryValue, &revision)

	adminJSON(t, handler, csrf, http.MethodDelete, "/admin/api/keys/"+strconv.FormatInt(keyID, 10), nil, http.StatusNoContent)
	assertRevision(t, registryValue, &revision)
	adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/backends/"+strconv.FormatInt(backendID, 10)+"/drain", nil, http.StatusOK)
	assertRevision(t, registryValue, &revision)
	adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/backends/"+strconv.FormatInt(backendID, 10)+"/resume", nil, http.StatusOK)
	assertRevision(t, registryValue, &revision)

	var listed strings.Builder
	for _, path := range []string{"/admin/api/clients", "/admin/api/pools", "/admin/api/backends", "/admin/api/status"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.SetBasicAuth(adminUser, adminPassword)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s = %d body=%s", path, response.Code, response.Body.String())
		}
		listed.WriteString(response.Body.String())
	}
	if strings.Contains(listed.String(), secret) {
		t.Fatal("one-time API key leaked into a subsequent list response")
	}
	if !strings.Contains(listed.String(), `"revision":8`) || runtime.ReconcileCount() != 8 {
		t.Fatalf("aggregate status/reconcile count: body=%s reconciles=%d", listed.String(), runtime.ReconcileCount())
	}
}

func TestAdminPoolSafetyJSONRoundTripAndValidation(t *testing.T) {
	handler, _, _ := newAdminFixture(t)
	csrf := fetchCSRF(t, handler)

	created := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/pools", map[string]any{
		"publicModelName": "qwen-72b", "upstreamModelName": "Qwen/Qwen2.5-72B-Instruct", "enabled": true,
		"maxGatewayInflight": 17, "maxWaiting": 9,
	}, http.StatusCreated)
	poolID := jsonInt64(t, created, "id")
	assertJSONNumber(t, created, "maxGatewayInflight", 17)
	assertJSONNumber(t, created, "maxWaiting", 9)

	updated := adminJSON(t, handler, csrf, http.MethodPut, "/admin/api/pools/"+strconv.FormatInt(poolID, 10), map[string]any{
		"publicModelName": "qwen-72b-updated", "upstreamModelName": "Qwen/Qwen2.5-72B-Instruct", "enabled": true,
		"maxGatewayInflight": 17, "maxWaiting": 9,
	}, http.StatusOK)
	assertJSONNumber(t, updated, "maxGatewayInflight", 17)
	assertJSONNumber(t, updated, "maxWaiting", 9)

	compatible := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/pools", map[string]any{
		"publicModelName": "legacy-client", "upstreamModelName": "legacy-upstream", "enabled": true,
	}, http.StatusCreated)
	assertJSONNumber(t, compatible, "maxGatewayInflight", 0)
	assertJSONNumber(t, compatible, "maxWaiting", 0)

	request := httptest.NewRequest(http.MethodGet, "/admin/api/pools", nil)
	request.SetBasicAuth(adminUser, adminPassword)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET pools = %d body=%s", response.Code, response.Body.String())
	}
	var listed struct {
		Pools []map[string]any `json:"pools"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	pools := listed.Pools
	if len(pools) != 2 {
		t.Fatalf("GET pools length = %d, want 2", len(pools))
	}
	var viewed map[string]any
	for _, pool := range pools {
		if pool["id"] == float64(poolID) {
			viewed = pool
		}
	}
	if viewed == nil {
		t.Fatalf("updated pool missing from JSON: %#v", pools)
	}
	assertJSONNumber(t, viewed, "maxGatewayInflight", 17)
	assertJSONNumber(t, viewed, "maxWaiting", 9)

	createError := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/pools", map[string]any{
		"publicModelName": "invalid-create", "upstreamModelName": "upstream", "enabled": true,
		"maxGatewayInflight": -1, "maxWaiting": 0,
	}, http.StatusBadRequest)
	assertAdminValidationError(t, createError, "max gateway inflight cannot be negative")

	updateError := adminJSON(t, handler, csrf, http.MethodPut, "/admin/api/pools/"+strconv.FormatInt(poolID, 10), map[string]any{
		"publicModelName": "invalid-update", "upstreamModelName": "upstream", "enabled": true,
		"maxGatewayInflight": 0, "maxWaiting": -1,
	}, http.StatusBadRequest)
	assertAdminValidationError(t, updateError, "max waiting cannot be negative")
}

func TestRevocationPublishesAfterRequestCancellation(t *testing.T) {
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	registryValue := registry.New(database)
	if err := registryValue.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	runtime := &adminRuntimeStub{values: make(map[int64]domain.BackendRuntime)}
	wrapper := &cancellingStore{SQLite: database}
	service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: wrapper, Registry: registryValue, Runtime: runtime,
		HMACSecret: []byte(strings.Repeat("h", 32)), Random: bytes.NewReader(bytes.Repeat([]byte{9}, 256)),
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := service.CreateClient(context.Background(), httpapi.ClientInput{
		Name: "revoked-client", Enabled: true, PriorityClass: domain.PriorityHigh, VLLMPriority: -10, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := service.CreateKey(context.Background(), client.ID, httpapi.KeyInput{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	wrapper.cancel = cancel
	if err := service.RevokeKey(ctx, key.ID); err != nil {
		t.Fatalf("RevokeKey() after committed cancellation = %v", err)
	}
	candidates := registryValue.Snapshot().KeyCandidates[key.Prefix]
	if len(candidates) != 1 || candidates[0].RevokedAt == nil {
		t.Fatalf("revocation was not published: %+v", candidates)
	}
}

func TestRevocationRemainsFailClosedWhenReloadFails(t *testing.T) {
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	registryValue := registry.New(database)
	if err := registryValue.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	failingRegistry := &reloadFailureRegistry{Registry: registryValue}
	service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Registry: failingRegistry,
		Runtime:    &adminRuntimeStub{values: make(map[int64]domain.BackendRuntime)},
		HMACSecret: []byte(strings.Repeat("h", 32)), Random: bytes.NewReader(bytes.Repeat([]byte{9}, 256)),
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := service.CreateClient(context.Background(), httpapi.ClientInput{
		Name: "revoked-client", Enabled: true, PriorityClass: domain.PriorityHigh, VLLMPriority: -10, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := service.CreateKey(context.Background(), client.ID, httpapi.KeyInput{})
	if err != nil {
		t.Fatal(err)
	}
	failingRegistry.fail = true
	if err := service.RevokeKey(context.Background(), key.ID); err == nil {
		t.Fatal("expected degraded reload error")
	}
	candidates := registryValue.Snapshot().KeyCandidates[key.Prefix]
	if len(candidates) != 1 || candidates[0].RevokedAt == nil {
		t.Fatalf("revoked key remained active after reload failure: %+v", candidates)
	}
}

const (
	adminUser     = "operator"
	adminPassword = "correct horse battery staple"
)

func newAdminFixture(t *testing.T) (http.Handler, *registry.Registry, *adminRuntimeStub) {
	t.Helper()
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	if err := database.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	registryValue := registry.New(database)
	if err := registryValue.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	runtime := &adminRuntimeStub{values: make(map[int64]domain.BackendRuntime)}
	service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Registry: registryValue, Runtime: runtime,
		HMACSecret: []byte(strings.Repeat("h", 32)), Random: bytes.NewReader(bytes.Repeat([]byte{9}, 4096)),
		Now: func() time.Time { return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	security, err := httpapi.NewAdminSecurity(httpapi.AdminSecurityConfig{
		Username: adminUser, Password: adminPassword, Random: bytes.NewReader(bytes.Repeat([]byte{8}, 4096)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return security.Wrap(httpapi.NewAdminAPI(service)), registryValue, runtime
}

func fetchCSRF(t *testing.T, handler http.Handler) *http.Cookie {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/admin/api/status", nil)
	request.SetBasicAuth(adminUser, adminPassword)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(response.Result().Cookies()) != 1 {
		t.Fatalf("csrf bootstrap = %d cookies=%v body=%s", response.Code, response.Result().Cookies(), response.Body.String())
	}
	return response.Result().Cookies()[0]
}

func adminJSON(t *testing.T, handler http.Handler, csrf *http.Cookie, method, path string, input any, wantStatus int) map[string]any {
	t.Helper()
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(encoded)
	}
	request := httptest.NewRequest(method, path, body)
	request.SetBasicAuth(adminUser, adminPassword)
	request.AddCookie(csrf)
	request.Header.Set("X-CSRF-Token", csrf.Value)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != wantStatus {
		t.Fatalf("%s %s = %d, want %d body=%s", method, path, response.Code, wantStatus, response.Body.String())
	}
	if response.Body.Len() == 0 {
		return nil
	}
	var output map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
		t.Fatalf("decode response: %v body=%s", err, response.Body.String())
	}
	return output
}

func jsonInt64(t *testing.T, value map[string]any, key string) int64 {
	t.Helper()
	number, ok := value[key].(float64)
	if !ok {
		t.Fatalf("%s missing from %#v", key, value)
	}
	return int64(number)
}

func assertJSONNumber(t *testing.T, value map[string]any, key string, want float64) {
	t.Helper()
	got, ok := value[key].(float64)
	if !ok || got != want {
		t.Fatalf("%s = %#v, want %v in %#v", key, value[key], want, value)
	}
}

func assertAdminValidationError(t *testing.T, value map[string]any, wantMessage string) {
	t.Helper()
	errorValue, ok := value["error"].(map[string]any)
	if !ok || errorValue["code"] != "validation_error" || errorValue["message"] != wantMessage {
		t.Fatalf("validation error = %#v, want code validation_error and message %q", value, wantMessage)
	}
}

func assertRevision(t *testing.T, registryValue *registry.Registry, revision *int64) {
	t.Helper()
	*revision++
	if got := registryValue.Snapshot().Revision; got != *revision {
		t.Fatalf("published revision = %d, want %d", got, *revision)
	}
}

type adminRuntimeStub struct {
	mu         sync.Mutex
	reconciles int
	values     map[int64]domain.BackendRuntime
}

type cancellingStore struct {
	*store.SQLite
	cancel context.CancelFunc
}

type reloadFailureRegistry struct {
	*registry.Registry
	fail bool
}

func (r *reloadFailureRegistry) Reload(ctx context.Context) error {
	if r.fail {
		return errors.New("forced reload failure")
	}
	return r.Registry.Reload(ctx)
}

func (s *cancellingStore) RevokeAPIKey(ctx context.Context, id int64) error {
	err := s.SQLite.RevokeAPIKey(ctx, id)
	if err == nil && s.cancel != nil {
		s.cancel()
	}
	return err
}

func (r *adminRuntimeStub) Reconcile(backends []domain.Backend) error {
	r.mu.Lock()
	r.reconciles++
	for _, backend := range backends {
		if _, exists := r.values[backend.ID]; !exists {
			r.values[backend.ID] = domain.BackendRuntime{BackendID: backend.ID, State: domain.BackendHealthy, Healthy: true, MetricsFresh: true, Pressure: .42}
		}
	}
	r.mu.Unlock()
	return nil
}

func (r *adminRuntimeStub) Snapshot(id int64, _ time.Time) domain.BackendRuntime {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.values[id]
}

func (r *adminRuntimeStub) PoolSnapshot(id int64, _ time.Time) domain.PoolRuntime {
	return domain.PoolRuntime{PoolID: id, State: domain.PoolNormal, AvailableBackends: 1, BestBackendPressure: .42}
}

func (r *adminRuntimeStub) ReconcileCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reconciles
}
