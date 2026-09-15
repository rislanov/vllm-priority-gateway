package httpapi_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
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
		writer.Header().Set("X-Rendered-CSRF", httpapi.AdminCSRFToken(request))
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
	if rendered := response.Header().Get("X-Rendered-CSRF"); rendered == "" || rendered != csrf.Value {
		t.Fatalf("first rendered CSRF token = %q, cookie = %q", rendered, csrf.Value)
	}
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

func TestAdminClientListUsesSingleRegistrySnapshot(t *testing.T) {
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	registryValue := &alternatingAdminRegistry{snapshots: []*registry.Snapshot{
		{
			Revision: 41,
			Clients: map[int64]domain.Client{
				1: {ID: 1, Name: "snapshot-41", Enabled: true, PriorityClass: domain.PriorityNormal},
			},
		},
		{
			Revision: 42,
			Clients: map[int64]domain.Client{
				2: {ID: 2, Name: "snapshot-42", Enabled: true, PriorityClass: domain.PriorityHigh},
			},
		},
	}}
	service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Analytics: database, Registry: registryValue,
		Runtime:    &adminRuntimeStub{values: make(map[int64]domain.BackendRuntime)},
		HMACSecret: []byte(strings.Repeat("h", 32)),
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/admin/api/clients", nil)
	response := httptest.NewRecorder()
	httpapi.NewAdminAPI(service).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET /admin/api/clients = %d body=%s", response.Code, response.Body.String())
	}
	var got struct {
		Revision int64                 `json:"revision"`
		Clients  []httpapi.AdminClient `json:"clients"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Revision != 41 || len(got.Clients) != 1 || got.Clients[0].ID != 1 || got.Clients[0].Name != "snapshot-41" {
		t.Fatalf("client list combined registry snapshots: %+v", got)
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
		"maxConcurrency": 24, "requestsPerMinute": 600, "tokensPerMinute": 60000, "modelPoolIds": []int64{poolID},
	}, http.StatusCreated)
	if clientResponse["requestsPerMinute"] != float64(600) || clientResponse["tokensPerMinute"] != float64(60000) {
		t.Fatalf("rate policy response=%#v", clientResponse)
	}
	clientID := jsonInt64(t, clientResponse, "id")
	assertRevision(t, registryValue, &revision)

	adminJSON(t, handler, csrf, http.MethodPut, "/admin/api/clients/"+strconv.FormatInt(clientID, 10), map[string]any{
		"name": "payments", "enabled": false, "priorityClass": "high", "vllmPriority": -50,
		"maxConcurrency": 12, "requestsPerMinute": 300, "tokensPerMinute": 30000, "modelPoolIds": []int64{poolID},
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

func TestAdminDeleteConfigurationPublishesAndEnforcesReferences(t *testing.T) {
	handler, registryValue, runtime := newAdminFixture(t)
	csrf := fetchCSRF(t, handler)
	revision := int64(0)

	pool := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/pools", map[string]any{
		"publicModelName": "qwen-72b", "upstreamModelName": "Qwen/Qwen2.5-72B-Instruct", "enabled": true,
	}, http.StatusCreated)
	poolID := jsonInt64(t, pool, "id")
	assertRevision(t, registryValue, &revision)

	backend := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/backends", map[string]any{
		"modelPoolId": poolID, "name": "gpu-a", "baseUrl": "http://127.0.0.1:9001", "enabled": true,
		"capacityHint": 1, "runningSoftLimit": 16,
	}, http.StatusCreated)
	backendID := jsonInt64(t, backend, "id")
	assertRevision(t, registryValue, &revision)

	client := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/clients", map[string]any{
		"name": "payments", "enabled": true, "priorityClass": "critical", "vllmPriority": -100,
		"maxConcurrency": 24, "modelPoolIds": []int64{poolID},
	}, http.StatusCreated)
	clientID := jsonInt64(t, client, "id")
	assertRevision(t, registryValue, &revision)

	adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/clients/"+strconv.FormatInt(clientID, 10)+"/keys", map[string]any{}, http.StatusCreated)
	assertRevision(t, registryValue, &revision)

	conflict := adminJSON(t, handler, csrf, http.MethodDelete, "/admin/api/pools/"+strconv.FormatInt(poolID, 10), nil, http.StatusConflict)
	errorValue, ok := conflict["error"].(map[string]any)
	if !ok || errorValue["code"] != "conflict" || errorValue["message"] != "model pool cannot be deleted while backends reference it" {
		t.Fatalf("delete referenced pool response = %#v", conflict)
	}
	if got := registryValue.Snapshot().Revision; got != revision {
		t.Fatalf("revision after rejected pool delete = %d, want %d", got, revision)
	}

	adminJSON(t, handler, csrf, http.MethodDelete, "/admin/api/backends/"+strconv.FormatInt(backendID, 10), nil, http.StatusNoContent)
	assertRevision(t, registryValue, &revision)
	adminJSON(t, handler, csrf, http.MethodDelete, "/admin/api/pools/"+strconv.FormatInt(poolID, 10), nil, http.StatusNoContent)
	assertRevision(t, registryValue, &revision)
	adminJSON(t, handler, csrf, http.MethodDelete, "/admin/api/clients/"+strconv.FormatInt(clientID, 10), nil, http.StatusNoContent)
	assertRevision(t, registryValue, &revision)

	view := registryValue.Snapshot()
	if len(view.Clients) != 0 || len(view.KeyCandidates) != 0 || len(view.PoolsByID) != 0 || len(view.BackendsByID) != 0 || len(view.Access) != 0 {
		t.Fatalf("snapshot after deletes = %+v", view)
	}
	if runtime.ReconcileCount() != int(revision)+1 {
		t.Fatalf("runtime reconciles = %d, want %d including fail-closed backend removal", runtime.ReconcileCount(), revision+1)
	}
}

func TestAdminPublishSerializesSnapshotAndRuntimeReconcile(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	pool, err := database.CreatePool(ctx, store.CreatePoolParams{
		PublicModelName: "public", UpstreamModelName: "upstream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	baseRegistry := registry.New(database)
	if err := baseRegistry.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	blockingRegistry := &snapshotBlockingRegistry{
		Registry: baseRegistry, blocked: make(chan struct{}), release: make(chan struct{}),
	}
	runtime := &reconcileHistoryRuntime{}
	service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Analytics: database, Registry: blockingRegistry, Runtime: runtime,
		HMACSecret: []byte(strings.Repeat("s", 32)),
	})
	if err != nil {
		t.Fatal(err)
	}
	create := func(name string) error {
		_, err := service.CreateBackend(ctx, httpapi.BackendInput{
			ModelPoolID: pool.ID, Name: name, BaseURL: "http://" + name + ".invalid",
			Enabled: true, CapacityHint: 1, RunningSoftLimit: 8,
		})
		return err
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- create("gpu-a") }()
	<-blockingRegistry.blocked
	secondDone := make(chan error, 1)
	go func() { secondDone <- create("gpu-b") }()
	deadline := time.Now().Add(time.Second)
	for {
		data, loadErr := database.LoadSnapshot(ctx)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if data.Revision >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second configuration transaction did not commit while first publication was paused")
		}
		time.Sleep(time.Millisecond)
	}
	close(blockingRegistry.release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if got := runtime.LatestIDs(); len(got) != 2 || got[0] == got[1] {
		t.Fatalf("latest reconciled backend IDs = %v, want both current backends", got)
	}
	if revision := baseRegistry.Snapshot().Revision; revision != 3 {
		t.Fatalf("published revision = %d, want 3", revision)
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

	boundary := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/pools", map[string]any{
		"publicModelName": "boundary", "upstreamModelName": "boundary", "enabled": true,
		"maxGatewayInflight": domain.MaxPoolGatewayInflight, "maxWaiting": 0,
	}, http.StatusCreated)
	assertJSONNumber(t, boundary, "maxGatewayInflight", domain.MaxPoolGatewayInflight)

	overMaximum := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/pools", map[string]any{
		"publicModelName": "over-maximum", "upstreamModelName": "over-maximum", "enabled": true,
		"maxGatewayInflight": domain.MaxPoolGatewayInflight + 1, "maxWaiting": 0,
	}, http.StatusBadRequest)
	assertAdminValidationError(t, overMaximum, "max gateway inflight must not exceed 100000")
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
		Store: wrapper, Analytics: wrapper, Registry: registryValue, Runtime: runtime,
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
		Store: database, Analytics: database, Registry: failingRegistry,
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

func TestClientDeletionRemainsFailClosedWhenReloadFails(t *testing.T) {
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
		Store: database, Analytics: database, Registry: failingRegistry,
		Runtime:    &adminRuntimeStub{values: make(map[int64]domain.BackendRuntime)},
		HMACSecret: []byte(strings.Repeat("h", 32)), Random: bytes.NewReader(bytes.Repeat([]byte{9}, 256)),
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := service.CreateClient(context.Background(), httpapi.ClientInput{
		Name: "deleted-client", Enabled: true, PriorityClass: domain.PriorityHigh, VLLMPriority: -10, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := service.CreateKey(context.Background(), client.ID, httpapi.KeyInput{})
	if err != nil {
		t.Fatal(err)
	}
	failingRegistry.fail = true
	if err := service.DeleteClient(context.Background(), client.ID); err == nil {
		t.Fatal("expected degraded reload error")
	}
	candidates := registryValue.Snapshot().KeyCandidates[key.Prefix]
	if len(candidates) != 1 || candidates[0].RevokedAt == nil {
		t.Fatalf("deleted client's key remained active after reload failure: %+v", candidates)
	}
}

func TestClientDeletionSerializesWithKeyPublicationAndRemainsFailClosed(t *testing.T) {
	previousMaxProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousMaxProcs)

	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	loader := newBlockingFailureLoader(database)
	registryValue := registry.New(loader)
	if err := registryValue.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	observedStore := &observedDeleteStore{SQLite: database, deleteCalled: make(chan struct{}, 1)}
	service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: observedStore, Analytics: database, Registry: registryValue,
		Runtime:    &adminRuntimeStub{values: make(map[int64]domain.BackendRuntime)},
		HMACSecret: []byte(strings.Repeat("h", 32)), Random: bytes.NewReader(bytes.Repeat([]byte{9}, 256)),
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := service.CreateClient(context.Background(), httpapi.ClientInput{
		Name: "racing-client", Enabled: true, PriorityClass: domain.PriorityHigh, VLLMPriority: -10, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	loader.ArmBlockThenFail()
	created := make(chan httpapi.CreatedKey, 1)
	createErrors := make(chan error, 1)
	go func() {
		key, createErr := service.CreateKey(context.Background(), client.ID, httpapi.KeyInput{})
		created <- key
		createErrors <- createErr
	}()
	<-loader.blocked

	deleteErrors := make(chan error, 1)
	deleteStarted := make(chan struct{})
	go func() {
		close(deleteStarted)
		deleteErrors <- service.DeleteClient(context.Background(), client.ID)
	}()
	<-deleteStarted
	runtime.Gosched()
	select {
	case <-observedStore.deleteCalled:
		close(loader.release)
		<-createErrors
		<-deleteErrors
		t.Fatal("client deletion reached SQLite while an older key snapshot was still being published")
	default:
	}
	close(loader.release)
	key := <-created
	if err := <-createErrors; err != nil {
		t.Fatalf("CreateKey() = %v", err)
	}
	if err := <-deleteErrors; err == nil || !strings.Contains(err.Error(), "forced reload failure") {
		t.Fatalf("DeleteClient() error = %v, want forced reload failure", err)
	}

	gatewayService := gateway.New(gateway.Dependencies{Registry: registryValue, HMACSecret: []byte(strings.Repeat("h", 32))})
	if _, apiErr := gatewayService.ValidateAPIKey(key.Secret); apiErr == nil {
		t.Fatal("deleted client's concurrently published API key still authenticates")
	}
	if err := service.DeleteClient(context.Background(), client.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("retry DeleteClient() error = %v, want sql.ErrNoRows after convergence", err)
	}
	if _, exists := registryValue.Snapshot().Clients[client.ID]; exists {
		t.Fatal("retry did not remove the deleted client from the registry")
	}
}

func TestBackendDeletionRemainsFailClosedAndRetryConvergesAfterReloadFailure(t *testing.T) {
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
	runtime := &reconcileHistoryRuntime{}
	service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Analytics: database, Registry: failingRegistry, Runtime: runtime,
		HMACSecret: []byte(strings.Repeat("h", 32)), Random: bytes.NewReader(bytes.Repeat([]byte{9}, 256)),
	})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := service.CreatePool(context.Background(), httpapi.PoolInput{
		PublicModelName: "model", UpstreamModelName: "upstream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	backend, err := service.CreateBackend(context.Background(), httpapi.BackendInput{
		ModelPoolID: pool.ID, Name: "backend", BaseURL: "http://127.0.0.1:8000", Enabled: true, CapacityHint: 1, RunningSoftLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	failingRegistry.fail = true
	if err := service.DeleteBackend(context.Background(), backend.ID); err == nil {
		t.Fatal("expected degraded reload error")
	}
	if _, exists := registryValue.Snapshot().BackendsByID[backend.ID]; exists {
		t.Fatal("deleted backend remained selectable after reload failure")
	}
	if ids := runtime.LatestIDs(); len(ids) != 0 {
		t.Fatalf("runtime still monitors deleted backend IDs %v", ids)
	}

	failingRegistry.fail = false
	if err := service.DeleteBackend(context.Background(), backend.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("retry DeleteBackend() error = %v, want sql.ErrNoRows after convergence", err)
	}
	data, err := database.LoadSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if registryValue.Snapshot().Revision != data.Revision {
		t.Fatalf("retry did not converge registry revision: got %d want %d", registryValue.Snapshot().Revision, data.Revision)
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
		Store: database, Analytics: database, Registry: registryValue, Runtime: runtime,
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

type observedDeleteStore struct {
	*store.SQLite
	deleteCalled chan struct{}
}

type blockingFailureLoader struct {
	loader  registry.Loader
	mu      sync.Mutex
	armed   bool
	fail    bool
	blocked chan struct{}
	release chan struct{}
}

func newBlockingFailureLoader(loader registry.Loader) *blockingFailureLoader {
	return &blockingFailureLoader{loader: loader}
}

func (l *blockingFailureLoader) ArmBlockThenFail() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.armed = true
	l.blocked = make(chan struct{})
	l.release = make(chan struct{})
}

func (l *blockingFailureLoader) LoadSnapshot(ctx context.Context) (registry.Data, error) {
	l.mu.Lock()
	if l.fail {
		l.fail = false
		l.mu.Unlock()
		return registry.Data{}, errors.New("forced reload failure")
	}
	armed := l.armed
	if armed {
		l.armed = false
	}
	blocked := l.blocked
	result := l.release
	l.mu.Unlock()

	data, err := l.loader.LoadSnapshot(ctx)
	if err != nil || !armed {
		return data, err
	}
	close(blocked)
	select {
	case <-result:
	case <-ctx.Done():
		return registry.Data{}, ctx.Err()
	}
	l.mu.Lock()
	l.fail = true
	l.mu.Unlock()
	return data, nil
}

func (s *observedDeleteStore) DeleteClient(ctx context.Context, id int64) ([]int64, error) {
	select {
	case s.deleteCalled <- struct{}{}:
	default:
	}
	return s.SQLite.DeleteClient(ctx, id)
}

type alternatingAdminRegistry struct {
	snapshots []*registry.Snapshot
	next      int
}

type snapshotBlockingRegistry struct {
	*registry.Registry
	mu      sync.Mutex
	armed   bool
	used    bool
	blocked chan struct{}
	release chan struct{}
}

func (r *snapshotBlockingRegistry) Reload(ctx context.Context) error {
	if err := r.Registry.Reload(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	if !r.used {
		r.armed = true
	}
	r.mu.Unlock()
	return nil
}

func (r *snapshotBlockingRegistry) Snapshot() *registry.Snapshot {
	r.mu.Lock()
	if r.armed {
		r.armed = false
		r.used = true
		snapshot := r.Registry.Snapshot()
		close(r.blocked)
		r.mu.Unlock()
		<-r.release
		return snapshot
	}
	r.mu.Unlock()
	return r.Registry.Snapshot()
}

type reconcileHistoryRuntime struct {
	mu     sync.Mutex
	latest []int64
}

func (r *reconcileHistoryRuntime) Reconcile(backends []domain.Backend) error {
	ids := make([]int64, 0, len(backends))
	for _, backend := range backends {
		ids = append(ids, backend.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	r.mu.Lock()
	r.latest = ids
	r.mu.Unlock()
	return nil
}

func (*reconcileHistoryRuntime) Snapshot(id int64, _ time.Time) domain.BackendRuntime {
	return domain.BackendRuntime{BackendID: id}
}

func (*reconcileHistoryRuntime) PoolSnapshot(id int64, _ time.Time) domain.PoolRuntime {
	return domain.PoolRuntime{PoolID: id}
}

func (r *reconcileHistoryRuntime) LatestIDs() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.latest...)
}

func (r *alternatingAdminRegistry) Reload(context.Context) error { return nil }

func (r *alternatingAdminRegistry) MarkKeyRevoked(int64, time.Time) bool { return false }

func (r *alternatingAdminRegistry) MarkBackendDeleted(int64) bool { return false }

func (r *alternatingAdminRegistry) Snapshot() *registry.Snapshot {
	snapshot := r.snapshots[r.next%len(r.snapshots)]
	r.next++
	return snapshot
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

func TestAdminClientPolicyBoundaries(t *testing.T) {
	handler, _, _ := newAdminFixture(t)
	csrf := fetchCSRF(t, handler)
	pool := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/pools", map[string]any{"publicModelName": "limits", "upstreamModelName": "limits", "enabled": true}, http.StatusCreated)
	poolID := jsonInt64(t, pool, "id")
	base := map[string]any{"name": "bounded", "enabled": true, "priorityClass": "high", "vllmPriority": -10, "maxConcurrency": domain.MaxClientConcurrency, "requestsPerMinute": domain.MaxRequestsPerMinute, "tokensPerMinute": domain.MaxTokensPerMinute, "modelPoolIds": []int64{poolID}}
	adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/clients", base, http.StatusCreated)
	for name, value := range map[string]any{"maxConcurrency": domain.MaxClientConcurrency + 1, "requestsPerMinute": domain.MaxRequestsPerMinute + 1, "tokensPerMinute": domain.MaxTokensPerMinute + 1} {
		invalid := maps.Clone(base)
		invalid["name"] = "invalid-" + name
		invalid[name] = value
		adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/clients", invalid, http.StatusBadRequest)
	}
}

func TestAdminMutationGateReturnsSafeRetryableUnavailableError(t *testing.T) {
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	registryValue := registry.New(database)
	if err := registryValue.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Analytics: database, Registry: registryValue,
		Runtime:    &adminRuntimeStub{values: make(map[int64]domain.BackendRuntime)},
		HMACSecret: []byte(strings.Repeat("h", 32)), MutationsAllowed: func() bool { return false },
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := httpapi.NewAdminAPI(service)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/admin/api/clients", strings.NewReader(`{"name":"blocked","enabled":true,"priorityClass":"normal","maxConcurrency":1}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"code":"configuration_unavailable"`) || strings.Contains(response.Body.String(), "postgres://") {
		t.Fatalf("unsafe or incorrect response: %s", response.Body.String())
	}
	if clients, err := database.ListClients(context.Background()); err != nil || len(clients) != 0 {
		t.Fatalf("mutation reached store: clients=%v err=%v", clients, err)
	}
	drain := httptest.NewRecorder()
	drainRequest := httptest.NewRequest(http.MethodPost, "/admin/api/backends/1/drain", nil)
	handler.ServeHTTP(drain, drainRequest)
	if drain.Code != http.StatusServiceUnavailable || !strings.Contains(drain.Body.String(), `"code":"configuration_unavailable"`) {
		t.Fatalf("drain gate status=%d body=%s", drain.Code, drain.Body.String())
	}
}
