package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/monitor"
	"github.com/rislanov/vllm-priority-gateway/internal/observability"
	"github.com/rislanov/vllm-priority-gateway/internal/pressure"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
	"github.com/rislanov/vllm-priority-gateway/internal/web"
)

const (
	adminUsername = "acceptance-operator"
	adminPassword = "correct horse battery staple"
)

var hmacSecret = []byte(strings.Repeat("a", 32))

type harness struct {
	t            *testing.T
	ctx          context.Context
	cancel       context.CancelFunc
	database     *store.SQLite
	databasePath string
	registry     *registry.Registry
	manager      *monitor.Manager
	metrics      *observability.Metrics
	server       *httptest.Server
	client       *http.Client
	csrf         string
	fakes        []*fakevllm.Server
	upstreams    []*httptest.Server
}

type harnessUsage struct {
	database *store.SQLite
	registry *registry.Registry
}

func TestHarnessCreatePoolWithSafetyLimits(t *testing.T) {
	h := newHarness(t)
	poolID := h.createPoolWithLimits("limited", 17, 9)
	pool := h.registry.Snapshot().PoolsByID[poolID]
	if pool.MaxGatewayInflight != 17 || pool.MaxWaiting != 9 {
		t.Fatalf("harness pool safety limits = (%d, %d), want (17, 9)", pool.MaxGatewayInflight, pool.MaxWaiting)
	}
}

func (u harnessUsage) Record(keyID int64, usedAt time.Time) {
	if u.database.TouchKeyLastUsed(context.Background(), keyID, usedAt) == nil {
		u.registry.MarkKeyUsed(keyID, usedAt)
	}
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	databasePath := filepath.Join(t.TempDir(), "acceptance.db")
	database, err := store.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	registryValue := registry.New(database)
	if err := registryValue.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	manager := monitor.NewManager(ctx, monitor.Options{
		HTTPClient: client, HealthInterval: 15 * time.Millisecond, HealthTimeout: 100 * time.Millisecond,
		MetricsInterval: 15 * time.Millisecond, MetricsTimeout: 100 * time.Millisecond, StaleAfter: 150 * time.Millisecond,
		UnhealthyAfter: 3, RecoveryAfter: 2,
		Limits: pressure.Limits{QueueSoft: 2, KVSoft: .8, KVHard: .95}, EWMAWindow: 10 * time.Millisecond,
		BusyThreshold: .7, SaturatedThreshold: 1,
		PoolThresholds: pressure.Thresholds{
			Busy: .7, Saturated: 1, Emergency: 1.4, BusyRecovery: .55,
			SaturatedRecovery: .85, EmergencyRecovery: 1.2,
			EnterWindow: 60 * time.Millisecond, RecoveryWindow: 90 * time.Millisecond,
		},
	})
	metrics := observability.NewMetrics()
	service := gateway.New(gateway.Dependencies{
		Registry: registryValue, HMACSecret: hmacSecret, Limiter: admission.NewLimiter(), Runtime: manager,
		Router: routing.New(.02, routing.FixedSource(0)), Forwarder: proxy.New(client), Observer: metrics,
		Usage: harnessUsage{database: database, registry: registryValue}, LookupEnv: os.LookupEnv,
	})
	publicHandler := httpapi.NewPublicHandler(service, 1<<20, nil)
	adminService, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Registry: registryValue, Runtime: manager, HMACSecret: hmacSecret, Random: rand.Reader,
	})
	if err != nil {
		t.Fatal(err)
	}
	adminWeb, err := web.New(adminService)
	if err != nil {
		t.Fatal(err)
	}
	security, err := httpapi.NewAdminSecurity(httpapi.AdminSecurityConfig{
		Username: adminUsername, Password: adminPassword, Random: rand.Reader,
	})
	if err != nil {
		t.Fatal(err)
	}
	adminAPI := httpapi.NewAdminAPI(adminService)
	router := chi.NewRouter()
	router.Handle("/metrics", metrics.Handler())
	router.Handle("/v1", publicHandler)
	router.Handle("/v1/*", publicHandler)
	router.Handle("/admin/api", security.Wrap(adminAPI))
	router.Handle("/admin/api/*", security.Wrap(adminAPI))
	router.Handle("/admin", security.Wrap(adminWeb))
	router.Handle("/admin/*", security.Wrap(adminWeb))
	server := httptest.NewServer(router)
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{
		t: t, ctx: ctx, cancel: cancel, database: database, databasePath: databasePath,
		registry: registryValue, manager: manager, metrics: metrics, server: server,
		client: &http.Client{Transport: client.Transport, Jar: jar, Timeout: 3 * time.Second},
	}
	h.bootstrapCSRF()
	t.Cleanup(h.close)
	return h
}

func (h *harness) close() {
	h.server.Close()
	h.cancel()
	h.manager.Shutdown()
	for _, upstream := range h.upstreams {
		upstream.Close()
	}
	if err := h.database.Close(); err != nil {
		h.t.Errorf("close database: %v", err)
	}
	if snapshot := h.manager.WorkerCount(); snapshot != 0 {
		h.t.Errorf("monitor workers leaked: %d", snapshot)
	}
}

func (h *harness) bootstrapCSRF() {
	response := h.admin(http.MethodGet, "/admin/api/status", nil, http.StatusOK)
	response.Body.Close()
	parsed, _ := url.Parse(h.server.URL)
	parsed.Path = "/admin"
	for _, cookie := range h.client.Jar.Cookies(parsed) {
		if cookie.Name == "llmgw_csrf" {
			h.csrf = cookie.Value
		}
	}
	if h.csrf == "" {
		h.t.Fatal("admin bootstrap did not issue a CSRF cookie")
	}
}

func (h *harness) admin(method, path string, input any, wantStatus int) *http.Response {
	h.t.Helper()
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			h.t.Fatal(err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, h.server.URL+path, body)
	if err != nil {
		h.t.Fatal(err)
	}
	request.SetBasicAuth(adminUsername, adminPassword)
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if method != http.MethodGet {
		request.Header.Set("X-CSRF-Token", h.csrf)
	}
	response, err := h.client.Do(request)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	if response.StatusCode != wantStatus {
		payload, _ := io.ReadAll(response.Body)
		response.Body.Close()
		h.t.Fatalf("%s %s = %d, want %d: %s", method, path, response.StatusCode, wantStatus, payload)
	}
	return response
}

func (h *harness) adminObject(method, path string, input any, wantStatus int) map[string]any {
	h.t.Helper()
	response := h.admin(method, path, input, wantStatus)
	defer response.Body.Close()
	if wantStatus == http.StatusNoContent {
		return nil
	}
	var output map[string]any
	if err := json.NewDecoder(response.Body).Decode(&output); err != nil {
		h.t.Fatal(err)
	}
	return output
}

func (h *harness) createPool(publicName string) int64 {
	return h.createPoolWithLimits(publicName, 0, 0)
}

func (h *harness) createPoolWithLimits(publicName string, maxGatewayInflight, maxWaiting int) int64 {
	h.t.Helper()
	output := h.adminObject(http.MethodPost, "/admin/api/pools", map[string]any{
		"publicModelName": publicName, "upstreamModelName": "fake-model", "enabled": true,
		"maxGatewayInflight": maxGatewayInflight, "maxWaiting": maxWaiting,
	}, http.StatusCreated)
	return numberID(h.t, output["id"])
}

func (h *harness) addFake(poolID int64, name string, state fakevllm.State) (*fakevllm.Server, int64) {
	h.t.Helper()
	fake := fakevllm.New()
	fake.SetState(state)
	upstream := httptest.NewServer(fake.Handler())
	h.fakes = append(h.fakes, fake)
	h.upstreams = append(h.upstreams, upstream)
	output := h.adminObject(http.MethodPost, "/admin/api/backends", map[string]any{
		"modelPoolId": poolID, "name": name, "baseUrl": upstream.URL, "enabled": true,
		"draining": false, "capacityHint": 1, "runningSoftLimit": 16, "upstreamApiKeyEnv": "",
	}, http.StatusCreated)
	return fake, numberID(h.t, output["id"])
}

func (h *harness) createClient(name string, class domain.PriorityClass, priority, concurrency int, poolIDs ...int64) (int64, string) {
	h.t.Helper()
	output := h.adminObject(http.MethodPost, "/admin/api/clients", map[string]any{
		"name": name, "enabled": true, "priorityClass": class, "vllmPriority": priority,
		"maxConcurrency": concurrency, "modelPoolIds": poolIDs,
	}, http.StatusCreated)
	clientID := numberID(h.t, output["id"])
	key := h.adminObject(http.MethodPost, "/admin/api/clients/"+strconv.FormatInt(clientID, 10)+"/keys", map[string]any{}, http.StatusCreated)
	return clientID, key["secret"].(string)
}

func (h *harness) public(method, path, key, body string) (*http.Response, []byte) {
	h.t.Helper()
	request, err := http.NewRequest(method, h.server.URL+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	if key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := h.client.Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	payload, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		h.t.Fatal(err)
	}
	return response, payload
}

func (h *harness) waitBackend(id int64, predicate func(domain.BackendRuntime) bool) domain.BackendRuntime {
	h.t.Helper()
	var last domain.BackendRuntime
	eventually(h.t, 2*time.Second, func() bool {
		last = h.manager.Snapshot(id, time.Now())
		return predicate(last)
	})
	return last
}

func (h *harness) waitPool(id int64, state domain.PoolState) domain.PoolRuntime {
	h.t.Helper()
	var last domain.PoolRuntime
	eventually(h.t, 2*time.Second, func() bool {
		last = h.manager.PoolSnapshot(id, time.Now())
		return last.State == state
	})
	return last
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true before deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func numberID(t *testing.T, value any) int64 {
	t.Helper()
	number, ok := value.(float64)
	if !ok {
		t.Fatalf("ID value = %#v", value)
	}
	return int64(number)
}

func databaseBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func postBody(model string, stream bool) string {
	return fmt.Sprintf(`{"model":%q,"prompt":"acceptance","stream":%t,"priority":-999}`, model, stream)
}
