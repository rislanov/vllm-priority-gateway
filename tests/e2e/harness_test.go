package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type e2eMode string

const (
	modeSmoke    e2eMode = "smoke"
	modePriority e2eMode = "priority"
)

type e2eConfig struct {
	baseURL            *url.URL
	adminUsername      string
	adminPassword      string
	model              string
	highKey            string
	highLoadKeys       []string
	criticalKey        string
	lowKeys            []string
	expectedBackends   int
	highRequestsPerKey int
	highMaxTokens      int
	saturationTimeout  time.Duration
	recoveryTimeout    time.Duration
	probeTimeout       time.Duration
	requireAllWaiting  bool
	drainBackendID     int64
}

func loadE2EConfig(t *testing.T, required e2eMode) e2eConfig {
	t.Helper()
	mode := e2eMode(strings.TrimSpace(os.Getenv("LLMGW_E2E_MODE")))
	if mode == "" {
		t.Skip("set LLMGW_E2E_MODE=smoke or priority to run external gateway checks")
	}
	if mode != modeSmoke && mode != modePriority {
		t.Fatalf("LLMGW_E2E_MODE = %q, want smoke or priority", mode)
	}
	if required == modePriority && mode != modePriority {
		t.Skip("set LLMGW_E2E_MODE=priority to run the intentional saturation test")
	}

	baseURL, err := url.Parse(requiredEnv(t, "LLMGW_E2E_GATEWAY_URL"))
	if err != nil || (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		t.Fatalf("LLMGW_E2E_GATEWAY_URL must be an absolute HTTP(S) URL: %v", err)
	}
	if baseURL.RawQuery != "" || baseURL.Fragment != "" || baseURL.User != nil {
		t.Fatal("LLMGW_E2E_GATEWAY_URL must not contain credentials, a query, or a fragment")
	}
	baseURL.Path = strings.TrimSuffix(baseURL.Path, "/")

	cfg := e2eConfig{
		baseURL:            baseURL,
		adminUsername:      requiredEnv(t, "LLMGW_E2E_ADMIN_USERNAME"),
		adminPassword:      requiredEnv(t, "LLMGW_E2E_ADMIN_PASSWORD"),
		model:              requiredEnv(t, "LLMGW_E2E_MODEL"),
		highKey:            requiredEnv(t, "LLMGW_E2E_HIGH_KEY"),
		expectedBackends:   intEnv(t, "LLMGW_E2E_EXPECTED_BACKENDS", 2),
		highRequestsPerKey: intEnv(t, "LLMGW_E2E_HIGH_REQUESTS_PER_KEY", 4),
		highMaxTokens:      intEnv(t, "LLMGW_E2E_HIGH_MAX_TOKENS", 768),
		saturationTimeout:  durationEnv(t, "LLMGW_E2E_SATURATION_TIMEOUT", time.Minute),
		recoveryTimeout:    durationEnv(t, "LLMGW_E2E_RECOVERY_TIMEOUT", 45*time.Second),
		probeTimeout:       durationEnv(t, "LLMGW_E2E_PROBE_TIMEOUT", time.Minute),
		requireAllWaiting:  boolEnv(t, "LLMGW_E2E_REQUIRE_ALL_WAITING", true),
		drainBackendID:     int64Env(t, "LLMGW_E2E_DRAIN_BACKEND_ID", 0),
	}
	if cfg.expectedBackends < 1 || cfg.highRequestsPerKey < 1 || cfg.highMaxTokens < 1 {
		t.Fatal("expected backends, requests per key, and max tokens must all be positive")
	}

	if mode == modePriority {
		cfg.highLoadKeys = listEnv(t, "LLMGW_E2E_HIGH_LOAD_KEYS", 1)
		cfg.criticalKey = requiredEnv(t, "LLMGW_E2E_CRITICAL_KEY")
		cfg.lowKeys = listEnv(t, "LLMGW_E2E_LOW_KEYS", 3)
		keys := []namedClientKey{
			{name: "LLMGW_E2E_HIGH_KEY", value: cfg.highKey},
			{name: "LLMGW_E2E_CRITICAL_KEY", value: cfg.criticalKey},
		}
		for index, key := range cfg.highLoadKeys {
			keys = append(keys, namedClientKey{name: fmt.Sprintf("LLMGW_E2E_HIGH_LOAD_KEYS[%d]", index), value: key})
		}
		for index, key := range cfg.lowKeys {
			keys = append(keys, namedClientKey{name: fmt.Sprintf("LLMGW_E2E_LOW_KEYS[%d]", index), value: key})
		}
		if first, second, duplicate := findDuplicateClientKey(keys); duplicate {
			t.Fatalf("%s and %s must belong to separate clients and use distinct API keys", first, second)
		}
	}
	return cfg
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func listEnv(t *testing.T, name string, minimum int) []string {
	t.Helper()
	parts := strings.Split(requiredEnv(t, name), ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	if len(values) < minimum {
		t.Fatalf("%s has %d value(s), want at least %d", name, len(values), minimum)
	}
	return values
}

type namedClientKey struct {
	name  string
	value string
}

func findDuplicateClientKey(keys []namedClientKey) (string, string, bool) {
	seen := make(map[string]string, len(keys))
	for _, key := range keys {
		if first, exists := seen[key.value]; exists {
			return first, key.name, true
		}
		seen[key.value] = key.name
	}
	return "", "", false
}

func intEnv(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("%s = %q: %v", name, value, err)
	}
	return parsed
}

func int64Env(t *testing.T, name string, fallback int64) int64 {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		t.Fatalf("%s = %q, want a non-negative integer", name, value)
	}
	return parsed
}

func durationEnv(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s = %q, want a positive Go duration", name, value)
	}
	return parsed
}

func boolEnv(t *testing.T, name string, fallback bool) bool {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		t.Fatalf("%s = %q: %v", name, value, err)
	}
	return parsed
}

type readyResponse struct {
	Status              string `json:"status"`
	Revision            int64  `json:"revision"`
	BackendAvailability int    `json:"backendAvailability"`
}

type adminStatus struct {
	Revision int64          `json:"revision"`
	Pools    []adminPool    `json:"pools"`
	Backends []adminBackend `json:"backends"`
}

type adminPool struct {
	ID              int64       `json:"id"`
	PublicModelName string      `json:"publicModelName"`
	Enabled         bool        `json:"enabled"`
	Runtime         poolRuntime `json:"runtime"`
}

type poolRuntime struct {
	State               string  `json:"State"`
	BestBackendPressure float64 `json:"BestBackendPressure"`
	AvailableBackends   int     `json:"AvailableBackends"`
	AllBackendsWaiting  bool    `json:"AllBackendsWaiting"`
}

type adminBackend struct {
	ID          int64          `json:"id"`
	ModelPoolID int64          `json:"modelPoolId"`
	Name        string         `json:"name"`
	Enabled     bool           `json:"enabled"`
	Draining    bool           `json:"draining"`
	Runtime     backendRuntime `json:"runtime"`
}

type backendRuntime struct {
	State        string  `json:"State"`
	Healthy      bool    `json:"Healthy"`
	MetricsFresh bool    `json:"MetricsFresh"`
	Running      int     `json:"Running"`
	Waiting      int     `json:"Waiting"`
	KVCacheUsage float64 `json:"KVCacheUsage"`
	Pressure     float64 `json:"Pressure"`
	Inflight     int     `json:"GatewayInflight"`
}

func (status adminStatus) requirePool(t *testing.T, model string) adminPool {
	t.Helper()
	for _, pool := range status.Pools {
		if pool.PublicModelName == model {
			return pool
		}
	}
	t.Fatalf("admin status does not contain pool %q", model)
	return adminPool{}
}

func (status adminStatus) requireBackend(t *testing.T, id int64) adminBackend {
	t.Helper()
	for _, backend := range status.Backends {
		if backend.ID == id {
			return backend
		}
	}
	t.Fatalf("admin status does not contain backend %d", id)
	return adminBackend{}
}

func (status adminStatus) requireHealthyFreshBackends(t *testing.T, poolID int64, minimum int) {
	t.Helper()
	count := 0
	for _, backend := range status.Backends {
		if backend.ModelPoolID == poolID && backend.Enabled && !backend.Draining && backend.Runtime.Healthy && backend.Runtime.MetricsFresh {
			count++
		}
	}
	if count < minimum {
		t.Fatalf("healthy metrics-fresh backends for pool %d = %d, want at least %d; backends=%+v", poolID, count, minimum, status.Backends)
	}
}

type modelsResponse struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

func (models modelsResponse) contains(model string) bool {
	for _, item := range models.Data {
		if item.ID == model {
			return true
		}
	}
	return false
}

type metricsResponse string

func (metrics metricsResponse) containsFamily(family string) bool {
	for _, line := range strings.Split(string(metrics), "\n") {
		if strings.HasPrefix(line, "# HELP "+family+" ") || strings.HasPrefix(line, family+"{") || strings.HasPrefix(line, family+" ") {
			return true
		}
	}
	return false
}

type remoteHarness struct {
	t      *testing.T
	cfg    e2eConfig
	client *http.Client
}

func newRemoteHarness(t *testing.T, cfg e2eConfig) *remoteHarness {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	return &remoteHarness{t: t, cfg: cfg, client: &http.Client{Transport: transport, Jar: jar}}
}

func (h *remoteHarness) endpoint(path string) string {
	base := *h.cfg.baseURL
	base.Path = strings.TrimSuffix(base.Path, "/") + path
	base.RawPath = ""
	return base.String()
}

func (h *remoteHarness) ready() readyResponse {
	h.t.Helper()
	var output readyResponse
	h.getJSON("/readyz", "", false, &output)
	return output
}

func (h *remoteHarness) adminStatus() adminStatus {
	h.t.Helper()
	var output adminStatus
	h.getJSON("/admin/api/status", "", true, &output)
	return output
}

func (h *remoteHarness) models(key string) modelsResponse {
	h.t.Helper()
	var output modelsResponse
	h.getJSON("/v1/models", key, false, &output)
	return output
}

func (h *remoteHarness) metrics() metricsResponse {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.probeTimeout)
	defer cancel()
	metrics, err := h.fetchMetrics(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	return metrics
}

func (h *remoteHarness) fetchMetrics(ctx context.Context) (metricsResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.endpoint("/metrics"), nil)
	if err != nil {
		return "", fmt.Errorf("create GET /metrics: %w", err)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("GET /metrics: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return "", fmt.Errorf("read /metrics: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET /metrics = %d: %s", response.StatusCode, bodyExcerpt(body))
	}
	return metricsResponse(body), nil
}

func (h *remoteHarness) getJSON(path, key string, admin bool, output any) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.probeTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.endpoint(path), nil)
	if err != nil {
		h.t.Fatal(err)
	}
	if key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}
	if admin {
		request.SetBasicAuth(h.cfg.adminUsername, h.cfg.adminPassword)
	}
	response, err := h.client.Do(request)
	if err != nil {
		h.t.Fatalf("GET %s: %v", path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		h.t.Fatalf("read %s: %v", path, err)
	}
	if response.StatusCode != http.StatusOK {
		h.t.Fatalf("GET %s = %d: %s", path, response.StatusCode, bodyExcerpt(body))
	}
	if err := json.Unmarshal(body, output); err != nil {
		h.t.Fatalf("decode %s: %v; body=%s", path, err, bodyExcerpt(body))
	}
}

type completionRequest struct {
	Key          string
	Prompt       string
	MaxTokens    int
	Stream       bool
	IgnoreEOS    bool
	Priority     *int
	ExtraHeaders map[string]string
}

type requestResult struct {
	Status     int
	RetryAfter string
	Body       []byte
	FirstByte  time.Duration
	Duration   time.Duration
	StreamDone bool
}

func (h *remoteHarness) completion(parent context.Context, input completionRequest) requestResult {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(parent, h.cfg.probeTimeout)
	defer cancel()
	payload := map[string]any{
		"model": h.cfg.model, "prompt": input.Prompt, "max_tokens": input.MaxTokens,
		"temperature": 0, "stream": input.Stream,
	}
	if input.IgnoreEOS {
		payload["ignore_eos"] = true
	}
	if input.Priority != nil {
		payload["priority"] = *input.Priority
	}
	body, err := json.Marshal(payload)
	if err != nil {
		h.t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint("/v1/completions"), bytes.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+input.Key)
	request.Header.Set("Content-Type", "application/json")
	for name, value := range input.ExtraHeaders {
		request.Header.Set(name, value)
	}
	started := time.Now()
	response, err := h.client.Do(request)
	if err != nil {
		h.t.Fatalf("POST /v1/completions: %v", err)
	}
	defer response.Body.Close()
	result := requestResult{Status: response.StatusCode, RetryAfter: response.Header.Get("Retry-After")}
	buffer := make([]byte, 32<<10)
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			if result.FirstByte == 0 {
				result.FirstByte = time.Since(started)
			}
			result.Body = append(result.Body, buffer[:n]...)
			if len(result.Body) > 8<<20 {
				h.t.Fatal("completion response exceeded 8 MiB")
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			h.t.Fatalf("read completion response: %v", readErr)
		}
	}
	result.Duration = time.Since(started)
	result.StreamDone = !input.Stream || bytes.Contains(result.Body, []byte("data: [DONE]"))
	return result
}

func (result requestResult) requireStatus(t *testing.T, want int) {
	t.Helper()
	if result.Status != want {
		t.Fatalf("completion status = %d, want %d; body=%s", result.Status, want, bodyExcerpt(result.Body))
	}
}

func (result requestResult) requireCompleteStream(t *testing.T) {
	t.Helper()
	result.requireStatus(t, http.StatusOK)
	if result.FirstByte <= 0 {
		t.Fatal("streaming completion returned no response bytes")
	}
	if !result.StreamDone {
		t.Fatalf("streaming completion ended without [DONE]; body=%s", bodyExcerpt(result.Body))
	}
}

func (result requestResult) requireOverloaded(t *testing.T) {
	t.Helper()
	result.requireStatus(t, http.StatusTooManyRequests)
	if result.RetryAfter == "" {
		t.Fatal("429 response has no Retry-After header")
	}
	retryAfter, err := strconv.Atoi(result.RetryAfter)
	if err != nil || retryAfter <= 0 {
		t.Fatalf("Retry-After = %q, want a positive integer number of seconds", result.RetryAfter)
	}
	var envelope struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(result.Body, &envelope); err != nil {
		t.Fatalf("decode 429 response: %v; body=%s", err, bodyExcerpt(result.Body))
	}
	if envelope.Error.Code != "gateway_overloaded" || envelope.Error.Type != "rate_limit_error" {
		t.Fatalf("429 error = %+v, want gateway_overloaded rate_limit_error", envelope.Error)
	}
}

func (h *remoteHarness) waitForPool(predicate func(adminPool) bool, timeout time.Duration) adminPool {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	var last adminPool
	for time.Now().Before(deadline) {
		last = h.adminStatus().requirePool(h.t, h.cfg.model)
		if predicate(last) {
			return last
		}
		time.Sleep(250 * time.Millisecond)
	}
	h.t.Fatalf("pool %q did not reach expected state within %s; last=%+v", h.cfg.model, timeout, last.Runtime)
	return adminPool{}
}

func (h *remoteHarness) waitForBackend(id int64, predicate func(adminBackend) bool, timeout time.Duration) adminBackend {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	var last adminBackend
	for time.Now().Before(deadline) {
		status := h.adminStatus()
		for _, backend := range status.Backends {
			if backend.ID == id {
				last = backend
				if predicate(backend) {
					return backend
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	h.t.Fatalf("backend %d did not reach expected state within %s; last=%+v", id, timeout, last)
	return adminBackend{}
}

func (h *remoteHarness) drainBackend(id int64) {
	h.adminMutation(fmt.Sprintf("/admin/api/backends/%d/drain", id))
}

func (h *remoteHarness) resumeBackend(id int64) {
	h.adminMutation(fmt.Sprintf("/admin/api/backends/%d/resume", id))
}

func (h *remoteHarness) adminMutation(path string) {
	h.t.Helper()
	_ = h.adminStatus()
	adminURL, err := url.Parse(h.endpoint(path))
	if err != nil {
		h.t.Fatal(err)
	}
	csrf := ""
	for _, cookie := range h.client.Jar.Cookies(adminURL) {
		if cookie.Name == "llmgw_csrf" {
			csrf = cookie.Value
		}
	}
	if csrf == "" {
		h.t.Fatal("admin status did not set llmgw_csrf cookie")
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.probeTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint(path), nil)
	if err != nil {
		h.t.Fatal(err)
	}
	request.SetBasicAuth(h.cfg.adminUsername, h.cfg.adminPassword)
	request.Header.Set("X-CSRF-Token", csrf)
	response, err := h.client.Do(request)
	if err != nil {
		h.t.Fatalf("POST %s: %v", path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		h.t.Fatalf("read POST %s: %v", path, err)
	}
	if response.StatusCode != http.StatusOK {
		h.t.Fatalf("POST %s = %d: %s", path, response.StatusCode, bodyExcerpt(body))
	}
}

type loadResult struct {
	Status int
	Err    error
}

type highLoad struct {
	t       *testing.T
	done    chan struct{}
	mu      sync.Mutex
	results []loadResult
}

func (h *remoteHarness) startHighLoad(ctx context.Context) *highLoad {
	h.t.Helper()
	load := &highLoad{t: h.t, done: make(chan struct{})}
	var group sync.WaitGroup
	for _, key := range h.cfg.highLoadKeys {
		for range h.cfg.highRequestsPerKey {
			group.Add(1)
			go func(key string) {
				defer group.Done()
				result := h.highLoadRequest(ctx, key)
				load.mu.Lock()
				load.results = append(load.results, result)
				load.mu.Unlock()
			}(key)
		}
	}
	go func() {
		group.Wait()
		close(load.done)
	}()
	return load
}

func (h *remoteHarness) highLoadRequest(ctx context.Context, key string) loadResult {
	payload, err := json.Marshal(map[string]any{
		"model": h.cfg.model, "prompt": "High-priority e2e saturation workload.",
		"max_tokens": h.cfg.highMaxTokens, "temperature": 0, "ignore_eos": true, "stream": true,
	})
	if err != nil {
		return loadResult{Err: err}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint("/v1/completions"), bytes.NewReader(payload))
	if err != nil {
		return loadResult{Err: err}
	}
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	response, err := h.client.Do(request)
	if err != nil {
		return loadResult{Err: err}
	}
	defer response.Body.Close()
	_, err = io.Copy(io.Discard, response.Body)
	return loadResult{Status: response.StatusCode, Err: err}
}

func (load *highLoad) wait(timeout time.Duration) {
	load.t.Helper()
	select {
	case <-load.done:
	case <-time.After(timeout):
		load.t.Errorf("high load did not stop within %s", timeout)
	}
}

func (load *highLoad) requireNoHTTPFailures(t *testing.T) {
	t.Helper()
	load.mu.Lock()
	defer load.mu.Unlock()
	for _, result := range load.results {
		if result.Status != 0 && result.Status != http.StatusOK {
			t.Errorf("high-load request returned HTTP %d", result.Status)
		}
		if result.Err != nil && !errors.Is(result.Err, context.Canceled) {
			t.Errorf("high-load request failed at transport/body layer: %v", result.Err)
		}
	}
}

func intPointer(value int) *int { return &value }

func bodyExcerpt(body []byte) string {
	const maximum = 512
	trimmed := strings.TrimSpace(string(body))
	if len(trimmed) > maximum {
		return trimmed[:maximum] + "…"
	}
	return trimmed
}
