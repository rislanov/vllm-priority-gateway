package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

func TestProjectedKeyUsageUpdatesRegistryAfterDurableWrite(t *testing.T) {
	destination := &keyUsageStoreStub{}
	projection := &keyUsageRegistryStub{}
	usedAt := time.Unix(1_700_000_000, 0).UTC()
	writer := projectedKeyUsageStore{destination: destination, projection: projection}
	if err := writer.TouchKeyLastUsed(context.Background(), 7, usedAt); err != nil {
		t.Fatal(err)
	}
	if destination.keyID != 7 || !destination.usedAt.Equal(usedAt) || projection.keyID != 7 || !projection.usedAt.Equal(usedAt) {
		t.Fatalf("destination=%+v projection=%+v", destination, projection)
	}
}

func TestRunServesHealthAndShutsDownGracefully(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	environment := validEnvironment(filepath.Join(t.TempDir(), "gateway.db"))
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	client := &http.Client{Timeout: 200 * time.Millisecond}
	url := "http://" + listener.Addr().String() + "/healthz"
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, requestErr := client.Get(url)
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("gateway did not become healthy")
		}
		time.Sleep(10 * time.Millisecond)
	}
	readyResponse, err := client.Get("http://" + listener.Addr().String() + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	var readiness struct {
		Status              string `json:"status"`
		Revision            int64  `json:"revision"`
		BackendAvailability int    `json:"backendAvailability"`
	}
	if err := json.NewDecoder(readyResponse.Body).Decode(&readiness); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, readyResponse.Body)
	readyResponse.Body.Close()
	if readyResponse.StatusCode != http.StatusOK || readiness.Status != "ready" || readiness.Revision != 0 || readiness.BackendAvailability != 0 {
		t.Fatalf("readiness = %d %+v", readyResponse.StatusCode, readiness)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("gateway did not shut down within deadline")
	}
}

func TestRunLetsActiveStreamFinishInsideGracePeriod(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{Tokens: []string{"one", "two"}, TokenDelay: 100 * time.Millisecond})
	upstream := httptest.NewServer(fake.Handler())
	defer upstream.Close()
	databasePath := filepath.Join(t.TempDir(), "gateway.db")
	secret := strings.Repeat("h", 32)
	clientKey := seedGatewayDatabase(t, databasePath, upstream.URL, []byte(secret))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	environment := validEnvironment(databasePath)
	environment["LLMGW_HEALTH_INTERVAL"] = "10ms"
	environment["LLMGW_METRICS_INTERVAL"] = "10ms"
	environment["LLMGW_UNHEALTHY_AFTER"] = "1"
	environment["LLMGW_RECOVERY_AFTER"] = "1"
	environment["LLMGW_SHUTDOWN_GRACE_PERIOD"] = "1s"
	done := make(chan error, 1)
	go func() { done <- run(ctx, mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{}) }()

	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}
	var response *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for {
		request, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/completions", strings.NewReader(`{"model":"qwen","stream":true}`))
		request.Header.Set("Authorization", "Bearer "+clientKey)
		response, err = client.Do(request)
		if err == nil && response.StatusCode == http.StatusOK {
			break
		}
		if response != nil {
			response.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(first, "one") {
		t.Fatalf("first stream frame = %q err=%v", first, err)
	}
	cancel()
	rest, err := io.ReadAll(reader)
	response.Body.Close()
	if err != nil || !strings.Contains(string(rest), "two") || !strings.Contains(string(rest), "[DONE]") {
		t.Fatalf("stream remainder = %q err=%v", rest, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gateway did not finish after active stream")
	}
	if snapshot := fake.Snapshot(); snapshot.ActiveRequests != 0 {
		t.Fatalf("upstream requests still active: %+v", snapshot)
	}
	database, err := store.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("database remained locked after shutdown: %v", err)
	}
	_ = database.Close()
}

func TestRunForceClosesActiveStreamAfterGracePeriod(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{Tokens: []string{"one", "two", "three"}, TokenDelay: 500 * time.Millisecond})
	upstream := httptest.NewServer(fake.Handler())
	defer upstream.Close()
	databasePath := filepath.Join(t.TempDir(), "gateway.db")
	secret := strings.Repeat("h", 32)
	clientKey := seedGatewayDatabase(t, databasePath, upstream.URL, []byte(secret))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	environment := validEnvironment(databasePath)
	environment["LLMGW_HEALTH_INTERVAL"] = "10ms"
	environment["LLMGW_METRICS_INTERVAL"] = "10ms"
	environment["LLMGW_UNHEALTHY_AFTER"] = "1"
	environment["LLMGW_RECOVERY_AFTER"] = "1"
	environment["LLMGW_SHUTDOWN_GRACE_PERIOD"] = "30ms"
	done := make(chan error, 1)
	go func() { done <- run(ctx, mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{}) }()

	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}
	var response *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for {
		request, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/completions", strings.NewReader(`{"model":"qwen","stream":true}`))
		request.Header.Set("Authorization", "Bearer "+clientKey)
		response, err = client.Do(request)
		if err == nil && response.StatusCode == http.StatusOK {
			break
		}
		if response != nil {
			response.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(first, "one") {
		t.Fatalf("first stream frame = %q err=%v", first, err)
	}
	cancel()
	_, _ = io.ReadAll(reader)
	response.Body.Close()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "graceful HTTP shutdown") {
			t.Fatalf("run error = %v, want forced shutdown error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gateway did not force-close after grace period")
	}
	deadline = time.Now().Add(time.Second)
	for fake.Snapshot().ActiveRequests != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if snapshot := fake.Snapshot(); snapshot.ActiveRequests != 0 {
		t.Fatalf("forced shutdown left upstream request active: %+v", snapshot)
	}
	database, err := store.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("database remained locked after forced shutdown: %v", err)
	}
	_ = database.Close()
}

func TestRunRejectsMissingHMACSecretBeforeServing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	environment := validEnvironment(filepath.Join(t.TempDir(), "gateway.db"))
	delete(environment, "LLMGW_API_KEY_HMAC_SECRET")
	err = run(context.Background(), mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "LLMGW_API_KEY_HMAC_SECRET") {
		t.Fatalf("run error = %v", err)
	}
}

func validEnvironment(databasePath string) map[string]string {
	return map[string]string{
		"LLMGW_LISTEN_ADDRESS":        "127.0.0.1:0",
		"LLMGW_DATABASE_PATH":         databasePath,
		"LLMGW_ADMIN_USERNAME":        "operator",
		"LLMGW_ADMIN_PASSWORD":        "correct horse battery staple",
		"LLMGW_API_KEY_HMAC_SECRET":   strings.Repeat("h", 32),
		"LLMGW_SHUTDOWN_GRACE_PERIOD": "500ms",
	}
}

func seedGatewayDatabase(t *testing.T, path, upstreamURL string, hmacSecret []byte) string {
	t.Helper()
	database, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := database.CreatePool(context.Background(), store.CreatePoolParams{PublicModelName: "qwen", UpstreamModelName: "fake-model", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateBackend(context.Background(), store.CreateBackendParams{ModelPoolID: pool.ID, Name: "gpu-a", BaseURL: upstreamURL, Enabled: true, CapacityHint: 1, RunningSoftLimit: 16}); err != nil {
		t.Fatal(err)
	}
	client, err := database.CreateClient(context.Background(), store.CreateClientParams{Name: "stream-client", Enabled: true, PriorityClass: domain.PriorityHigh, VLLMPriority: -10, MaxConcurrency: 1, ModelPoolIDs: []int64{pool.ID}})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := apikey.Generate(bytes.NewReader(bytes.Repeat([]byte{7}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateAPIKey(context.Background(), store.CreateAPIKeyParams{ClientID: client.ID, Prefix: plain.Prefix, SecretHash: apikey.Digest(hmacSecret, plain.Value)}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	return plain.Value
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

type keyUsageStoreStub struct {
	keyID  int64
	usedAt time.Time
}

func (s *keyUsageStoreStub) TouchKeyLastUsed(_ context.Context, keyID int64, usedAt time.Time) error {
	s.keyID, s.usedAt = keyID, usedAt
	return nil
}

type keyUsageRegistryStub struct {
	keyID  int64
	usedAt time.Time
}

func (s *keyUsageRegistryStub) MarkKeyUsed(keyID int64, usedAt time.Time) bool {
	s.keyID, s.usedAt = keyID, usedAt
	return true
}
