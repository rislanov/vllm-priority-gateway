package fakevllm_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
)

func TestHealthMetricsAndModels(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{Running: 12, Waiting: 8, KVCacheUsage: .94, Models: []string{"upstream-model"}})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()

	for _, path := range []string{"/health", "/v1/models"} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d", path, response.StatusCode)
		}
		response.Body.Close()
	}
	response, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	for _, want := range []string{
		`vllm:num_requests_running{model_name="upstream-model"} 12`,
		`vllm:num_requests_waiting{model_name="upstream-model"} 8`,
		`vllm:kv_cache_usage_perc{model_name="upstream-model"} 0.94`,
	} {
		if !bytes.Contains(body, []byte(want)) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestLegacyKVMetrics(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{KVCacheUsage: .5, LegacyKVMetrics: true})
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	fake.Handler().ServeHTTP(recorder, request)
	body := recorder.Body.String()
	if !strings.Contains(body, "vllm:gpu_cache_usage_perc") || strings.Contains(body, "vllm:kv_cache_usage_perc") {
		t.Fatalf("legacy metrics = %s", body)
	}
}

func TestOrdinaryCompletionRecordsObservableRequest(t *testing.T) {
	fake := fakevllm.New()
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"upstream","input":"hello"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Vllm-Priority", "-10")
	request.Header.Set("X-Request-Id", "gateway-request")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !json.Valid(body) {
		t.Fatalf("response = %d %s", response.StatusCode, body)
	}
	snapshot := fake.Snapshot()
	if len(snapshot.Requests) != 1 {
		t.Fatalf("requests = %+v", snapshot.Requests)
	}
	record := snapshot.Requests[0]
	if record.Path != "/v1/responses" || record.Model != "upstream" || record.Priority != "-10" || record.RequestID != "gateway-request" {
		t.Fatalf("request record = %+v", record)
	}
}

func TestStreamingFlushesFramesWithDelay(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{TokenDelay: 75 * time.Millisecond, Tokens: []string{"one", "two"}})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"upstream","stream":true}`))
	started := time.Now()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	firstAt := time.Since(started)
	if !strings.HasPrefix(first, "data: ") || !strings.Contains(first, "one") {
		t.Fatalf("first frame = %q", first)
	}
	if firstAt >= 60*time.Millisecond {
		t.Fatalf("first frame took %s; response appears buffered", firstAt)
	}
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if !bytes.Contains(rest, []byte("two")) || !bytes.Contains(rest, []byte("data: [DONE]")) {
		t.Fatalf("stream tail = %s", rest)
	}
}

func TestClientCancellationReachesSimulator(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{TTFT: 10 * time.Second})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/completions", strings.NewReader(`{"model":"upstream"}`))
	result := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if response != nil {
			response.Body.Close()
		}
		result <- err
	}()
	waitFor(t, time.Second, func() bool { return fake.Snapshot().ActiveRequests == 1 })
	cancel()
	if err := <-result; err == nil {
		t.Fatal("client request unexpectedly succeeded")
	}
	waitFor(t, time.Second, func() bool {
		snapshot := fake.Snapshot()
		return snapshot.ActiveRequests == 0 && snapshot.CancelledRequests == 1
	})
}

func TestAdminStateControlsFailures(t *testing.T) {
	fake := fakevllm.New()
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	payload := `{"running":3,"waiting":2,"kvCacheUsage":0.75,"ttftMs":5,"tokenDelayMs":7,"healthFailures":1,"httpStatus":429}`
	request, _ := http.NewRequest(http.MethodPut, server.URL+"/admin/state", strings.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d", response.StatusCode)
	}
	health, err := http.Get(server.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	health.Body.Close()
	if health.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("first health status = %d", health.StatusCode)
	}
	health, _ = http.Get(server.URL + "/health")
	health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("second health status = %d", health.StatusCode)
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
