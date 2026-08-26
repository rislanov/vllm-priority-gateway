package monitor_test

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"github.com/rislanov/vllm-priority-gateway/internal/monitor"
	"github.com/rislanov/vllm-priority-gateway/internal/pressure"
)

func monitorOptions(client *http.Client) monitor.Options {
	return monitor.Options{
		HTTPClient: client, HealthInterval: time.Hour, MetricsInterval: time.Hour,
		HealthTimeout: time.Second, MetricsTimeout: time.Second, StaleAfter: 5 * time.Second,
		UnhealthyAfter: 3, RecoveryAfter: 2,
		Limits:        pressure.Limits{QueueSoft: 2, KVSoft: .8, KVHard: .95, RunningSoft: 8},
		EWMAWindow:    4 * time.Second,
		BusyThreshold: .7, SaturatedThreshold: 1,
		PoolThresholds: pressure.Thresholds{
			Busy: .7, Saturated: 1, Emergency: 1.4,
			BusyRecovery: .55, SaturatedRecovery: .85, EmergencyRecovery: 1.2,
			EnterWindow: 3 * time.Second, RecoveryWindow: 10 * time.Second,
		},
	}
}

func testBackend(baseURL string, id, poolID int64) domain.Backend {
	return domain.Backend{
		ID: id, ModelPoolID: poolID, Name: "backend", BaseURL: baseURL,
		Enabled: true, CapacityHint: 1, RunningSoftLimit: 8,
	}
}

func TestWorkerHealthFailureAndRecoveryCounts(t *testing.T) {
	var failing atomic.Bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" && failing.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	worker, err := monitor.NewWorker(testBackend(server.URL, 1, 1), monitorOptions(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	for range 2 {
		worker.PollHealth(context.Background(), now)
		now = now.Add(time.Second)
	}
	if !worker.Snapshot(now).Healthy {
		t.Fatal("worker did not recover after two successes")
	}
	failing.Store(true)
	for range 2 {
		worker.PollHealth(context.Background(), now)
		now = now.Add(time.Second)
	}
	if !worker.Snapshot(now).Healthy {
		t.Fatal("worker became unhealthy before three failures")
	}
	worker.PollHealth(context.Background(), now)
	if worker.Snapshot(now).Healthy {
		t.Fatal("worker stayed healthy after three failures")
	}
	failing.Store(false)
	worker.PollHealth(context.Background(), now.Add(time.Second))
	if worker.Snapshot(now.Add(time.Second)).Healthy {
		t.Fatal("worker recovered after one success")
	}
	worker.PollHealth(context.Background(), now.Add(2*time.Second))
	if !worker.Snapshot(now.Add(2 * time.Second)).Healthy {
		t.Fatal("worker did not recover after two successes")
	}
}

func TestWorkerMetricsPressureAndStaleness(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{Running: 8, Waiting: 2, KVCacheUsage: .875})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	worker, err := monitor.NewWorker(testBackend(server.URL, 1, 1), monitorOptions(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	worker.PollHealth(context.Background(), now)
	worker.PollHealth(context.Background(), now)
	if err := worker.PollMetrics(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	snapshot := worker.Snapshot(now)
	if !snapshot.MetricsFresh || math.Abs(snapshot.RawPressure-.85) > 1e-9 || math.Abs(snapshot.Pressure-.85) > 1e-9 || snapshot.State != domain.BackendBusy {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	stale := worker.Snapshot(now.Add(6 * time.Second))
	if stale.MetricsFresh || stale.State != domain.BackendUnhealthy {
		t.Fatalf("stale snapshot = %+v", stale)
	}
}

func TestWorkerDoesNotFollowHealthOrMetricsRedirects(t *testing.T) {
	var redirectedRequests atomic.Int64
	sink := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		redirectedRequests.Add(1)
		if request.URL.Path == "/metrics" {
			_, _ = writer.Write([]byte("vllm:num_requests_running 0\nvllm:num_requests_waiting 0\nvllm:gpu_cache_usage_perc 0\n"))
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer sink.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, sink.URL+request.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	worker, err := monitor.NewWorker(testBackend(redirector.URL, 1, 1), monitorOptions(redirector.Client()))
	if err != nil {
		t.Fatal(err)
	}
	if worker.PollHealth(context.Background(), time.Now()) {
		t.Fatal("redirected health response was accepted")
	}
	if err := worker.PollMetrics(context.Background(), time.Now()); err == nil {
		t.Fatal("redirected metrics response was accepted")
	}
	if got := redirectedRequests.Load(); got != 0 {
		t.Fatalf("monitor followed %d redirect(s) outside the backend boundary", got)
	}
}

func TestWorkerDrainingOverridesPressureState(t *testing.T) {
	fake := fakevllm.New()
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	backend := testBackend(server.URL, 1, 1)
	backend.Draining = true
	worker, err := monitor.NewWorker(backend, monitorOptions(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	worker.PollHealth(context.Background(), now)
	worker.PollHealth(context.Background(), now)
	if err := worker.PollMetrics(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if got := worker.Snapshot(now).State; got != domain.BackendDraining {
		t.Fatalf("state = %s", got)
	}
}
