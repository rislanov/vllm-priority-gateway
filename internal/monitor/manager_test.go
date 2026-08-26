package monitor_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"github.com/rislanov/vllm-priority-gateway/internal/monitor"
)

func TestManagerReconcileStartsKeepsAndStopsWorkers(t *testing.T) {
	fake := fakevllm.New()
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := monitor.NewManager(ctx, monitorOptions(server.Client()))
	defer manager.Shutdown()
	first := testBackend(server.URL, 1, 1)
	second := testBackend(server.URL, 2, 1)
	third := testBackend(server.URL, 3, 2)
	if err := manager.Reconcile([]domain.Backend{first, second}); err != nil {
		t.Fatal(err)
	}
	secondGeneration := manager.WorkerGeneration(2)
	if manager.WorkerCount() != 2 || secondGeneration == 0 {
		t.Fatalf("workers = %d, second generation = %d", manager.WorkerCount(), secondGeneration)
	}
	if err := manager.Reconcile([]domain.Backend{second, third}); err != nil {
		t.Fatal(err)
	}
	if manager.WorkerCount() != 2 || manager.HasWorker(1) || !manager.HasWorker(3) {
		t.Fatalf("unexpected reconciled workers")
	}
	if manager.WorkerGeneration(2) != secondGeneration {
		t.Fatal("unchanged backend worker was restarted")
	}
	if manager.WorkerGeneration(3) <= secondGeneration {
		t.Fatal("new backend did not receive a new generation")
	}
}

func TestManagerInflightReleaseIsIdempotent(t *testing.T) {
	fake := fakevllm.New()
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := monitor.NewManager(ctx, monitorOptions(server.Client()))
	defer manager.Shutdown()
	if err := manager.Reconcile([]domain.Backend{testBackend(server.URL, 1, 1)}); err != nil {
		t.Fatal(err)
	}
	release, ok := manager.IncrementInflight(1)
	if !ok {
		t.Fatal("IncrementInflight() rejected known backend")
	}
	if got := manager.Snapshot(1, time.Now()).GatewayInflight; got != 1 {
		t.Fatalf("inflight = %d", got)
	}
	release()
	release()
	if got := manager.Snapshot(1, time.Now()).GatewayInflight; got != 0 {
		t.Fatalf("inflight after release = %d", got)
	}
}
