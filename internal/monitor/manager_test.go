package monitor_test

import (
	"context"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"github.com/rislanov/vllm-priority-gateway/internal/monitor"
)

func TestManagerAcquirePoolEnforcesPositiveLimitAndTracksUnlimited(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := monitor.NewManager(ctx, monitorOptions(nil))
	defer manager.Shutdown()

	releaseFirst, ok := manager.AcquirePool(9, 2)
	if !ok {
		t.Fatal("first limited pool lease was rejected")
	}
	releaseSecond, ok := manager.AcquirePool(9, 2)
	if !ok {
		t.Fatal("second limited pool lease was rejected")
	}
	if release, acquired := manager.AcquirePool(9, 2); acquired || release != nil {
		t.Fatal("third limited pool lease was admitted at limit 2")
	}
	if got := manager.PoolSnapshot(9, time.Now()).GatewayInflight; got != 2 {
		t.Fatalf("limited pool gateway inflight = %d, want 2", got)
	}
	releaseFirst()
	if release, acquired := manager.AcquirePool(9, 2); !acquired || release == nil {
		t.Fatal("pool lease was not admitted after a release")
	} else {
		release()
	}
	releaseSecond()

	unlimited := make([]func(), 0, 3)
	for range 3 {
		release, acquired := manager.AcquirePool(10, 0)
		if !acquired || release == nil {
			t.Fatal("unlimited pool lease was rejected")
		}
		unlimited = append(unlimited, release)
	}
	if got := manager.PoolSnapshot(10, time.Now()).GatewayInflight; got != 3 {
		t.Fatalf("unlimited pool gateway inflight = %d, want 3", got)
	}
	for _, release := range unlimited {
		release()
	}
	if got := manager.PoolSnapshot(10, time.Now()).GatewayInflight; got != 0 {
		t.Fatalf("unlimited pool gateway inflight after release = %d, want 0", got)
	}
}

func TestManagerPoolReleaseIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := monitor.NewManager(ctx, monitorOptions(nil))
	defer manager.Shutdown()

	release, ok := manager.AcquirePool(9, 1)
	if !ok {
		t.Fatal("pool lease was rejected")
	}
	release()
	release()
	if got := manager.PoolSnapshot(9, time.Now()).GatewayInflight; got != 0 {
		t.Fatalf("pool gateway inflight after duplicate release = %d, want 0", got)
	}
}

func TestManagerAcquirePoolIsAtomicUnderConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := monitor.NewManager(ctx, monitorOptions(nil))
	defer manager.Shutdown()

	start := make(chan struct{})
	releaseAll := make(chan struct{})
	var acquired atomic.Int64
	var workers sync.WaitGroup
	for range 64 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			release, ok := manager.AcquirePool(9, 2)
			if !ok {
				return
			}
			acquired.Add(1)
			<-releaseAll
			release()
		}()
	}
	close(start)
	deadline := time.Now().Add(time.Second)
	for acquired.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := acquired.Load(); got != 2 {
		close(releaseAll)
		workers.Wait()
		t.Fatalf("concurrent acquisitions = %d, want exactly 2", got)
	}
	if got := manager.PoolSnapshot(9, time.Now()).GatewayInflight; got != 2 {
		close(releaseAll)
		workers.Wait()
		t.Fatalf("concurrent pool gateway inflight = %d, want 2", got)
	}
	close(releaseAll)
	workers.Wait()
	if got := manager.PoolSnapshot(9, time.Now()).GatewayInflight; got != 0 {
		t.Fatalf("pool gateway inflight after concurrent releases = %d, want 0", got)
	}
}

func TestManagerPoolLeaseSurvivesReconcileRemoval(t *testing.T) {
	fake := fakevllm.New()
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := monitor.NewManager(ctx, monitorOptions(server.Client()))
	defer manager.Shutdown()
	if err := manager.Reconcile([]domain.Backend{testBackend(server.URL, 1, 9)}); err != nil {
		t.Fatal(err)
	}
	release, ok := manager.AcquirePool(9, 1)
	if !ok {
		t.Fatal("pool lease was rejected")
	}
	if err := manager.Reconcile(nil); err != nil {
		t.Fatal(err)
	}
	if got := manager.PoolSnapshot(9, time.Now()).GatewayInflight; got != 1 {
		t.Fatalf("pool gateway inflight after reconcile removal = %d, want 1", got)
	}
	if another, acquired := manager.AcquirePool(9, 1); acquired || another != nil {
		t.Fatal("reconcile removal bypassed the active pool limit")
	}
	release()
	if got := manager.PoolSnapshot(9, time.Now()).GatewayInflight; got != 0 {
		t.Fatalf("pool gateway inflight after removal release = %d, want 0", got)
	}
}

func TestManagerPoolSnapshotAggregatesWaitingAndCircuitAvailability(t *testing.T) {
	fakeA := fakevllm.New()
	fakeA.SetState(fakevllm.State{Waiting: 2})
	serverA := httptest.NewServer(fakeA.Handler())
	defer serverA.Close()
	fakeB := fakevllm.New()
	fakeB.SetState(fakevllm.State{Waiting: 3})
	serverB := httptest.NewServer(fakeB.Handler())
	defer serverB.Close()
	fakeDraining := fakevllm.New()
	fakeDraining.SetState(fakevllm.State{Waiting: 7})
	serverDraining := httptest.NewServer(fakeDraining.Handler())
	defer serverDraining.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := monitorOptions(serverA.Client())
	options.HealthInterval = 5 * time.Millisecond
	options.MetricsInterval = 5 * time.Millisecond
	options.RecoveryAfter = 1
	manager := monitor.NewManager(ctx, options)
	defer manager.Shutdown()
	draining := testBackend(serverDraining.URL, 3, 9)
	draining.Draining = true
	if err := manager.Reconcile([]domain.Backend{
		testBackend(serverA.URL, 1, 9),
		testBackend(serverB.URL, 2, 9),
		draining,
	}); err != nil {
		t.Fatal(err)
	}

	waitPoolRuntime(t, manager, 9, func(runtime domain.PoolRuntime) bool {
		return runtime.AvailableBackends == 2 && runtime.TotalWaiting == 5
	})
	base := time.Now()
	completeBackend(t, manager, 1, base, domain.InferenceFailure)
	completeBackend(t, manager, 1, base.Add(time.Millisecond), domain.InferenceFailure)
	waitPoolRuntime(t, manager, 9, func(runtime domain.PoolRuntime) bool {
		return runtime.AvailableBackends == 1 && runtime.TotalWaiting == 5
	})
}

func waitPoolRuntime(t *testing.T, manager *monitor.Manager, poolID int64, ready func(domain.PoolRuntime) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime := manager.PoolSnapshot(poolID, time.Now())
		if ready(runtime) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pool runtime did not reach expected state: %+v", manager.PoolSnapshot(poolID, time.Now()))
}

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

func TestManagerAcquireBackendTracksInflightAndCircuitOutcome(t *testing.T) {
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
	at := time.Now()
	complete, ok := manager.AcquireBackend(1, at)
	if !ok {
		t.Fatal("AcquireBackend() rejected known backend")
	}
	snapshot := manager.Snapshot(1, at)
	if snapshot.GatewayInflight != 1 || snapshot.CircuitState != domain.CircuitClosed || !snapshot.CircuitAvailable {
		t.Fatalf("snapshot after acquire = %+v", snapshot)
	}
	complete(domain.InferenceFailure)
	snapshot = manager.Snapshot(1, at)
	if snapshot.GatewayInflight != 0 || snapshot.CircuitFailures != 1 || snapshot.CircuitState != domain.CircuitClosed {
		t.Fatalf("snapshot after failure = %+v", snapshot)
	}
}

func TestManagerOpenCircuitRejectsUntilHalfOpenProbe(t *testing.T) {
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

	base := time.Now()
	completeBackend(t, manager, 1, base, domain.InferenceFailure)
	completeBackend(t, manager, 1, base.Add(time.Second), domain.InferenceFailure)
	open := manager.Snapshot(1, base.Add(time.Second))
	if open.CircuitState != domain.CircuitOpen || open.CircuitAvailable || !open.CircuitRetryAt.Equal(base.Add(6*time.Second)) {
		t.Fatalf("open snapshot = %+v", open)
	}
	if complete, ok := manager.AcquireBackend(1, base.Add(5*time.Second)); ok || complete != nil {
		t.Fatal("AcquireBackend() admitted a request before cooldown")
	}

	probe, ok := manager.AcquireBackend(1, base.Add(6*time.Second))
	if !ok || probe == nil {
		t.Fatal("AcquireBackend() rejected the half-open probe")
	}
	halfOpen := manager.Snapshot(1, base.Add(6*time.Second))
	if halfOpen.CircuitState != domain.CircuitHalfOpen || halfOpen.CircuitAvailable || halfOpen.CircuitProbesInFlight != 1 || halfOpen.GatewayInflight != 1 {
		t.Fatalf("half-open snapshot = %+v", halfOpen)
	}
	if complete, ok := manager.AcquireBackend(1, base.Add(6*time.Second)); ok || complete != nil {
		t.Fatal("AcquireBackend() admitted a second half-open probe")
	}
	probe(domain.InferenceSuccess)
	closed := manager.Snapshot(1, base.Add(6*time.Second))
	if closed.CircuitState != domain.CircuitClosed || !closed.CircuitAvailable || closed.CircuitFailures != 0 || closed.GatewayInflight != 0 {
		t.Fatalf("closed snapshot = %+v", closed)
	}

	completeBackend(t, manager, 1, base.Add(7*time.Second), domain.InferenceFailure)
	completeBackend(t, manager, 1, base.Add(8*time.Second), domain.InferenceFailure)
	probe, ok = manager.AcquireBackend(1, base.Add(13*time.Second))
	if !ok || probe == nil {
		t.Fatal("AcquireBackend() rejected the second half-open probe")
	}
	probe(domain.InferenceFailure)
	reopened := manager.Snapshot(1, base.Add(13*time.Second))
	if reopened.CircuitState != domain.CircuitOpen || reopened.CircuitAvailable || !reopened.CircuitRetryAt.Equal(base.Add(18*time.Second)) || reopened.GatewayInflight != 0 {
		t.Fatalf("reopened snapshot = %+v", reopened)
	}
}

func TestManagerBackendCompletionIsIdempotent(t *testing.T) {
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
	at := time.Now()
	complete, ok := manager.AcquireBackend(1, at)
	if !ok {
		t.Fatal("AcquireBackend() rejected known backend")
	}
	complete(domain.InferenceFailure)
	complete(domain.InferenceFailure)
	snapshot := manager.Snapshot(1, at)
	if snapshot.GatewayInflight != 0 || snapshot.CircuitFailures != 1 {
		t.Fatalf("snapshot after duplicate completion = %+v", snapshot)
	}
}

func TestManagerReconcileKeepsOrResetsCircuitWithWorkerIdentity(t *testing.T) {
	fake := fakevllm.New()
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := monitor.NewManager(ctx, monitorOptions(server.Client()))
	defer manager.Shutdown()
	backend := testBackend(server.URL, 1, 1)
	if err := manager.Reconcile([]domain.Backend{backend}); err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	completeBackend(t, manager, backend.ID, base, domain.InferenceFailure)
	completeBackend(t, manager, backend.ID, base.Add(time.Second), domain.InferenceFailure)
	if got := manager.Snapshot(backend.ID, base.Add(time.Second)).CircuitState; got != domain.CircuitOpen {
		t.Fatalf("circuit state = %q, want open", got)
	}

	if err := manager.Reconcile([]domain.Backend{backend}); err != nil {
		t.Fatal(err)
	}
	retained := manager.Snapshot(backend.ID, base.Add(time.Second))
	if retained.CircuitState != domain.CircuitOpen || retained.CircuitFailures != 2 {
		t.Fatalf("retained circuit = %+v", retained)
	}

	backend.Name = "replacement"
	if err := manager.Reconcile([]domain.Backend{backend}); err != nil {
		t.Fatal(err)
	}
	reset := manager.Snapshot(backend.ID, base.Add(time.Second))
	if reset.CircuitState != domain.CircuitClosed || reset.CircuitFailures != 0 || !reset.CircuitAvailable {
		t.Fatalf("reset circuit = %+v", reset)
	}
}

func completeBackend(t *testing.T, manager *monitor.Manager, backendID int64, at time.Time, outcome domain.InferenceOutcome) {
	t.Helper()
	complete, ok := manager.AcquireBackend(backendID, at)
	if !ok {
		t.Fatalf("AcquireBackend(%d, %s) rejected", backendID, at)
	}
	complete(outcome)
}

func TestManagerAdvancesPoolHysteresisWithoutSnapshotReads(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{Running: 32, Waiting: 4, KVCacheUsage: 1})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := monitorOptions(server.Client())
	options.HealthInterval = 5 * time.Millisecond
	options.MetricsInterval = 5 * time.Millisecond
	options.RecoveryAfter = 1
	options.EWMAWindow = time.Millisecond
	options.PoolThresholds.EnterWindow = 30 * time.Millisecond
	manager := monitor.NewManager(ctx, options)
	defer manager.Shutdown()
	if err := manager.Reconcile([]domain.Backend{testBackend(server.URL, 1, 9)}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		runtime := manager.Snapshot(1, time.Now())
		if runtime.Healthy && runtime.MetricsFresh && runtime.Pressure >= 1.4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("backend did not become pressured: %+v", runtime)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(options.PoolThresholds.EnterWindow + 3*options.MetricsInterval)
	if state := manager.PoolSnapshot(9, time.Now()).State; state != domain.PoolEmergency {
		t.Fatalf("first pool read after sustained pressure = %s, want emergency", state)
	}
}
