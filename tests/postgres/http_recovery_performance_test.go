package postgres_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/loadgen"
	"github.com/rislanov/vllm-priority-gateway/internal/monitor"
	"github.com/rislanov/vllm-priority-gateway/internal/pressure"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
	basestore "github.com/rislanov/vllm-priority-gateway/internal/store"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

const postgresHTTPRawKey = "llmgw_test12aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

var postgresHTTPSecret = []byte(strings.Repeat("p", 32))

type postgresHTTPGateway struct {
	server    *httptest.Server
	client    *http.Client
	registry  *registry.Registry
	manager   *monitor.Manager
	leases    *coordination.LeaseManager
	admission *coordpostgres.AdmissionCoordinator
	circuit   *coordpostgres.CircuitCoordinator
	transport *http.Transport
}

type postgresHTTPGatewayOptions struct {
	CoordinationTimeout time.Duration
	CanceledHandlerDone chan<- struct{}
}

func newPostgresHTTPGateway(
	t *testing.T,
	ctx context.Context,
	store *pgstore.Store,
	options circuitbreaker.Options,
	coordinationReady func() bool,
	emergency *coordination.EmergencyAdmission,
	refreshInterval time.Duration,
	settings ...postgresHTTPGatewayOptions,
) *postgresHTTPGateway {
	t.Helper()
	var configuration postgresHTTPGatewayOptions
	if len(settings) > 0 {
		configuration = settings[0]
	}
	if configuration.CoordinationTimeout <= 0 {
		configuration.CoordinationTimeout = 150 * time.Millisecond
	}
	if refreshInterval <= 0 {
		refreshInterval = 15 * time.Millisecond
	}
	staleAfter := 200 * time.Millisecond
	if refreshInterval > staleAfter/2 {
		staleAfter = 2 * refreshInterval
	}
	registryValue := registry.New(store)
	if err := registryValue.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	admissionCoordinator := coordpostgres.NewAdmissionCoordinator(store, configuration.CoordinationTimeout)
	circuitCoordinator, err := coordpostgres.NewCircuitCoordinator(store, configuration.CoordinationTimeout, options)
	if err != nil {
		t.Fatal(err)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	manager := monitor.NewManager(ctx, monitor.Options{
		HTTPClient: client, HealthInterval: 15 * time.Millisecond, HealthTimeout: 100 * time.Millisecond,
		MetricsInterval: refreshInterval, MetricsTimeout: 100 * time.Millisecond, StaleAfter: staleAfter,
		UnhealthyAfter: 3, RecoveryAfter: 2, Circuit: options, CircuitCoordinator: circuitCoordinator,
		AdmissionRuntime: admissionCoordinator, ReplicaID: uuid.New(), ProbeTTL: 2 * time.Second, ProbeRenewInterval: 500 * time.Millisecond,
		Limits: pressure.Limits{QueueSoft: 2, KVSoft: .8, KVHard: .95}, EWMAWindow: 10 * time.Millisecond,
		BusyThreshold: .7, SaturatedThreshold: 1,
		PoolThresholds: pressure.Thresholds{
			Busy: .7, Saturated: 1, Emergency: 1.4, BusyRecovery: .55,
			SaturatedRecovery: .85, EmergencyRecovery: 1.2,
			EnterWindow: 60 * time.Millisecond, RecoveryWindow: 90 * time.Millisecond,
		},
	})
	if err := manager.Reconcile(backendsFromSnapshot(registryValue.Snapshot())); err != nil {
		manager.Shutdown()
		t.Fatal(err)
	}
	leases := coordination.NewLeaseManager(ctx, admissionCoordinator, coordination.LeaseManagerOptions{
		RenewInterval: 250 * time.Millisecond, CompletionBacklog: 256, ShutdownTimeout: 2 * time.Second,
	})
	service := gateway.New(gateway.Dependencies{
		Registry: registryValue, HMACSecret: postgresHTTPSecret, Admission: admissionCoordinator, Leases: leases,
		ReplicaID: uuid.New(), LeaseTTL: 2 * time.Second, Emergency: emergency, CoordinationReady: coordinationReady,
		Runtime: manager, Router: routing.New(.02, routing.FixedSource(0)), Forwarder: proxy.New(client), RetryAfter: 20 * time.Millisecond,
	})
	publicHandler := httpapi.NewPublicHandler(service, 1<<20, nil)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer func() {
			if configuration.CanceledHandlerDone != nil && request.Context().Err() != nil {
				select {
				case configuration.CanceledHandlerDone <- struct{}{}:
				default:
				}
			}
		}()
		publicHandler.ServeHTTP(writer, request)
	}))
	gatewayValue := &postgresHTTPGateway{
		server: server, client: client, registry: registryValue, manager: manager, leases: leases,
		admission: admissionCoordinator, circuit: circuitCoordinator, transport: transport,
	}
	t.Cleanup(gatewayValue.close)
	return gatewayValue
}

func (g *postgresHTTPGateway) close() {
	g.server.Close()
	g.manager.Shutdown()
	_ = g.leases.Close()
	g.transport.CloseIdleConnections()
}

func (g *postgresHTTPGateway) post(ctx context.Context, stream bool) (*http.Response, error) {
	body := fmt.Sprintf(`{"model":"test-model","prompt":"acceptance","stream":%t}`, stream)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, g.server.URL+"/v1/completions", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+postgresHTTPRawKey)
	request.Header.Set("Content-Type", "application/json")
	return g.client.Do(request)
}

func preparePostgresHTTPFixture(t *testing.T, store *pgstore.Store, upstreamURL string, concurrency int) fixture {
	t.Helper()
	value := updateFixtureLimits(t, store, createFixture(t, store, 0), concurrency, concurrency)
	updated, err := store.UpdateBackend(context.Background(), value.backend.ID, basestore.UpdateBackendParams{
		ModelPoolID: value.pool.ID, Name: value.backend.Name, BaseURL: upstreamURL, Enabled: true,
		CapacityHint: 1, RunningSoftLimit: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	value.backend = updated
	digest := apikey.Digest(postgresHTTPSecret, postgresHTTPRawKey)
	if _, err := store.ConfigPool().Exec(context.Background(), "UPDATE api_keys SET secret_hash=$2::bytea WHERE id=$1::bigint", value.key.ID, digest[:]); err != nil {
		t.Fatal(err)
	}
	return refreshFixture(t, store, value)
}

func backendsFromSnapshot(snapshot *registry.Snapshot) []domain.Backend {
	values := make([]domain.Backend, 0, len(snapshot.BackendsByID))
	for _, backend := range snapshot.BackendsByID {
		values = append(values, backend)
	}
	return values
}

func waitPostgresGatewayBackend(t *testing.T, value *postgresHTTPGateway, backendID int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		snapshot := value.manager.Snapshot(backendID, time.Now())
		backend := value.registry.Snapshot().BackendsByID[backendID]
		pool := value.manager.PoolSnapshot(backend.ModelPoolID, time.Now())
		// Backend polls and the pool observer run independently. HTTP admission
		// needs the observer's eligible pool snapshot as well as a fresh backend.
		if snapshot.Healthy && snapshot.MetricsFresh && snapshot.CircuitAvailable && pool.State != domain.PoolUnavailable {
			return
		}
		if snapshot.Healthy && snapshot.MetricsFresh && snapshot.CircuitAvailable && pool.State == domain.PoolUnavailable {
			// Outage fixtures deliberately stop periodic pool observation for an
			// hour. Publish the initial healthy topology before injecting faults.
			if err := value.manager.Reconcile(backendsFromSnapshot(value.registry.Snapshot())); err != nil {
				t.Fatal(err)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway did not become eligible: backend=%+v pool=%+v", snapshot, pool)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTwoPostgresHTTPGatewaysShareSSELeaseAndReleaseItOnDisconnect(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{TTFT: 10 * time.Millisecond, TokenDelay: 2 * time.Second, Tokens: []string{"one", "two"}})
	upstream := httptest.NewServer(fake.Handler())
	t.Cleanup(upstream.Close)
	fixture := preparePostgresHTTPFixture(t, store, upstream.URL, 1)
	options := circuitbreaker.Options{FailureThreshold: 5, FailureWindow: time.Minute, OpenCooldown: time.Second, HalfOpenMaxProbes: 1}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	first := newPostgresHTTPGateway(t, ctx, store, options, nil, nil, 0)
	second := newPostgresHTTPGateway(t, ctx, store, options, nil, nil, 0)
	waitPostgresGatewayBackend(t, first, fixture.backend.ID)
	waitPostgresGatewayBackend(t, second, fixture.backend.ID)

	streamContext, cancelStream := context.WithCancel(context.Background())
	stream, err := first.post(streamContext, true)
	if err != nil {
		t.Fatal(err)
	}
	if stream.StatusCode != http.StatusOK || !strings.HasPrefix(stream.Header.Get("Content-Type"), "text/event-stream") {
		payload, _ := io.ReadAll(stream.Body)
		stream.Body.Close()
		t.Fatalf("SSE response=%d content-type=%q body=%q", stream.StatusCode, stream.Header.Get("Content-Type"), payload)
	}
	reader := bufio.NewReader(stream.Body)
	frame, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(frame, `"content":"one"`) {
		stream.Body.Close()
		t.Fatalf("first SSE frame=%q err=%v", frame, err)
	}

	competing, err := second.post(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	competingBody, _ := io.ReadAll(competing.Body)
	competing.Body.Close()
	if competing.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("competing gateway response=%d body=%s, want distributed 429", competing.StatusCode, competingBody)
	}

	cancelStream()
	stream.Body.Close()
	waitUntil(t, 3*time.Second, func() bool {
		var active int
		err := store.CoordinationPool().QueryRow(context.Background(), "SELECT count(*) FROM request_leases WHERE expires_at>clock_timestamp()").Scan(&active)
		return err == nil && active == 0 && fake.Snapshot().CancelledRequests > 0
	})

	fake.SetState(fakevllm.State{})
	recovered, err := second.post(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := io.ReadAll(recovered.Body)
	recovered.Body.Close()
	if recovered.StatusCode != http.StatusOK {
		t.Fatalf("post-disconnect response=%d body=%s", recovered.StatusCode, payload)
	}
}

func TestPostgresGatewayPerformanceSmoke(t *testing.T) {
	runPostgresGatewayPerformance(t, 50, false, "unqualified developer host")
}

func TestPostgresGatewayPerformanceAcceptance(t *testing.T) {
	if os.Getenv("LLMGW_RUN_POSTGRES_PERF") != "1" {
		t.Skip("LLMGW_RUN_POSTGRES_PERF is not set for the documented reference environment")
	}
	referenceProfile := strings.TrimSpace(os.Getenv("LLMGW_POSTGRES_PERF_REFERENCE"))
	if referenceProfile == "" {
		t.Fatal("LLMGW_POSTGRES_PERF_REFERENCE must describe the CPU/host and database topology")
	}
	runPostgresGatewayPerformance(t, 500, true, referenceProfile)
}

func runPostgresGatewayPerformance(t *testing.T, requestCount int, enforceBudget bool, referenceProfile string) {
	t.Helper()
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fake := fakevllm.New()
	upstream := httptest.NewServer(fake.Handler())
	t.Cleanup(upstream.Close)
	fixture := preparePostgresHTTPFixture(t, store, upstream.URL, 128)
	options := circuitbreaker.Options{FailureThreshold: 5, FailureWindow: time.Minute, OpenCooldown: time.Second, HalfOpenMaxProbes: 1}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	first := newPostgresHTTPGateway(t, ctx, store, options, nil, nil, 0)
	second := newPostgresHTTPGateway(t, ctx, store, options, nil, nil, 0)
	waitPostgresGatewayBackend(t, first, fixture.backend.ID)
	waitPostgresGatewayBackend(t, second, fixture.backend.ID)

	gatewayConfig := loadgen.Config{
		URL: first.server.URL, Key: postgresHTTPRawKey, Model: fixture.pool.PublicModelName,
		Requests: requestCount, Parallelism: 1, PromptSize: 32, MaxTokens: 1, Seed: 42, HTTPClient: first.client,
	}
	directConfig := gatewayConfig
	directConfig.URL = upstream.URL
	directConfig.Key = "direct-fake"
	directConfig.Model = fixture.pool.UpstreamModelName
	warmGateway, warmDirect := gatewayConfig, directConfig
	warmGateway.Requests, warmDirect.Requests = 20, 20
	if _, err := loadgen.Run(context.Background(), warmGateway); err != nil {
		t.Fatal(err)
	}
	if _, err := loadgen.Run(context.Background(), warmDirect); err != nil {
		t.Fatal(err)
	}
	direct, err := loadgen.Run(context.Background(), directConfig)
	if err != nil {
		t.Fatal(err)
	}
	result, err := loadgen.Run(context.Background(), gatewayConfig)
	if err != nil {
		t.Fatal(err)
	}
	if result.Successes != requestCount || result.Overloaded != 0 || result.ServerErrors != 0 || result.Failures != 0 {
		t.Fatalf("PostgreSQL load result=%+v", result)
	}
	p50 := max(result.Latency.P50-direct.Latency.P50, 0)
	p99 := max(result.Latency.P99-direct.Latency.P99, 0)
	var postgresVersion string
	if err := store.ConfigPool().QueryRow(context.Background(), "SHOW server_version").Scan(&postgresVersion); err != nil {
		t.Fatal(err)
	}
	t.Logf("environment profile=%q os=%s arch=%s go=%s postgres=%s topology=two-in-process-gateways/one-postgres/one-fake-vllm requests=%d parallelism=%d", referenceProfile, runtime.GOOS, runtime.GOARCH, runtime.Version(), postgresVersion, requestCount, gatewayConfig.Parallelism)
	t.Logf("PostgreSQL gateway-added latency p50=%s p99=%s (gateway=%s/%s direct=%s/%s)", p50, p99, result.Latency.P50, result.Latency.P99, direct.Latency.P50, direct.Latency.P99)
	if enforceBudget && (p50 >= 5*time.Millisecond || p99 >= 20*time.Millisecond) {
		t.Fatalf("opt-in PostgreSQL gateway-added latency budget exceeded: p50=%s p99=%s", p50, p99)
	}
}

func TestPostgresGatewayOutageEmergencyReplayAndRecoveryBarriers(t *testing.T) {
	directDSN := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if directDSN == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	faultProxy, runtimeDSN := newPostgresFaultProxy(t, directDSN)
	store := openTestStoreWithMigration(t, runtimeDSN, directDSN)
	verifyPool, err := pgxpool.New(context.Background(), directDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(verifyPool.Close)

	fake := fakevllm.New()
	upstream := httptest.NewServer(fake.Handler())
	t.Cleanup(upstream.Close)
	fixture := preparePostgresHTTPFixture(t, store, upstream.URL, 8)
	options := circuitbreaker.Options{FailureThreshold: 5, FailureWindow: time.Minute, OpenCooldown: time.Second, HalfOpenMaxProbes: 1}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var recovery *coordination.RecoveryManager
	var replica *coordpostgres.ReplicaManager
	var gatewayValue *postgresHTTPGateway
	ready := func() bool {
		return recovery != nil && recovery.Status().State == coordination.RecoveryReady &&
			replica != nil && replica.Status().Available && replica.Compatible() && gatewayValue != nil &&
			gatewayValue.admission.Status().Available && gatewayValue.circuit.Status().Available
	}
	gatewayValue = newPostgresHTTPGateway(t, ctx, store, options, ready, coordination.NewEmergencyAdmission(1, 1), time.Hour)
	waitPostgresGatewayBackend(t, gatewayValue, fixture.backend.ID)
	replica = coordpostgres.NewReplicaManager(store, uuid.New(), coordpostgres.ReplicaPolicy{
		LeaseTTL: 2 * time.Second, LeaseRenewInterval: 500 * time.Millisecond, Circuit: options,
	}, 150*time.Millisecond)
	if err := replica.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(replica.Close)

	var barrierOrder []string
	assertOutageBarriers := false
	barrier := func(name string, step func(context.Context) error) func(context.Context) error {
		return func(stepContext context.Context) error {
			barrierOrder = append(barrierOrder, name)
			return step(stepContext)
		}
	}
	recovery = coordination.NewRecoveryManager(coordination.RecoverySteps{
		Ping:             barrier("ping", func(stepContext context.Context) error { return store.ConfigPool().Ping(stepContext) }),
		CheckFingerprint: barrier("fingerprint", replica.Check),
		ReloadConfiguration: barrier("configuration", func(stepContext context.Context) error {
			if err := gatewayValue.registry.Reload(stepContext); err != nil {
				return err
			}
			return gatewayValue.manager.Reconcile(backendsFromSnapshot(gatewayValue.registry.Snapshot()))
		}),
		ReconcileLeases: func(stepContext context.Context) error {
			barrierOrder = append(barrierOrder, "leases")
			if assertOutageBarriers && gatewayValue.admission.Status().Available {
				t.Fatal("admission recovered before the lease reconciliation barrier")
			}
			if err := gatewayValue.leases.Reconcile(stepContext); err != nil {
				return err
			}
			err := gatewayValue.admission.RefreshInflight(stepContext)
			if assertOutageBarriers && (err != nil || !gatewayValue.admission.Status().Available) {
				t.Fatalf("lease barrier did not restore admission availability: status=%+v err=%v", gatewayValue.admission.Status(), err)
			}
			return err
		},
		ReplayFailures: func(stepContext context.Context) error {
			barrierOrder = append(barrierOrder, "failures")
			if assertOutageBarriers {
				if backlog := gatewayValue.manager.CircuitReplayBacklog(); backlog != 1 {
					t.Fatalf("failure barrier entered with backlog=%d, want 1", backlog)
				}
				if receipts := databaseTableCount(t, verifyPool, "backend_circuit_failure_receipts"); receipts != 0 {
					t.Fatalf("failure receipt existed before replay barrier: count=%d", receipts)
				}
			}
			err := gatewayValue.manager.ReplayCircuitFailures(stepContext)
			if assertOutageBarriers && (err != nil || gatewayValue.manager.CircuitReplayBacklog() != 0 || databaseTableCount(t, verifyPool, "backend_circuit_failure_receipts") != 1) {
				t.Fatalf("failure barrier side effects incomplete: backlog=%d receipts=%d err=%v", gatewayValue.manager.CircuitReplayBacklog(), databaseTableCount(t, verifyPool, "backend_circuit_failure_receipts"), err)
			}
			return err
		},
		RefreshCircuits: func(stepContext context.Context) error {
			barrierOrder = append(barrierOrder, "circuits")
			if assertOutageBarriers && recovery.Status().State != coordination.RecoveryDegraded {
				t.Fatalf("readiness left degraded state before final barrier: %+v", recovery.Status())
			}
			err := gatewayValue.circuit.Refresh(stepContext)
			if assertOutageBarriers && recovery.Status().State != coordination.RecoveryDegraded {
				t.Fatalf("readiness became ready inside final barrier: %+v", recovery.Status())
			}
			return err
		},
	}, time.Now)
	recovery.MarkDegraded(nil)
	if err := recovery.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	barrierOrder = nil

	initial, err := gatewayValue.post(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	initialBody, _ := io.ReadAll(initial.Body)
	initial.Body.Close()
	if initial.StatusCode != http.StatusOK {
		t.Fatalf("initial distributed request=%d body=%s", initial.StatusCode, initialBody)
	}
	waitForDatabaseCount(t, verifyPool, "request_leases", 0)
	beforeOutage := databaseTableCount(t, verifyPool, "admission_operations")
	requestsBeforeOutage := len(fake.Snapshot().Requests)

	fake.SetState(fakevllm.State{HTTPStatus: http.StatusInternalServerError})
	faultProxy.SetAvailable(false)
	outage, err := gatewayValue.post(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, outage.Body)
	outage.Body.Close()
	if outage.StatusCode < 500 {
		t.Fatalf("outage request status=%d, want upstream failure after emergency admission", outage.StatusCode)
	}
	if databaseTableCount(t, verifyPool, "admission_operations") != beforeOutage {
		t.Fatal("outage request unexpectedly created a distributed admission operation")
	}
	if requests := len(fake.Snapshot().Requests); requests != requestsBeforeOutage+1 {
		t.Fatalf("outage request did not traverse emergency admission to upstream: requests=%d want=%d", requests, requestsBeforeOutage+1)
	}
	waitUntil(t, time.Second, func() bool { return gatewayValue.manager.CircuitReplayBacklog() == 1 })
	if gatewayValue.admission.Status().Available {
		t.Fatalf("admission status stayed available during outage: %+v", gatewayValue.admission.Status())
	}

	faultProxy.SetAvailable(true)
	recovery.MarkDegraded(fmt.Errorf("test PostgreSQL outage"))
	assertOutageBarriers = true
	recoveryDeadline := time.Now().Add(3 * time.Second)
	for {
		barrierOrder = nil
		err := recovery.Recover(context.Background())
		if err == nil {
			break
		}
		if time.Now().After(recoveryDeadline) {
			t.Fatalf("recover barriers: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	wantOrder := []string{"ping", "fingerprint", "configuration", "leases", "failures", "circuits"}
	if !reflect.DeepEqual(barrierOrder, wantOrder) {
		t.Fatalf("recovery barrier order=%v, want %v", barrierOrder, wantOrder)
	}
	if recovery.Status().State != coordination.RecoveryReady || !gatewayValue.admission.Status().Available || !gatewayValue.circuit.Status().Available || !replica.Status().Available {
		t.Fatalf("system not ready after recovery: recovery=%+v admission=%+v circuit=%+v replica=%+v", recovery.Status(), gatewayValue.admission.Status(), gatewayValue.circuit.Status(), replica.Status())
	}
	if gatewayValue.manager.CircuitReplayBacklog() != 0 {
		t.Fatalf("failure replay backlog=%d, want 0", gatewayValue.manager.CircuitReplayBacklog())
	}
	var replayedFailures int
	if err := verifyPool.QueryRow(context.Background(), "SELECT count(*) FROM backend_circuit_failure_receipts").Scan(&replayedFailures); err != nil {
		t.Fatal(err)
	}
	if replayedFailures == 0 {
		t.Fatal("recovery did not durably replay the circuit failure")
	}

	fake.SetState(fakevllm.State{})
	recovered, err := gatewayValue.post(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := io.ReadAll(recovered.Body)
	recovered.Body.Close()
	if recovered.StatusCode != http.StatusOK {
		t.Fatalf("recovered request=%d body=%s", recovered.StatusCode, payload)
	}
	waitUntil(t, 2*time.Second, func() bool {
		return databaseTableCount(t, verifyPool, "admission_operations") == beforeOutage+1
	})
}

func databaseTableCount(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func waitForDatabaseCount(t *testing.T, pool *pgxpool.Pool, table string, want int) {
	t.Helper()
	waitUntil(t, 3*time.Second, func() bool { return databaseTableCount(t, pool, table) == want })
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
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

type postgresFaultProxy struct {
	listener net.Listener
	target   string

	mu        sync.Mutex
	available bool
	closed    bool
	conns     map[net.Conn]struct{}
}

func newPostgresFaultProxy(t *testing.T, dsn string) (*postgresFaultProxy, string) {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Hostname() == "" {
		t.Fatalf("parse PostgreSQL test DSN for fault proxy: %v", err)
	}
	port := parsed.Port()
	if port == "" {
		port = "5432"
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	value := &postgresFaultProxy{
		listener: listener, target: net.JoinHostPort(parsed.Hostname(), port), available: true, conns: make(map[net.Conn]struct{}),
	}
	go value.serve()
	t.Cleanup(value.Close)
	proxyURL := *parsed
	proxyURL.Host = listener.Addr().String()
	query := proxyURL.Query()
	query.Set("connect_timeout", "1")
	proxyURL.RawQuery = query.Encode()
	return value, proxyURL.String()
}

func (p *postgresFaultProxy) serve() {
	for {
		downstream, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		available := p.available && !p.closed
		p.mu.Unlock()
		if !available {
			_ = downstream.Close()
			continue
		}
		upstream, err := net.DialTimeout("tcp", p.target, time.Second)
		if err != nil {
			_ = downstream.Close()
			continue
		}
		p.mu.Lock()
		if !p.available || p.closed {
			p.mu.Unlock()
			_ = downstream.Close()
			_ = upstream.Close()
			continue
		}
		p.conns[downstream] = struct{}{}
		p.conns[upstream] = struct{}{}
		p.mu.Unlock()
		go p.pipe(downstream, upstream)
		go p.pipe(upstream, downstream)
	}
}

func (p *postgresFaultProxy) pipe(destination, source net.Conn) {
	_, _ = io.Copy(destination, source)
	_ = destination.Close()
	_ = source.Close()
	p.mu.Lock()
	delete(p.conns, destination)
	delete(p.conns, source)
	p.mu.Unlock()
}

func (p *postgresFaultProxy) SetAvailable(available bool) {
	p.mu.Lock()
	p.available = available
	connections := make([]net.Conn, 0, len(p.conns))
	if !available {
		for connection := range p.conns {
			connections = append(connections, connection)
		}
	}
	p.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func (p *postgresFaultProxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.available = false
	connections := make([]net.Conn, 0, len(p.conns))
	for connection := range p.conns {
		connections = append(connections, connection)
	}
	p.mu.Unlock()
	_ = p.listener.Close()
	for _, connection := range connections {
		_ = connection.Close()
	}
}
