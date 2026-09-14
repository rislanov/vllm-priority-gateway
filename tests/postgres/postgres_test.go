package postgres_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/contracttest"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
	basestore "github.com/rislanov/vllm-priority-gateway/internal/store"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

func openTestStore(t *testing.T, runtimeURL string) *pgstore.Store {
	t.Helper()
	if runtimeURL == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	direct := runtimeURL
	if runtimeURL == os.Getenv("LLMGW_POSTGRES_POOLER_TEST_DSN") {
		direct = os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	}
	if runtimeURL == os.Getenv("LLMGW_POSTGRES_HA_TEST_DSN") && os.Getenv("LLMGW_POSTGRES_HA_MIGRATION_DSN") != "" {
		direct = os.Getenv("LLMGW_POSTGRES_HA_MIGRATION_DSN")
	}
	return openTestStoreWithMigration(t, runtimeURL, direct)
}

func openTestStoreWithMigration(t *testing.T, runtimeURL, migrationURL string) *pgstore.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := pgstore.Open(ctx, pgstore.Options{DatabaseURL: runtimeURL, MigrationURL: migrationURL, ConfigMaxConns: 4, AnalyticsMaxConns: 4, CoordinationMaxConns: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	resetDatabase(t, store)
	return store
}

func resetDatabase(t *testing.T, store *pgstore.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := store.ConfigPool().Exec(ctx, `TRUNCATE TABLE backend_circuit_active_failures,backend_circuit_failures,backend_circuit_failure_receipts,backend_circuit_probes,backend_circuit_state,coordination_rate_state,request_leases,admission_operations,coordination_replicas,client_admission_scopes,pool_admission_scopes,usage_requests,client_model_access,api_keys,backends,clients,model_pools RESTART IDENTITY CASCADE; UPDATE config_meta SET revision=0 WHERE singleton=1`)
	if err != nil {
		t.Fatalf("reset dedicated PostgreSQL test database: %v", err)
	}
}

func TestPostgresConfigurationPreservesConflictAndNotFoundCauses(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	ctx := context.Background()
	params := basestore.CreatePoolParams{PublicModelName: "duplicate-model", UpstreamModelName: "upstream", Enabled: true, MaxGatewayInflight: 1}
	if _, err := store.CreatePool(ctx, params); err != nil {
		t.Fatal(err)
	}
	_, err := store.CreatePool(ctx, params)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("duplicate pool error = %v, want wrapped PostgreSQL unique violation", err)
	}
	_, err = store.UpdateClient(ctx, 999, basestore.UpdateClientParams{
		Name: "missing", Enabled: true, PriorityClass: domain.PriorityHigh, MaxConcurrency: 1,
	})
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("missing client error = %v, want pgx.ErrNoRows", err)
	}
}

func TestPostgresConcurrentMigrationStartup(t *testing.T) {
	runtimeURL := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if runtimeURL == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const replicas = 4
	stores := make(chan *pgstore.Store, replicas)
	errorsSeen := make(chan error, replicas)
	var group sync.WaitGroup
	for index := 0; index < replicas; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			store, err := pgstore.Open(ctx, pgstore.Options{
				DatabaseURL: runtimeURL, MigrationURL: runtimeURL,
				ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 2,
			})
			if err != nil {
				errorsSeen <- err
				return
			}
			stores <- store
		}()
	}
	group.Wait()
	close(stores)
	close(errorsSeen)
	for store := range stores {
		_ = store.Close()
	}
	for err := range errorsSeen {
		t.Errorf("concurrent migration startup: %v", err)
	}
}

func TestPostgresDirtyMigrationRefusesStartup(t *testing.T) {
	dsn := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	store := openTestStore(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := store.ConfigPool().Exec(ctx, "UPDATE schema_migrations SET dirty=TRUE"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.ConfigPool().Exec(cleanupCtx, "UPDATE schema_migrations SET dirty=FALSE")
	})
	opened, err := pgstore.Open(ctx, pgstore.Options{
		DatabaseURL: dsn, MigrationURL: dsn,
		ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 2,
	})
	if opened != nil {
		_ = opened.Close()
	}
	if err == nil {
		t.Fatal("dirty migration state allowed startup")
	}
}

func TestPostgresRejectsPre16Server(t *testing.T) {
	dsn := os.Getenv("LLMGW_POSTGRES_PRE16_TEST_DSN")
	if dsn == "" {
		t.Skip("LLMGW_POSTGRES_PRE16_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := pgstore.Open(ctx, pgstore.Options{
		DatabaseURL: dsn, MigrationURL: dsn,
		ConfigMaxConns: 1, AnalyticsMaxConns: 1, CoordinationMaxConns: 1,
	})
	if store != nil {
		_ = store.Close()
	}
	if err == nil {
		t.Fatal("PostgreSQL server older than 16 was accepted")
	}
}

type fixture struct {
	client   domain.Client
	pool     domain.ModelPool
	backend  domain.Backend
	key      domain.APIKey
	revision int64
}

func createFixture(t *testing.T, store *pgstore.Store, rpm int64) fixture {
	return createRateFixture(t, store, rpm, 0)
}

func createRateFixture(t *testing.T, store *pgstore.Store, rpm, tpm int64) fixture {
	t.Helper()
	ctx := context.Background()
	pool, err := store.CreatePool(ctx, basestore.CreatePoolParams{PublicModelName: "test-model", UpstreamModelName: "upstream", Enabled: true, MaxGatewayInflight: 1})
	if err != nil {
		t.Fatal(err)
	}
	client, err := store.CreateClient(ctx, basestore.CreateClientParams{Name: "test-client", Enabled: true, PriorityClass: domain.PriorityHigh, MaxConcurrency: 1, RequestsPerMinute: rpm, TokensPerMinute: tpm, ModelPoolIDs: []int64{pool.ID}})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("test-key"))
	key, err := store.CreateAPIKey(ctx, basestore.CreateAPIKeyParams{ClientID: client.ID, Prefix: "llmgw_test12", SecretHash: hash})
	if err != nil {
		t.Fatal(err)
	}
	backend, err := store.CreateBackend(ctx, basestore.CreateBackendParams{ModelPoolID: pool.ID, Name: "gpu", BaseURL: "http://127.0.0.1:1", Enabled: true, CapacityHint: 1, RunningSoftLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return fixture{client: client, pool: pool, backend: backend, key: key, revision: snapshot.Revision}
}

func admissionRequest(f fixture, replica uuid.UUID) coordination.AdmissionRequest {
	return coordination.AdmissionRequest{LeaseID: uuid.New(), OperationStartedAt: time.Now().UTC(), RequestID: uuid.NewString(), ReplicaID: replica, ConfigurationRevision: f.revision, APIKeyID: f.key.ID, ClientID: f.client.ID, PoolID: f.pool.ID, ClientPolicyRevision: f.client.Revision, EffectiveClientLimit: f.client.MaxConcurrency, ConfiguredClientLimit: f.client.MaxConcurrency, PoolGatewayInflightLimit: f.pool.MaxGatewayInflight, RequestsPerMinute: f.client.RequestsPerMinute, TokensPerMinute: f.client.TokensPerMinute, LeaseTTL: time.Minute}
}

func refreshFixture(t *testing.T, store *pgstore.Store, f fixture) fixture {
	t.Helper()
	snapshot, err := store.LoadSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.revision = snapshot.Revision
	for _, client := range snapshot.Clients {
		if client.ID == f.client.ID {
			f.client = client
			break
		}
	}
	for _, pool := range snapshot.Pools {
		if pool.ID == f.pool.ID {
			f.pool = pool
			break
		}
	}
	for _, backend := range snapshot.Backends {
		if backend.ID == f.backend.ID {
			f.backend = backend
			break
		}
	}
	return f
}

func updateFixtureLimits(t *testing.T, store *pgstore.Store, f fixture, clientLimit, poolLimit int) fixture {
	t.Helper()
	if _, err := store.UpdatePool(context.Background(), f.pool.ID, basestore.UpdatePoolParams{
		PublicModelName: f.pool.PublicModelName, UpstreamModelName: f.pool.UpstreamModelName,
		Enabled: f.pool.Enabled, MaxGatewayInflight: poolLimit, MaxWaiting: f.pool.MaxWaiting,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateClient(context.Background(), f.client.ID, basestore.UpdateClientParams{
		Name: f.client.Name, Enabled: f.client.Enabled, PriorityClass: f.client.PriorityClass,
		VLLMPriority: f.client.VLLMPriority, MaxConcurrency: clientLimit,
		RequestsPerMinute: f.client.RequestsPerMinute, TokensPerMinute: f.client.TokensPerMinute,
		ModelPoolIDs: []int64{f.pool.ID},
	}); err != nil {
		t.Fatal(err)
	}
	return refreshFixture(t, store, f)
}

func TestPostgresAdmissionContentionAcrossReplicasNeverOversubscribes(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	coordinators := []*coordpostgres.AdmissionCoordinator{
		coordpostgres.NewAdmissionCoordinator(store, 2*time.Second),
		coordpostgres.NewAdmissionCoordinator(store, 2*time.Second),
	}
	var admitted atomic.Int32
	var rejected atomic.Int32
	var first coordination.LeaseIdentity
	var firstMu sync.Mutex
	var group sync.WaitGroup
	for index := 0; index < 24; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			decision, err := coordinators[index%len(coordinators)].Acquire(context.Background(), admissionRequest(fixture, uuid.New()))
			if err != nil {
				t.Errorf("Acquire(): %v", err)
				return
			}
			if decision.Admitted() {
				admitted.Add(1)
				firstMu.Lock()
				first = decision.Lease.Identity()
				firstMu.Unlock()
				return
			}
			if decision.Reason != coordination.ReasonConcurrencyExhausted {
				t.Errorf("rejection reason=%q", decision.Reason)
				return
			}
			rejected.Add(1)
		}(index)
	}
	group.Wait()
	if admitted.Load() != 1 || rejected.Load() != 23 {
		t.Fatalf("admitted=%d rejected=%d", admitted.Load(), rejected.Load())
	}
	if _, err := coordinators[0].Complete(context.Background(), []coordination.LeaseCompletion{{Lease: first}}); err != nil {
		t.Fatal(err)
	}
}

func TestTwoGatewaysRenewLongStreamThroughPostgresLeaseManager(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	secret := []byte(strings.Repeat("h", 32))
	rawKey := "llmgw_test12" + strings.Repeat("a", 37)
	digest := apikey.Digest(secret, rawKey)
	if _, err := store.ConfigPool().Exec(context.Background(), "UPDATE api_keys SET secret_hash=$2::bytea WHERE id=$1::bigint", fixture.key.ID, digest[:]); err != nil {
		t.Fatal(err)
	}
	registryValue := registry.New(store)
	if err := registryValue.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	renewTicks := make(chan time.Time, 1)
	leases := coordination.NewLeaseManager(context.Background(), coordinator, coordination.LeaseManagerOptions{
		RenewTicks: renewTicks, CompletionBacklog: 8, ShutdownTimeout: time.Second,
	})
	t.Cleanup(func() { leases.Close() })
	firstForwarder := &blockingGatewayForwarder{started: make(chan struct{}), release: make(chan struct{})}
	first := newPostgresGatewayService(registryValue, secret, coordinator, leases, firstForwarder, 500*time.Millisecond)
	secondForwarder := &blockingGatewayForwarder{started: make(chan struct{}), release: make(chan struct{})}
	close(secondForwarder.release)
	second := newPostgresGatewayService(registryValue, secret, coordinator, nil, secondForwarder, 500*time.Millisecond)

	firstDone := make(chan *gateway.APIError, 1)
	go func() {
		_, _, apiErr := first.Forward(context.Background(), httptest.NewRecorder(), postgresGatewayRequest(rawKey, "long-stream"))
		firstDone <- apiErr
	}()
	select {
	case <-firstForwarder.started:
	case apiErr := <-firstDone:
		t.Fatalf("first gateway returned before upstream: %+v", apiErr)
	case <-time.After(time.Second):
		t.Fatal("first gateway did not reach the held upstream")
	}
	var initialExpiry time.Time
	if err := store.CoordinationPool().QueryRow(context.Background(), "SELECT expires_at FROM request_leases").Scan(&initialExpiry); err != nil {
		t.Fatal(err)
	}
	if delay := time.Until(initialExpiry.Add(-200 * time.Millisecond)); delay > 0 {
		time.Sleep(delay)
	}
	renewTicks <- time.Now()
	var renewedExpiry time.Time
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := store.CoordinationPool().QueryRow(context.Background(), "SELECT expires_at FROM request_leases").Scan(&renewedExpiry); err == nil && renewedExpiry.After(initialExpiry) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !renewedExpiry.After(initialExpiry) {
		t.Fatalf("lease expiry was not renewed: initial=%s current=%s", initialExpiry, renewedExpiry)
	}
	if delay := time.Until(initialExpiry.Add(30 * time.Millisecond)); delay > 0 {
		time.Sleep(delay)
	}

	_, _, apiErr := second.Forward(context.Background(), httptest.NewRecorder(), postgresGatewayRequest(rawKey, "competing"))
	if apiErr == nil || apiErr.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("second gateway result=%+v, want distributed concurrency rejection", apiErr)
	}
	select {
	case <-secondForwarder.started:
		t.Fatal("second gateway reached upstream after the original lease expiry")
	default:
	}

	close(firstForwarder.release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first gateway error=%+v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first gateway did not finish")
	}
}

type postgresGatewayRuntime struct{}

func (postgresGatewayRuntime) PoolSnapshot(poolID int64, _ time.Time) domain.PoolRuntime {
	return domain.PoolRuntime{PoolID: poolID, State: domain.PoolNormal}
}
func (postgresGatewayRuntime) AcquirePool(int64, int) (func(), bool) { return func() {}, true }
func (postgresGatewayRuntime) Snapshot(backendID int64, _ time.Time) domain.BackendRuntime {
	return domain.BackendRuntime{BackendID: backendID, Healthy: true, MetricsFresh: true, CircuitAvailable: true}
}
func (postgresGatewayRuntime) AcquireBackend(domain.Backend, time.Time) (func(domain.InferenceOutcome), bool) {
	return func(domain.InferenceOutcome) {}, true
}

type blockingGatewayForwarder struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingGatewayForwarder) Forward(ctx context.Context, _ http.ResponseWriter, request proxy.Request) proxy.Result {
	f.once.Do(func() { close(f.started) })
	select {
	case <-f.release:
		request.Target.Complete(domain.InferenceSuccess)
		return proxy.Result{BackendID: request.Target.Backend.ID, Status: http.StatusOK}
	case <-ctx.Done():
		request.Target.Complete(domain.InferenceNeutral)
		return proxy.Result{BackendID: request.Target.Backend.ID, Cancelled: true, Err: ctx.Err()}
	}
}

func newPostgresGatewayService(snapshot gateway.SnapshotProvider, secret []byte, coordinator coordination.AdmissionCoordinator, leases *coordination.LeaseManager, forwarder gateway.Forwarder, ttl time.Duration) *gateway.Service {
	return gateway.New(gateway.Dependencies{
		Registry: snapshot, HMACSecret: secret, Admission: coordinator, Leases: leases, LeaseTTL: ttl,
		Runtime: postgresGatewayRuntime{}, Router: routing.New(0.05, routing.FixedSource(0)), Forwarder: forwarder,
	})
}

func postgresGatewayRequest(apiKey, requestID string) gateway.ForwardRequest {
	return gateway.ForwardRequest{
		Method: http.MethodPost, Path: "/v1/chat/completions", APIKey: apiKey, RequestID: requestID,
		Headers: make(http.Header), Body: []byte(`{"model":"test-model","messages":[{"role":"user","content":"hello"}],"stream":true}`),
	}
}

func TestPostgresAdmissionBackendNeutralContract(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	contracttest.AdmissionBaseline(t, func(t *testing.T, rpm int64) (coordination.AdmissionCoordinator, func() coordination.AdmissionRequest) {
		resetDatabase(t, store)
		fixture := createFixture(t, store, rpm)
		coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
		return coordinator, func() coordination.AdmissionRequest { return admissionRequest(fixture, uuid.New()) }
	})
}

func TestPostgresCircuitBackendNeutralContract(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	contracttest.CircuitBaseline(t, func(t *testing.T) (coordination.CircuitCoordinator, coordination.BackendIdentity) {
		resetDatabase(t, store)
		fixture := createFixture(t, store, 0)
		coordinator, err := coordpostgres.NewCircuitCoordinator(store, time.Second, circuitbreaker.Options{
			FailureThreshold: 1, FailureWindow: time.Hour, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		return coordinator, coordination.BackendIdentity{ID: fixture.backend.ID, Revision: fixture.backend.Revision, Enabled: true}
	})
}

func TestPostgresAdmissionReplayAndImmutableInputConflict(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	first := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	second := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	request := admissionRequest(fixture, uuid.New())
	decision, err := first.Acquire(context.Background(), request)
	if err != nil || !decision.Admitted() {
		t.Fatalf("initial acquire=%+v err=%v", decision, err)
	}
	replayed, err := second.Acquire(context.Background(), request)
	if err != nil || !replayed.Admitted() || replayed.Lease.LeaseID != decision.Lease.LeaseID || !replayed.Lease.ExpiresAt.Equal(decision.Lease.ExpiresAt) {
		t.Fatalf("replayed acquire=%+v err=%v", replayed, err)
	}
	request.EffectiveClientLimit++
	conflict, err := second.Acquire(context.Background(), request)
	if err != nil || conflict.Reason != coordination.ReasonIdempotencyConflict {
		t.Fatalf("conflicting replay=%+v err=%v", conflict, err)
	}
}

func TestPostgresAdmissionRejectsFutureOperationTimestamp(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	request := admissionRequest(fixture, uuid.New())
	request.OperationStartedAt = time.Now().UTC().Add(6 * time.Minute)
	decision, err := coordpostgres.NewAdmissionCoordinator(store, time.Second).Acquire(context.Background(), request)
	if err != nil || decision.Reason != coordination.ReasonStaleOperation {
		t.Fatalf("future acquire=%+v err=%v", decision, err)
	}
}

func TestPostgresSoftTPMChargesActualUsageExactlyOnce(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createRateFixture(t, store, 0, 5)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	decision, err := coordinator.Acquire(context.Background(), admissionRequest(fixture, uuid.New()))
	if err != nil || !decision.Admitted() {
		t.Fatalf("acquire=%+v err=%v", decision, err)
	}
	completion := coordination.LeaseCompletion{Lease: decision.Lease.Identity(), Usage: &coordination.TokenUsage{InputTokens: 4, OutputTokens: 2}}
	if _, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{completion}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{completion}); err != nil {
		t.Fatal(err)
	}
	rejected, err := coordinator.Acquire(context.Background(), admissionRequest(fixture, uuid.New()))
	if err != nil || rejected.Reason != coordination.ReasonTPMExhausted {
		t.Fatalf("TPM decision=%+v err=%v", rejected, err)
	}
}

func TestPostgresCRUDAndAggregateAdmission(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 1)
	first := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	second := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	replicaA, replicaB := uuid.New(), uuid.New()
	decision, err := first.Acquire(context.Background(), admissionRequest(f, replicaA))
	if err != nil || !decision.Admitted() {
		t.Fatalf("first acquire=%+v err=%v", decision, err)
	}
	rejected, err := second.Acquire(context.Background(), admissionRequest(f, replicaB))
	if err != nil || rejected.Reason != coordination.ReasonConcurrencyExhausted {
		t.Fatalf("aggregate rejection=%+v err=%v", rejected, err)
	}
	if _, err := first.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: decision.Lease.Identity()}}); err != nil {
		t.Fatal(err)
	}
	rateRejected, err := second.Acquire(context.Background(), admissionRequest(f, replicaB))
	if err != nil || rateRejected.Reason != coordination.ReasonRPMExhausted {
		t.Fatalf("RPM rejection=%+v err=%v", rateRejected, err)
	}
}

func TestPostgresUnlimitedPoolAccountingSurvivesZeroToPositiveTransition(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := updateFixtureLimits(t, store, createFixture(t, store, 0), 3, 0)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	first, err := coordinator.Acquire(context.Background(), admissionRequest(f, uuid.New()))
	if err != nil || !first.Admitted() {
		t.Fatalf("first unlimited acquire=%+v err=%v", first, err)
	}
	second, err := coordinator.Acquire(context.Background(), admissionRequest(f, uuid.New()))
	if err != nil || !second.Admitted() {
		t.Fatalf("second unlimited acquire=%+v err=%v", second, err)
	}
	if _, err := store.UpdatePool(context.Background(), f.pool.ID, basestore.UpdatePoolParams{
		PublicModelName: f.pool.PublicModelName, UpstreamModelName: f.pool.UpstreamModelName,
		Enabled: f.pool.Enabled, MaxGatewayInflight: 1, MaxWaiting: f.pool.MaxWaiting,
	}); err != nil {
		t.Fatal(err)
	}
	f = refreshFixture(t, store, f)
	rejected, err := coordinator.Acquire(context.Background(), admissionRequest(f, uuid.New()))
	if err != nil || rejected.Reason != coordination.ReasonConcurrencyExhausted {
		t.Fatalf("post-transition decision=%+v err=%v", rejected, err)
	}
	if _, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: first.Lease.Identity()}, {Lease: second.Lease.Identity()}}); err != nil {
		t.Fatal(err)
	}
	admitted, err := coordinator.Acquire(context.Background(), admissionRequest(f, uuid.New()))
	if err != nil || !admitted.Admitted() {
		t.Fatalf("acquire after owners completed=%+v err=%v", admitted, err)
	}
}

func TestPostgresClientLimitReductionCountsExistingOwners(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := updateFixtureLimits(t, store, createFixture(t, store, 0), 3, 0)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	for index := 0; index < 2; index++ {
		decision, err := coordinator.Acquire(context.Background(), admissionRequest(f, uuid.New()))
		if err != nil || !decision.Admitted() {
			t.Fatalf("owner %d acquire=%+v err=%v", index, decision, err)
		}
	}
	if _, err := store.UpdateClient(context.Background(), f.client.ID, basestore.UpdateClientParams{
		Name: f.client.Name, Enabled: f.client.Enabled, PriorityClass: f.client.PriorityClass,
		VLLMPriority: f.client.VLLMPriority, MaxConcurrency: 1,
		RequestsPerMinute: f.client.RequestsPerMinute, TokensPerMinute: f.client.TokensPerMinute,
		ModelPoolIDs: []int64{f.pool.ID},
	}); err != nil {
		t.Fatal(err)
	}
	f = refreshFixture(t, store, f)
	rejected, err := coordinator.Acquire(context.Background(), admissionRequest(f, uuid.New()))
	if err != nil || rejected.Reason != coordination.ReasonConcurrencyExhausted {
		t.Fatalf("reduced-limit decision=%+v err=%v", rejected, err)
	}
}

func TestPostgresCompletedAcquireReplayDoesNotDebitRPMAgain(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 2)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	request := admissionRequest(f, uuid.New())
	decision, err := coordinator.Acquire(context.Background(), request)
	if err != nil || !decision.Admitted() {
		t.Fatalf("acquire=%+v err=%v", decision, err)
	}
	if _, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: decision.Lease.Identity()}}); err != nil {
		t.Fatal(err)
	}
	replayed, err := coordinator.Acquire(context.Background(), request)
	if err != nil || replayed.Reason != coordination.ReasonLeaseLost {
		t.Fatalf("completed replay=%+v err=%v", replayed, err)
	}
	var balance float64
	if err := store.CoordinationPool().QueryRow(context.Background(), "SELECT balance FROM coordination_rate_state WHERE client_id=$1::bigint AND rate_kind='rpm'", f.client.ID).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance < 0.99 || balance > 1.01 {
		t.Fatalf("RPM balance after replay=%f, want one debit from capacity 2", balance)
	}
}

func TestPostgresExpiredLeaseCannotBeRenewedOrResurrected(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	request := admissionRequest(f, uuid.New())
	request.LeaseTTL = 15 * time.Millisecond
	decision, err := coordinator.Acquire(context.Background(), request)
	if err != nil || !decision.Admitted() {
		t.Fatalf("acquire=%+v err=%v", decision, err)
	}
	time.Sleep(25 * time.Millisecond)
	renewed, err := coordinator.Renew(context.Background(), []coordination.LeaseIdentity{decision.Lease.Identity()})
	if err != nil || len(renewed) != 1 || renewed[0].Reason != coordination.ReasonLeaseLost {
		t.Fatalf("renew=%+v err=%v", renewed, err)
	}
	replayed, err := coordinator.Acquire(context.Background(), request)
	if err != nil || replayed.Reason != coordination.ReasonLeaseLost {
		t.Fatalf("expired replay=%+v err=%v", replayed, err)
	}
}

func TestPostgresCleanupDoesNotExpireLeaseRenewedWhileWaitingForLock(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, 2*time.Second)
	request := admissionRequest(fixture, uuid.New())
	decision, err := coordinator.Acquire(context.Background(), request)
	if err != nil || !decision.Admitted() {
		t.Fatalf("Acquire() = %+v, %v", decision, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := store.CoordinationPool().Exec(ctx, "UPDATE request_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE lease_id=$1::uuid", request.LeaseID); err != nil {
		t.Fatal(err)
	}
	renewal, err := store.CoordinationPool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer renewal.Rollback(ctx)
	if _, err := renewal.Exec(ctx, "UPDATE request_leases SET expires_at=clock_timestamp()+interval '1 minute' WHERE lease_id=$1::uuid", request.LeaseID); err != nil {
		t.Fatal(err)
	}
	cleanupDone := make(chan error, 1)
	go func() { cleanupDone <- coordinator.Cleanup(ctx, 1) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var waiting bool
		err := store.ConfigPool().QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM pg_stat_activity
			WHERE datname=current_database() AND wait_event_type='Lock'
			  AND query LIKE 'SELECT lease_id FROM request_leases WHERE lease_id=%FOR UPDATE%'
		)`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cleanup did not reach the lease row lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := renewal.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-cleanupDone; err != nil {
		t.Fatal(err)
	}
	var expiresAt time.Time
	var marked bool
	if err := store.CoordinationPool().QueryRow(ctx, `SELECT l.expires_at,o.lease_expired_at IS NOT NULL
		FROM request_leases l JOIN admission_operations o USING(lease_id) WHERE l.lease_id=$1::uuid`, request.LeaseID).Scan(&expiresAt, &marked); err != nil {
		t.Fatal(err)
	}
	if marked || !expiresAt.After(time.Now()) {
		t.Fatalf("renewed lease expires_at=%s marked_expired=%t", expiresAt, marked)
	}
}

func TestPostgresLockWaitUsesPostLockExpiryAcrossClientPools(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	secondPool, err := store.CreatePool(context.Background(), basestore.CreatePoolParams{
		PublicModelName: "second-model", UpstreamModelName: "second-upstream", Enabled: true, MaxGatewayInflight: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateClient(context.Background(), fixture.client.ID, basestore.UpdateClientParams{
		Name: fixture.client.Name, Enabled: true, PriorityClass: fixture.client.PriorityClass,
		VLLMPriority: fixture.client.VLLMPriority, MaxConcurrency: 1,
		RequestsPerMinute: fixture.client.RequestsPerMinute, TokensPerMinute: fixture.client.TokensPerMinute,
		ModelPoolIDs: []int64{fixture.pool.ID, secondPool.ID},
	}); err != nil {
		t.Fatal(err)
	}
	fixture = refreshFixture(t, store, fixture)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, 2*time.Second)
	firstRequest := admissionRequest(fixture, uuid.New())
	firstRequest.LeaseTTL = 200 * time.Millisecond
	first, err := coordinator.Acquire(context.Background(), firstRequest)
	if err != nil || !first.Admitted() {
		t.Fatalf("first=%+v err=%v", first, err)
	}

	holder, err := store.CoordinationPool().Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(context.Background())
	if _, err := holder.Exec(context.Background(), "SELECT client_id FROM client_admission_scopes WHERE client_id=$1::bigint FOR UPDATE", fixture.client.ID); err != nil {
		t.Fatal(err)
	}
	renewed := make(chan []coordination.RenewResult, 1)
	renewErrors := make(chan error, 1)
	go func() {
		results, err := coordinator.Renew(context.Background(), []coordination.LeaseIdentity{first.Lease.Identity()})
		renewed <- results
		renewErrors <- err
	}()
	secondRequest := admissionRequest(fixture, uuid.New())
	secondRequest.PoolID = secondPool.ID
	secondRequest.PoolGatewayInflightLimit = secondPool.MaxGatewayInflight
	secondDecision := make(chan coordination.AdmissionDecision, 1)
	secondErrors := make(chan error, 1)
	go func() {
		decision, err := coordinator.Acquire(context.Background(), secondRequest)
		secondDecision <- decision
		secondErrors <- err
	}()
	if delay := time.Until(first.Lease.ExpiresAt.Add(20 * time.Millisecond)); delay > 0 {
		time.Sleep(delay)
	}
	if err := holder.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}

	results, err := <-renewed, <-renewErrors
	if err != nil || len(results) != 1 || results[0].Reason != coordination.ReasonLeaseLost {
		t.Fatalf("renew after lock wait=%+v err=%v", results, err)
	}
	decision, err := <-secondDecision, <-secondErrors
	if err != nil || !decision.Admitted() {
		t.Fatalf("other-pool acquire after lock wait=%+v err=%v", decision, err)
	}
}

func TestPostgresSoftTPMMissingUsageAndPolicyGenerationReset(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createRateFixture(t, store, 0, 1)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	first, err := coordinator.Acquire(context.Background(), admissionRequest(f, uuid.New()))
	if err != nil || !first.Admitted() {
		t.Fatalf("first acquire=%+v err=%v", first, err)
	}
	if _, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: first.Lease.Identity()}}); err != nil {
		t.Fatal(err)
	}
	second, err := coordinator.Acquire(context.Background(), admissionRequest(f, uuid.New()))
	if err != nil || !second.Admitted() {
		t.Fatalf("missing usage consumed TPM: decision=%+v err=%v", second, err)
	}
	usage := &coordination.TokenUsage{InputTokens: 2}
	if _, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: second.Lease.Identity(), Usage: usage}}); err != nil {
		t.Fatal(err)
	}
	rejected, err := coordinator.Acquire(context.Background(), admissionRequest(f, uuid.New()))
	if err != nil || rejected.Reason != coordination.ReasonTPMExhausted {
		t.Fatalf("negative TPM decision=%+v err=%v", rejected, err)
	}
	if _, err := store.UpdateClient(context.Background(), f.client.ID, basestore.UpdateClientParams{
		Name: f.client.Name, Enabled: f.client.Enabled, PriorityClass: f.client.PriorityClass,
		VLLMPriority: f.client.VLLMPriority, MaxConcurrency: f.client.MaxConcurrency,
		RequestsPerMinute: 0, TokensPerMinute: 3, ModelPoolIDs: []int64{f.pool.ID},
	}); err != nil {
		t.Fatal(err)
	}
	f = refreshFixture(t, store, f)
	reset, err := coordinator.Acquire(context.Background(), admissionRequest(f, uuid.New()))
	if err != nil || !reset.Admitted() {
		t.Fatalf("new policy generation did not reset TPM: decision=%+v err=%v", reset, err)
	}
}

func TestPostgresAnalyticsBatchIsIdempotentAndQueryable(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	now := time.Now().UTC()
	input, output, cached := int64(10), int64(4), int64(3)
	record := analytics.RequestRecord{
		OccurredAt: now, RequestID: uuid.NewString(), ClientID: f.client.ID, ClientName: f.client.Name,
		ModelPoolID: f.pool.ID, ModelName: f.pool.PublicModelName, BackendName: f.backend.Name,
		HTTPStatus: 200, DurationMS: 12, UsageAvailable: true,
		InputTokens: &input, OutputTokens: &output, CacheReadTokens: &cached,
	}
	if err := store.InsertUsageBatch(context.Background(), []analytics.RequestRecord{record, record}); err != nil {
		t.Fatal(err)
	}
	dataset, page, err := store.AnalyticsDashboard(context.Background(), analytics.Filter{From: now.Add(-time.Minute), To: now.Add(time.Minute)}, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dataset.Summary.RequestCount != 1 || dataset.Summary.InputTokens != input || dataset.Summary.OutputTokens != output || page.Total != 1 || len(page.Requests) != 1 {
		t.Fatalf("dataset=%+v page=%+v", dataset.Summary, page)
	}
}

func TestPostgresAnalyticsSeriesPreservesNanosecondRangeData(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	from := time.Date(2026, time.September, 14, 10, 0, 0, 123456789, time.UTC)
	input, output := int64(10), int64(2)
	record := postgresAnalyticsRecord("fractional-range", from.Add(time.Minute), 1, "client", 1, "model", &input, &output, nil)
	if err := store.InsertUsageBatch(context.Background(), []analytics.RequestRecord{record}); err != nil {
		t.Fatal(err)
	}

	dataset, err := store.Analytics(context.Background(), analytics.Filter{From: from, To: from.Add(15 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	var requests, tokens int64
	for _, point := range dataset.Series {
		requests += point.RequestCount
		tokens += point.InputTokens
	}
	if requests != 1 || tokens != input {
		t.Fatalf("series lost fractional-boundary data: requests=%d input_tokens=%d, want 1 and %d", requests, tokens, input)
	}
}

func TestPostgresUsageRangeBoundsCeilFractionalMicrosecondsForEveryQueryPath(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	ctx := context.Background()
	base := time.Date(2026, time.September, 14, 12, 0, 0, 123_456_000, time.UTC)
	if err := store.InsertUsageBatch(ctx, []analytics.RequestRecord{
		postgresAnalyticsRecord("at-base", base, 1, "client", 10, "model", nil, nil, nil),
		postgresAnalyticsRecord("at-next-microsecond", base.Add(time.Microsecond), 1, "client", 10, "model", nil, nil, nil),
	}); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		filter analytics.Filter
		wantID string
	}{
		{
			name:   "fractional inclusive from excludes the containing stored microsecond",
			filter: analytics.Filter{From: base.Add(time.Nanosecond), To: base.Add(2 * time.Microsecond)},
			wantID: "at-next-microsecond",
		},
		{
			name:   "fractional exclusive to includes the containing stored microsecond",
			filter: analytics.Filter{From: base, To: base.Add(time.Nanosecond)},
			wantID: "at-base",
		},
		{
			name:   "exact microsecond remains unchanged",
			filter: analytics.Filter{From: base, To: base.Add(time.Microsecond)},
			wantID: "at-base",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dataset, err := store.Analytics(ctx, test.filter)
			if err != nil {
				t.Fatal(err)
			}
			page, err := store.UsageRequests(ctx, test.filter, 100, 0)
			if err != nil {
				t.Fatal(err)
			}
			dashboard, dashboardPage, err := store.AnalyticsDashboard(ctx, test.filter, 100, 0)
			if err != nil {
				t.Fatal(err)
			}
			var streamed []string
			if err := store.StreamUsageRequests(ctx, test.filter, func(record analytics.RequestRecord) error {
				streamed = append(streamed, record.RequestID)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			pageIDs := make([]string, 0, len(page.Requests))
			for _, record := range page.Requests {
				pageIDs = append(pageIDs, record.RequestID)
			}
			dashboardIDs := make([]string, 0, len(dashboardPage.Requests))
			for _, record := range dashboardPage.Requests {
				dashboardIDs = append(dashboardIDs, record.RequestID)
			}
			if dataset.Summary.RequestCount != 1 || dashboard.Summary.RequestCount != 1 || page.Total != 1 ||
				!reflect.DeepEqual(pageIDs, []string{test.wantID}) || dashboardPage.Total != 1 ||
				!reflect.DeepEqual(dashboardIDs, []string{test.wantID}) ||
				!reflect.DeepEqual(streamed, []string{test.wantID}) {
				t.Fatalf("range query mismatch: analytics=%d dashboard=%d page=%+v dashboardPage=%+v stream=%v",
					dataset.Summary.RequestCount, dashboard.Summary.RequestCount, page, dashboardPage, streamed)
			}
		})
	}
}

func TestPostgresAnalyticsBoundsExtremeCustomRangeSeries(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	ctx := context.Background()
	from := time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(9999, time.December, 31, 23, 59, 59, 999_999_999, time.UTC)
	occurredAt := time.Date(9999, time.December, 31, 23, 59, 59, 998_000_000, time.UTC)
	if err := store.InsertUsageBatch(ctx, []analytics.RequestRecord{
		postgresAnalyticsRecord("extreme-range", occurredAt, 1, "client", 10, "model", nil, nil, nil),
	}); err != nil {
		t.Fatal(err)
	}

	dataset, err := store.Analytics(ctx, analytics.Filter{From: from, To: to})
	if err != nil {
		t.Fatal(err)
	}
	if dataset.Summary.RequestCount != 1 {
		t.Fatalf("Analytics().Summary.RequestCount = %d, want 1 for the complete RFC 3339 year range", dataset.Summary.RequestCount)
	}
	if len(dataset.Series) == 0 || len(dataset.Series) > 366 {
		t.Fatalf("Analytics().Series length = %d, want 1 through 366 points", len(dataset.Series))
	}
	if dataset.Series[len(dataset.Series)-1].RequestCount != 1 {
		t.Fatalf("last series point = %+v, want the matching request", dataset.Series[len(dataset.Series)-1])
	}
}

func TestPostgresAnalyticsMatchesFilterNullAndCacheRatioContract(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	from := time.Date(2026, time.September, 14, 10, 0, 0, 0, time.UTC)
	to := from.Add(15 * time.Minute)
	input10, input20, input7 := int64(10), int64(20), int64(7)
	output2, output3, output1, cache4 := int64(2), int64(3), int64(1), int64(4)
	records := []analytics.RequestRecord{
		postgresAnalyticsRecord("before", from.Add(-time.Millisecond), 9, "excluded", 90, "excluded-model", &input7, &output1, nil),
		postgresAnalyticsRecord("cache-known", from, 1, "client-old", 10, "model-old", &input10, &output2, &cache4),
		postgresAnalyticsRecord("cache-unknown", from.Add(5*time.Minute), 1, "client-current", 10, "model-current", &input20, &output3, nil),
		postgresAnalyticsRecord("unmetered", from.Add(10*time.Minute), 2, "client-two", 20, "model-two", nil, nil, nil),
		postgresAnalyticsRecord("excluded-to", to, 2, "client-two", 20, "model-two", &input7, &output1, nil),
		postgresAnalyticsRecord("latest-name", to.Add(time.Minute), 1, "client-renamed", 10, "model-renamed", nil, nil, nil),
	}
	if err := store.InsertUsageBatch(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	dataset, err := store.Analytics(context.Background(), analytics.Filter{From: from, To: to})
	if err != nil {
		t.Fatal(err)
	}
	if dataset.Summary.RequestCount != 3 || dataset.Summary.MeteredRequestCount != 2 || dataset.Summary.InputTokens != 30 || dataset.Summary.OutputTokens != 5 {
		t.Fatalf("summary=%+v", dataset.Summary)
	}
	if dataset.Summary.CacheReadTokens == nil || *dataset.Summary.CacheReadTokens != 4 || dataset.Summary.CacheHitRatio == nil || *dataset.Summary.CacheHitRatio != 0.4 {
		t.Fatalf("cache summary=%+v", dataset.Summary)
	}
	if len(dataset.Series) != 3 || dataset.Series[1].CacheReadTokens != nil || dataset.Series[1].CacheHitRatio != nil || dataset.Series[2].InputTokens != 0 {
		t.Fatalf("series=%+v", dataset.Series)
	}
	wantClients := []analytics.Dimension{{ID: 1, Name: "client-renamed"}, {ID: 2, Name: "client-two"}, {ID: 9, Name: "excluded"}}
	wantModels := []analytics.Dimension{{ID: 10, Name: "model-renamed"}, {ID: 20, Name: "model-two"}, {ID: 90, Name: "excluded-model"}}
	if !reflect.DeepEqual(dataset.Clients, wantClients) || !reflect.DeepEqual(dataset.Models, wantModels) {
		t.Fatalf("dimensions=%+v/%+v", dataset.Clients, dataset.Models)
	}
	clientOne, modelTwenty, available, unavailable := int64(1), int64(20), true, false
	for _, test := range []struct {
		name   string
		filter analytics.Filter
		want   int64
	}{
		{name: "client", filter: analytics.Filter{From: from, To: to, ClientID: &clientOne}, want: 2},
		{name: "model", filter: analytics.Filter{From: from, To: to, ModelPoolID: &modelTwenty}, want: 1},
		{name: "metered", filter: analytics.Filter{From: from, To: to, UsageAvailable: &available}, want: 2},
		{name: "unmetered", filter: analytics.Filter{From: from, To: to, UsageAvailable: &unavailable}, want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			filtered, err := store.Analytics(context.Background(), test.filter)
			if err != nil || filtered.Summary.RequestCount != test.want {
				t.Fatalf("filtered summary=%+v err=%v, want %d", filtered.Summary, err, test.want)
			}
		})
	}
}

func postgresAnalyticsRecord(requestID string, occurredAt time.Time, clientID int64, clientName string, modelPoolID int64, modelName string, inputTokens, outputTokens, cacheReadTokens *int64) analytics.RequestRecord {
	return analytics.RequestRecord{
		OccurredAt: occurredAt, RequestID: requestID, ClientID: clientID, ClientName: clientName,
		ModelPoolID: modelPoolID, ModelName: modelName, HTTPStatus: http.StatusOK, DurationMS: 1,
		UsageAvailable: inputTokens != nil && outputTokens != nil,
		InputTokens:    inputTokens, OutputTokens: outputTokens, CacheReadTokens: cacheReadTokens,
	}
}

func TestPostgresConfigNotificationReloadsCommittedRevision(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	revisions := make(chan int64, 8)
	go store.WatchConfig(ctx, time.Minute, func(reloadCtx context.Context) error {
		revision, err := store.CurrentRevision(reloadCtx)
		if err == nil {
			select {
			case revisions <- revision:
			default:
			}
		}
		return err
	})

	// The watcher reloads once before connecting and once immediately after
	// LISTEN succeeds. Waiting for both closes the connection-establishment gap.
	for index := 0; index < 2; index++ {
		select {
		case <-revisions:
		case <-ctx.Done():
			t.Fatal("watcher did not establish its LISTEN connection")
		}
	}
	before, err := store.CurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreatePool(ctx, basestore.CreatePoolParams{
		PublicModelName: "notify-model", UpstreamModelName: "notify-upstream", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case revision := <-revisions:
		if revision != before+1 {
			t.Fatalf("notified revision=%d, want %d", revision, before+1)
		}
	case <-ctx.Done():
		t.Fatal("committed configuration notification did not trigger reload")
	}
}

func TestPostgresAnalyticsRetentionDrainsMultipleBatches(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	ctx := context.Background()
	_, err := store.AnalyticsPool().Exec(ctx, `INSERT INTO usage_requests(occurred_at,request_id,client_id,client_name,model_pool_id,model_name,backend_name,http_status,duration_ms,retry_count,disconnected,usage_available)
		SELECT clock_timestamp()-interval '2 hours','retention-'||g::text,1,'client',1,'model','backend',200,1,0,false,false FROM generate_series(1,1001) g`)
	if err != nil {
		t.Fatal(err)
	}
	recorder := analytics.NewRecorder(store, time.Hour, nil, nil)
	count := 1001
	deadline := time.Now().Add(time.Second)
	for count > 0 && time.Now().Before(deadline) {
		if err = store.AnalyticsPool().QueryRow(ctx, "SELECT count(*) FROM usage_requests").Scan(&count); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err = recorder.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.AnalyticsPool().QueryRow(ctx, "SELECT count(*) FROM usage_requests").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("retention left %d expired rows until the next hourly tick", count)
	}
}

func TestPostgresCircuitCapacityIsSharedAcrossCoordinators(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Second, HalfOpenMaxProbes: 1}
	first, err := coordpostgres.NewCircuitCoordinator(store, time.Second, options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := coordpostgres.NewCircuitCoordinator(store, time.Second, options)
	if err != nil {
		t.Fatal(err)
	}
	identity := coordination.BackendIdentity{ID: f.backend.ID, Revision: f.backend.Revision, Enabled: true}
	if err := first.Reconcile(context.Background(), []coordination.BackendIdentity{identity}); err != nil {
		t.Fatal(err)
	}
	if err := second.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	acquire := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Minute}
	closed, err := first.Acquire(context.Background(), acquire)
	if err != nil || closed.Reason != "" {
		t.Fatalf("closed acquire=%+v err=%v", closed, err)
	}
	if _, err := first.Complete(context.Background(), coordination.CircuitCompletion{AttemptID: acquire.AttemptID, Backend: identity, Generation: closed.Snapshot.Generation, Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if opened := first.Snapshot(f.backend.ID, time.Now()); opened.State != domain.CircuitOpen || opened.FailureCount != 1 {
		t.Fatalf("local open snapshot=%+v, want one retained failure", opened)
	}
	if err := second.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if opened := second.Snapshot(f.backend.ID, time.Now()); opened.State != domain.CircuitOpen || opened.FailureCount != 1 {
		t.Fatalf("remote open snapshot=%+v, want one retained failure", opened)
	}
	time.Sleep(options.OpenCooldown + 50*time.Millisecond)
	if err := second.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if halfOpen := second.Snapshot(f.backend.ID, time.Now()); halfOpen.State != domain.CircuitHalfOpen || !halfOpen.Available || halfOpen.FailureCount != 1 {
		t.Fatalf("remote half-open snapshot=%+v, want available with retained failure", halfOpen)
	}
	probeRequest := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Minute}
	probe, err := first.Acquire(context.Background(), probeRequest)
	if err != nil || probe.Permit == nil {
		t.Fatalf("probe=%+v err=%v", probe, err)
	}
	probeRequest.AttemptID = uuid.New()
	probeRequest.ReplicaID = uuid.New()
	probeRequest.AcquisitionStartedAt = time.Now().UTC()
	denied, err := second.Acquire(context.Background(), probeRequest)
	if err != nil || denied.Reason != coordination.ReasonProbeCapacityExhausted {
		t.Fatalf("shared probe denial=%+v err=%v", denied, err)
	}
	reopened, err := first.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: probe.Permit.PermitID, Backend: identity, Generation: probe.Permit.Generation,
		Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC(),
	})
	if err != nil || reopened.State != domain.CircuitOpen || reopened.FailureCount != 2 {
		t.Fatalf("failed probe completion=%+v err=%v, want two retained failures", reopened, err)
	}
	if err := second.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if reopened := second.Snapshot(f.backend.ID, time.Now()); reopened.State != domain.CircuitOpen || reopened.FailureCount != 2 {
		t.Fatalf("remote reopened snapshot=%+v, want two retained failures", reopened)
	}
}

func TestPostgresClosedCircuitFastPathsIgnoreOtherBackendLock(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	healthy, err := store.CreateBackend(context.Background(), basestore.CreateBackendParams{
		ModelPoolID: fixture.pool.ID, Name: "healthy", BaseURL: "http://healthy.invalid", Enabled: true, CapacityHint: 1, RunningSoftLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := coordpostgres.NewCircuitCoordinator(store, 100*time.Millisecond, circuitbreaker.Options{
		FailureThreshold: 2, FailureWindow: time.Minute, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	backends := []coordination.BackendIdentity{
		{ID: fixture.backend.ID, Revision: fixture.backend.Revision, Enabled: true},
		{ID: healthy.ID, Revision: healthy.Revision, Enabled: true},
	}
	if err = coordinator.Reconcile(context.Background(), backends); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ConfigPool().Exec(context.Background(), "UPDATE backend_circuit_state SET state='half_open' WHERE backend_id=$1", fixture.backend.ID); err != nil {
		t.Fatal(err)
	}
	holder, err := store.ConfigPool().Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	holderOpen := true
	defer func() {
		if holderOpen {
			_ = holder.Rollback(context.Background())
		}
	}()
	if _, err = holder.Exec(context.Background(), "SELECT backend_id FROM backend_circuit_state WHERE backend_id=$1 FOR UPDATE", fixture.backend.ID); err != nil {
		t.Fatal(err)
	}
	if err = coordinator.Refresh(context.Background()); err == nil {
		t.Fatal("refresh unexpectedly succeeded while an unrelated backend row was locked")
	}
	if status := coordinator.Status(); status.Available {
		t.Fatalf("failed refresh did not degrade coordinator: %+v", status)
	}

	request := coordination.CircuitAcquireRequest{
		AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(),
		Backend: backends[1], ProbeTTL: time.Minute,
	}
	decision, err := coordinator.Acquire(context.Background(), request)
	if err != nil || decision.Reason != "" {
		t.Fatalf("healthy backend acquire=%+v err=%v", decision, err)
	}
	if status := coordinator.Status(); status.Available {
		t.Fatalf("closed acquire incorrectly healed coordinator after failed refresh: %+v", status)
	}

	_, err = coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: uuid.New(), Backend: backends[1], Generation: decision.Snapshot.Generation,
		ReportedOutcomeAt: time.Now().UTC(), Outcome: domain.InferenceSuccess,
	})
	if err != nil {
		t.Fatalf("healthy backend completion: %v", err)
	}
	if status := coordinator.Status(); status.Available {
		t.Fatalf("closed completion incorrectly healed coordinator after failed refresh: %+v", status)
	}
	if err = holder.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	holderOpen = false
	if err = coordinator.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh after lock release: %v", err)
	}
	if status := coordinator.Status(); !status.Available {
		t.Fatalf("successful refresh did not restore coordinator: %+v", status)
	}
}

func TestPostgresCircuitCompletionReplayCanonicalizesTimestampPrecision(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	coordinator, err := coordpostgres.NewCircuitCoordinator(store, time.Second, circuitbreaker.Options{
		FailureThreshold: 2, FailureWindow: time.Minute, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := coordination.BackendIdentity{ID: fixture.backend.ID, Revision: fixture.backend.Revision, Enabled: true}
	request := coordination.CircuitAcquireRequest{
		AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute,
	}
	decision, err := coordinator.Acquire(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	completion := coordination.CircuitCompletion{
		AttemptID: request.AttemptID, Backend: backend, Generation: decision.Snapshot.Generation,
		Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC().Truncate(time.Microsecond).Add(123 * time.Nanosecond),
	}
	if _, err = coordinator.Complete(context.Background(), completion); err != nil {
		t.Fatal(err)
	}
	if _, err = coordinator.Complete(context.Background(), completion); err != nil {
		t.Fatalf("identical completion replay rejected: %v", err)
	}
}

func TestPostgresClosedCircuitAcquireAndNeutralCompletionDoNotTakeStateLock(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	coordinator, err := coordpostgres.NewCircuitCoordinator(store, 50*time.Millisecond, circuitbreaker.Options{
		FailureThreshold: 2, FailureWindow: time.Minute, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	holder, err := store.ConfigPool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(ctx)
	if _, err = holder.Exec(ctx, "SELECT backend_id FROM backend_circuit_state WHERE backend_id=$1::bigint FOR UPDATE", fixture.backend.ID); err != nil {
		t.Fatal(err)
	}
	backend := coordination.BackendIdentity{ID: fixture.backend.ID, Revision: fixture.backend.Revision, Enabled: true}
	request := coordination.CircuitAcquireRequest{
		AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute,
	}
	decision, err := coordinator.Acquire(ctx, request)
	if err != nil || decision.Reason != "" {
		t.Fatalf("closed acquire blocked by state lock: reason=%s err=%v", decision.Reason, err)
	}
	if _, err = coordinator.Complete(ctx, coordination.CircuitCompletion{
		AttemptID: request.AttemptID, Backend: backend, Generation: decision.Snapshot.Generation,
		Outcome: domain.InferenceNeutral, ReportedOutcomeAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("closed neutral completion blocked by state lock: %v", err)
	}
}

func TestPostgresClosedCircuitFailureCountExpiresWithWindow(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 3, FailureWindow: 40 * time.Millisecond, OpenCooldown: time.Second, HalfOpenMaxProbes: 1}
	coordinator, err := coordpostgres.NewCircuitCoordinator(store, time.Second, options)
	if err != nil {
		t.Fatal(err)
	}
	identity := coordination.BackendIdentity{ID: f.backend.ID, Revision: f.backend.Revision, Enabled: true}
	if err := coordinator.Reconcile(context.Background(), []coordination.BackendIdentity{identity}); err != nil {
		t.Fatal(err)
	}
	request := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Minute}
	decision, err := coordinator.Acquire(context.Background(), request)
	if err != nil || decision.Reason != "" {
		t.Fatalf("closed acquire=%+v err=%v", decision, err)
	}
	completion := coordination.CircuitCompletion{
		AttemptID: request.AttemptID, Backend: identity, Generation: decision.Snapshot.Generation,
		Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC(),
	}
	failed, err := coordinator.Complete(context.Background(), completion)
	if err != nil || failed.State != domain.CircuitClosed || failed.FailureCount != 1 {
		t.Fatalf("closed failure snapshot=%+v err=%v", failed, err)
	}
	var originalReceiptEvent time.Time
	if err = store.ConfigPool().QueryRow(context.Background(), "SELECT event_at FROM backend_circuit_failure_receipts WHERE attempt_id=$1::uuid", completion.AttemptID).Scan(&originalReceiptEvent); err != nil {
		t.Fatal(err)
	}
	time.Sleep(options.FailureWindow + 20*time.Millisecond)
	nextRequest := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Minute}
	nextDecision, err := coordinator.Acquire(context.Background(), nextRequest)
	if err != nil || nextDecision.Reason != "" {
		t.Fatalf("next closed acquire=%+v err=%v", nextDecision, err)
	}
	nextFailed, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: nextRequest.AttemptID, Backend: identity, Generation: nextDecision.Snapshot.Generation,
		Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC(),
	})
	if err != nil || nextFailed.State != domain.CircuitClosed || nextFailed.FailureCount != 1 {
		t.Fatalf("next closed failure snapshot=%+v err=%v", nextFailed, err)
	}
	var originalEvents, originalActiveEvents, originalReceipts int
	if err = store.ConfigPool().QueryRow(context.Background(), "SELECT count(*) FROM backend_circuit_failures WHERE attempt_id=$1::uuid", completion.AttemptID).Scan(&originalEvents); err != nil {
		t.Fatal(err)
	}
	if err = store.ConfigPool().QueryRow(context.Background(), "SELECT count(*) FROM backend_circuit_active_failures WHERE attempt_id=$1::uuid", completion.AttemptID).Scan(&originalActiveEvents); err != nil {
		t.Fatal(err)
	}
	if err = store.ConfigPool().QueryRow(context.Background(), "SELECT count(*) FROM backend_circuit_failure_receipts WHERE attempt_id=$1::uuid", completion.AttemptID).Scan(&originalReceipts); err != nil {
		t.Fatal(err)
	}
	if originalEvents != 0 || originalActiveEvents != 0 || originalReceipts != 1 {
		t.Fatalf("rolling prune left events=%d active_events=%d receipts=%d for original completion, want 0, 0, and 1", originalEvents, originalActiveEvents, originalReceipts)
	}

	replayed, err := coordinator.Complete(context.Background(), completion)
	if err != nil || replayed.State != domain.CircuitClosed || replayed.FailureCount != 1 {
		t.Fatalf("failure replay after rolling prune=%+v err=%v", replayed, err)
	}
	var replayEvents, replayActiveEvents int
	var replayReceiptEvent time.Time
	if err = store.ConfigPool().QueryRow(context.Background(), "SELECT count(*) FROM backend_circuit_failures WHERE attempt_id=$1::uuid", completion.AttemptID).Scan(&replayEvents); err != nil {
		t.Fatal(err)
	}
	if err = store.ConfigPool().QueryRow(context.Background(), "SELECT count(*) FROM backend_circuit_active_failures WHERE attempt_id=$1::uuid", completion.AttemptID).Scan(&replayActiveEvents); err != nil {
		t.Fatal(err)
	}
	if err = store.ConfigPool().QueryRow(context.Background(), "SELECT event_at FROM backend_circuit_failure_receipts WHERE attempt_id=$1::uuid", completion.AttemptID).Scan(&replayReceiptEvent); err != nil {
		t.Fatal(err)
	}
	if replayEvents != 0 || replayActiveEvents != 0 || !replayReceiptEvent.Equal(originalReceiptEvent) {
		t.Fatalf("replay restored pruned event or changed receipt timestamp: events=%d active_events=%d timestamp=%s, want 0, 0, and %s", replayEvents, replayActiveEvents, replayReceiptEvent, originalReceiptEvent)
	}
}

func openHalfOpenProbe(t *testing.T, store *pgstore.Store, f fixture, options circuitbreaker.Options, probeTTL time.Duration) (*coordpostgres.CircuitCoordinator, coordination.BackendIdentity, coordination.CircuitAcquireRequest, coordination.CircuitDecision) {
	t.Helper()
	coordinator, err := coordpostgres.NewCircuitCoordinator(store, time.Second, options)
	if err != nil {
		t.Fatal(err)
	}
	identity := coordination.BackendIdentity{ID: f.backend.ID, Revision: f.backend.Revision, Enabled: true}
	if err := coordinator.Reconcile(context.Background(), []coordination.BackendIdentity{identity}); err != nil {
		t.Fatal(err)
	}
	failureRequest := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: probeTTL}
	closed, err := coordinator.Acquire(context.Background(), failureRequest)
	if err != nil || closed.Reason != "" {
		t.Fatalf("closed acquire=%+v err=%v", closed, err)
	}
	if _, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: failureRequest.AttemptID, Backend: identity, Generation: closed.Snapshot.Generation,
		Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(options.OpenCooldown + 5*time.Millisecond)
	probeRequest := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: probeTTL}
	probe, err := coordinator.Acquire(context.Background(), probeRequest)
	if err != nil || probe.Permit == nil {
		t.Fatalf("probe acquire=%+v err=%v", probe, err)
	}
	return coordinator, identity, probeRequest, probe
}

func TestPostgresHalfOpenAcquireReplayAtCapacityOneReturnsSamePermit(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Millisecond, HalfOpenMaxProbes: 1}
	first, _, request, initial := openHalfOpenProbe(t, store, f, options, time.Minute)
	second, err := coordpostgres.NewCircuitCoordinator(store, time.Second, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	replayed, err := second.Acquire(context.Background(), request)
	if err != nil || replayed.Permit == nil || replayed.Permit.PermitID != initial.Permit.PermitID || !replayed.Permit.ExpiresAt.Equal(initial.Permit.ExpiresAt) {
		t.Fatalf("replayed probe=%+v err=%v", replayed, err)
	}
	_ = first
}

func TestPostgresExpiredProbeReopensCircuitAndCannotBeRevived(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Millisecond, HalfOpenMaxProbes: 1}
	coordinator, _, request, probe := openHalfOpenProbe(t, store, f, options, 15*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	if err := coordinator.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := coordinator.Snapshot(f.backend.ID, time.Now())
	if snapshot.State != domain.CircuitOpen || snapshot.Generation <= probe.Snapshot.Generation || snapshot.FailureCount != 2 {
		t.Fatalf("snapshot after expired probe=%+v", snapshot)
	}
	if err := coordinator.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	halfOpen := coordinator.Snapshot(f.backend.ID, time.Now())
	if halfOpen.State != domain.CircuitHalfOpen || halfOpen.Generation != snapshot.Generation+1 || halfOpen.FailureCount != snapshot.FailureCount {
		t.Fatalf("snapshot after reopened cooldown=%+v, open snapshot=%+v", halfOpen, snapshot)
	}
	renewed, err := coordinator.RenewProbes(context.Background(), []coordination.ProbeIdentity{*probe.Permit})
	if err != nil || len(renewed) != 1 || renewed[0].Reason != coordination.ReasonLeaseLost {
		t.Fatalf("renew expired probe=%+v err=%v", renewed, err)
	}
	replayed, err := coordinator.Acquire(context.Background(), request)
	if err != nil || replayed.Reason != coordination.ReasonLeaseLost {
		t.Fatalf("replay expired probe=%+v err=%v", replayed, err)
	}
}

func TestPostgresSuccessfulHalfOpenGenerationClearsRetainedFailures(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Millisecond, HalfOpenMaxProbes: 1}
	coordinator, identity, _, probe := openHalfOpenProbe(t, store, f, options, time.Minute)

	closed, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: probe.Permit.PermitID, Backend: identity, Generation: probe.Permit.Generation,
		Outcome: domain.InferenceSuccess, ReportedOutcomeAt: time.Now().UTC(),
	})
	if err != nil || closed.State != domain.CircuitClosed || closed.FailureCount != 0 {
		t.Fatalf("successful probe completion=%+v err=%v", closed, err)
	}
	if err := coordinator.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if closed := coordinator.Snapshot(f.backend.ID, time.Now()); closed.State != domain.CircuitClosed || closed.FailureCount != 0 {
		t.Fatalf("refreshed closed snapshot=%+v", closed)
	}
}

func TestPostgresDelayedHalfOpenFailurePrunesRollingHistory(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: 40 * time.Millisecond, OpenCooldown: time.Millisecond, HalfOpenMaxProbes: 1}
	coordinator, identity, _, probe := openHalfOpenProbe(t, store, f, options, time.Minute)
	time.Sleep(options.FailureWindow + 20*time.Millisecond)

	reopened, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: probe.Permit.PermitID, Backend: identity, Generation: probe.Permit.Generation,
		Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC(),
	})
	if err != nil || reopened.State != domain.CircuitOpen || reopened.FailureCount != 1 {
		t.Fatalf("delayed failed probe completion=%+v err=%v, want only the new rolling failure", reopened, err)
	}
}

func TestPostgresRenewReconcilesExpiredPeerBeforeExtendingProbe(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: 100 * time.Millisecond, HalfOpenMaxProbes: 2}
	coordinator, identity, _, first := openHalfOpenProbe(t, store, f, options, 250*time.Millisecond)
	secondRequest := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Minute}
	second, err := coordinator.Acquire(context.Background(), secondRequest)
	if err != nil || second.Permit == nil {
		t.Fatalf("second probe=%+v err=%v", second, err)
	}
	if delay := time.Until(first.Permit.ExpiresAt) + 20*time.Millisecond; delay > 0 {
		time.Sleep(delay)
	}

	renewed, err := coordinator.RenewProbes(context.Background(), []coordination.ProbeIdentity{*second.Permit})
	if err != nil || len(renewed) != 1 || renewed[0].Reason != coordination.ReasonLeaseLost {
		t.Fatalf("renew after peer expiry=%+v err=%v, want lease_lost", renewed, err)
	}
	rows, err := store.CoordinationPool().Query(context.Background(), "SELECT permit_id,outcome FROM backend_circuit_probes WHERE permit_id=ANY($1::uuid[]) ORDER BY permit_id", []string{first.Permit.PermitID.String(), second.Permit.PermitID.String()})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	outcomes := make(map[uuid.UUID]string)
	for rows.Next() {
		var id uuid.UUID
		var outcome string
		if err := rows.Scan(&id, &outcome); err != nil {
			t.Fatal(err)
		}
		outcomes[id] = outcome
	}
	if outcomes[first.Permit.PermitID] != "expired" || outcomes[second.Permit.PermitID] != "superseded" {
		t.Fatalf("probe outcomes=%v", outcomes)
	}
}

func TestPostgresHalfOpenSuccessThenExpiredPeerAndLateFailureStaysOpen(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Millisecond, HalfOpenMaxProbes: 2}
	coordinator, identity, _, expiring := openHalfOpenProbe(t, store, fixture, options, time.Minute)
	secondRequest := coordination.CircuitAcquireRequest{
		AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Second,
	}
	second, err := coordinator.Acquire(context.Background(), secondRequest)
	if err != nil || second.Permit == nil {
		t.Fatalf("second probe=%+v err=%v", second, err)
	}
	waiting, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: second.Permit.PermitID, Backend: identity, Generation: second.Permit.Generation,
		Outcome: domain.InferenceSuccess, ReportedOutcomeAt: time.Now().UTC(),
	})
	if err != nil || waiting.State != domain.CircuitHalfOpen {
		t.Fatalf("success with peer outstanding=%+v err=%v", waiting, err)
	}
	if _, err := store.CoordinationPool().Exec(
		context.Background(),
		"UPDATE backend_circuit_probes SET expires_at=clock_timestamp()-interval '1 second' WHERE permit_id=$1::uuid AND outcome IS NULL",
		expiring.Permit.PermitID,
	); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	opened := coordinator.Snapshot(fixture.backend.ID, time.Now())
	if opened.State != domain.CircuitOpen || opened.FailureCount != 2 {
		t.Fatalf("expired peer snapshot=%+v", opened)
	}
	late, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: expiring.Permit.PermitID, Backend: identity, Generation: expiring.Permit.Generation,
		Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC(),
	})
	if err != nil || late.State != domain.CircuitOpen || late.FailureCount != opened.FailureCount {
		t.Fatalf("late failure=%+v err=%v, opened=%+v", late, err, opened)
	}
}

func TestPostgresHalfOpenFailureSupersedesCrashedPeer(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: 100 * time.Millisecond, HalfOpenMaxProbes: 2}
	coordinator, identity, _, crashed := openHalfOpenProbe(t, store, fixture, options, time.Minute)
	failingRequest := coordination.CircuitAcquireRequest{
		AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Minute,
	}
	failing, err := coordinator.Acquire(context.Background(), failingRequest)
	if err != nil || failing.Permit == nil {
		t.Fatalf("failing probe=%+v err=%v", failing, err)
	}
	opened, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: failing.Permit.PermitID, Backend: identity, Generation: failing.Permit.Generation,
		Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC(),
	})
	if err != nil || opened.State != domain.CircuitOpen {
		t.Fatalf("failed probe=%+v err=%v", opened, err)
	}
	var outcome string
	if err := store.CoordinationPool().QueryRow(context.Background(), "SELECT outcome FROM backend_circuit_probes WHERE permit_id=$1::uuid", crashed.Permit.PermitID).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "superseded" {
		t.Fatalf("crashed peer outcome=%q", outcome)
	}
	late, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: crashed.Permit.PermitID, Backend: identity, Generation: crashed.Permit.Generation,
		Outcome: domain.InferenceSuccess, ReportedOutcomeAt: time.Now().UTC(),
	})
	if err != nil || late.State != domain.CircuitOpen || late.Generation != opened.Generation {
		t.Fatalf("late superseded success=%+v err=%v", late, err)
	}
}

func TestPostgresBackendRevisionSupersedesUnfinishedProbe(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Millisecond, HalfOpenMaxProbes: 1}
	coordinator, identity, request, probe := openHalfOpenProbe(t, store, f, options, time.Minute)
	updated, err := store.UpdateBackend(context.Background(), f.backend.ID, basestore.UpdateBackendParams{
		ModelPoolID: f.backend.ModelPoolID, Name: f.backend.Name, BaseURL: f.backend.BaseURL,
		Enabled: f.backend.Enabled, Draining: f.backend.Draining, CapacityHint: f.backend.CapacityHint,
		RunningSoftLimit: f.backend.RunningSoftLimit, UpstreamAPIKeyEnv: f.backend.UpstreamAPIKeyEnv,
	})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := coordinator.Acquire(context.Background(), request)
	if err != nil || replayed.Reason != coordination.ReasonLeaseLost {
		t.Fatalf("replay after backend revision=%+v err=%v, want lease_lost", replayed, err)
	}
	conflict := request
	conflict.ProbeTTL += time.Second
	conflicted, err := coordinator.Acquire(context.Background(), conflict)
	if err != nil || conflicted.Reason != coordination.ReasonIdempotencyConflict {
		t.Fatalf("conflicting replay after backend revision=%+v err=%v, want idempotency_conflict", conflicted, err)
	}
	if _, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: probe.Permit.PermitID, Backend: identity, Generation: probe.Permit.Generation,
		Outcome: domain.InferenceSuccess, ReportedOutcomeAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := coordinator.Snapshot(updated.ID, time.Now())
	if snapshot.Backend.Revision != updated.Revision || snapshot.State != domain.CircuitClosed {
		t.Fatalf("snapshot after backend supersession=%+v updated=%+v", snapshot, updated)
	}
	var outcome string
	if err := store.CoordinationPool().QueryRow(context.Background(), "SELECT outcome FROM backend_circuit_probes WHERE permit_id=$1::uuid", probe.Permit.PermitID).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "superseded" {
		t.Fatalf("probe outcome=%q", outcome)
	}
}

func TestPostgresStaleReconcileCannotUndoAuthoritativeBackendDrain(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	coordinator, err := coordpostgres.NewCircuitCoordinator(store, time.Second, circuitbreaker.Options{
		FailureThreshold: 2, FailureWindow: time.Minute, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	stale := coordination.BackendIdentity{ID: fixture.backend.ID, Revision: fixture.backend.Revision, Enabled: true}
	if err = store.SetBackendDraining(ctx, fixture.backend.ID, true); err != nil {
		t.Fatal(err)
	}
	if err = coordinator.Reconcile(ctx, []coordination.BackendIdentity{stale}); err != nil {
		t.Fatal(err)
	}

	for _, identity := range []coordination.BackendIdentity{
		stale,
		{ID: fixture.backend.ID, Revision: fixture.backend.Revision + 1, Enabled: true},
	} {
		decision, acquireErr := coordinator.Acquire(ctx, coordination.CircuitAcquireRequest{
			AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Minute,
		})
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		if decision.Reason != coordination.ReasonStaleBackend {
			t.Fatalf("Acquire(%+v) = %+v, want stale_backend for authoritative drain", identity, decision)
		}
	}
}

func TestPostgresRejectsIncompatibleLiveReplica(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	base := coordpostgres.ReplicaPolicy{LeaseTTL: time.Minute, LeaseRenewInterval: 10 * time.Second, Circuit: circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Second, HalfOpenMaxProbes: 1}}
	first := coordpostgres.NewReplicaManager(store, uuid.New(), base, time.Second)
	if err := first.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	changed := base
	changed.Circuit.HalfOpenMaxProbes = 2
	second := coordpostgres.NewReplicaManager(store, uuid.New(), changed, time.Second)
	if err := second.Start(context.Background()); err == nil {
		second.Close()
		t.Fatal("incompatible live replica was accepted")
	}
}

func TestPostgresConcurrentIncompatibleReplicaRegistration(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	ctx := context.Background()
	barrier, err := store.ConfigPool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Rollback(ctx)
	if _, err = barrier.Exec(ctx, "LOCK TABLE coordination_replicas IN SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	policy := coordpostgres.ReplicaPolicy{
		LeaseTTL: time.Minute, LeaseRenewInterval: 20 * time.Second,
		Circuit: circuitbreaker.Options{FailureThreshold: 2, FailureWindow: time.Minute, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1},
	}
	firstManager := coordpostgres.NewReplicaManager(store, uuid.New(), policy, 5*time.Second)
	policy.Circuit.FailureThreshold = 3
	secondManager := coordpostgres.NewReplicaManager(store, uuid.New(), policy, 5*time.Second)
	results := make(chan error, 2)
	go func() { results <- firstManager.Start(ctx) }()
	go func() { results <- secondManager.Start(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	waiting := 0
	for waiting < 2 && time.Now().Before(deadline) {
		if err = store.ConfigPool().QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'INSERT INTO coordination_replicas%'").Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err = barrier.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	firstErr, secondErr := <-results, <-results
	defer firstManager.Close()
	defer secondManager.Close()
	if waiting != 2 {
		t.Fatalf("could not establish registration barrier; waiting=%d", waiting)
	}
	if firstErr == nil && secondErr == nil {
		t.Fatal("both incompatible replicas registered successfully")
	}
}

func TestPostgresRejectsOlderCoordinationContract(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	current := coordpostgres.ReplicaPolicy{LeaseTTL: time.Minute, LeaseRenewInterval: 10 * time.Second, Circuit: circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Second, HalfOpenMaxProbes: 1}}
	first := coordpostgres.NewReplicaManager(store, uuid.New(), current, time.Second)
	if err := first.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	older := current
	older.ContractVersion = 1
	second := coordpostgres.NewReplicaManager(store, uuid.New(), older, time.Second)
	if err := second.Start(context.Background()); err == nil {
		second.Close()
		t.Fatal("older coordination contract was accepted alongside version 2")
	}
}

func TestPostgresTransactionPoolerRuntime(t *testing.T) {
	pooler := os.Getenv("LLMGW_POSTGRES_POOLER_TEST_DSN")
	if pooler == "" {
		t.Skip("LLMGW_POSTGRES_POOLER_TEST_DSN is not set")
	}
	store := openTestStore(t, pooler)
	f := createFixture(t, store, 0)
	if _, err := store.LoadSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	decision, err := coordinator.Acquire(context.Background(), admissionRequest(f, uuid.New()))
	if err != nil || !decision.Admitted() {
		t.Fatalf("pooler coordination=%+v err=%v", decision, err)
	}
	if _, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: decision.Lease.Identity()}}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.InsertUsageBatch(context.Background(), []analytics.RequestRecord{{
		OccurredAt: now, RequestID: uuid.NewString(), ClientID: f.client.ID, ClientName: f.client.Name,
		ModelPoolID: f.pool.ID, ModelName: f.pool.PublicModelName, BackendName: f.backend.Name, HTTPStatus: 200,
	}}); err != nil {
		t.Fatal(err)
	}
	if page, err := store.UsageRequests(context.Background(), analytics.Filter{From: now.Add(-time.Minute), To: now.Add(time.Minute)}, 10, 0); err != nil || page.Total != 1 {
		t.Fatalf("pooler analytics page=%+v err=%v", page, err)
	}
}

func TestPostgresTransactionPoolerReassignsServerConnections(t *testing.T) {
	pooler := os.Getenv("LLMGW_POSTGRES_POOLER_TEST_DSN")
	if pooler == "" {
		t.Skip("LLMGW_POSTGRES_POOLER_TEST_DSN is not set")
	}
	_ = openTestStore(t, pooler)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	config, err := pgx.ParseConfig(pooler)
	if err != nil {
		t.Fatal(err)
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeExec
	first, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	blocker, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close(context.Background())

	for attempt := 0; attempt < 128; attempt++ {
		firstTx, err := first.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var releasedPID int32
		if err := firstTx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&releasedPID); err != nil {
			_ = firstTx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := firstTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		blockerTx, err := blocker.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var blockedPID int32
		if err := blockerTx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blockedPID); err != nil {
			_ = blockerTx.Rollback(ctx)
			t.Fatal(err)
		}
		if blockedPID != releasedPID {
			_ = blockerTx.Rollback(ctx)
			continue
		}

		// blockerTx now owns the exact server connection previously used by
		// first. A new transaction on first must be assigned another server.
		reassignedTx, err := first.Begin(ctx)
		if err != nil {
			_ = blockerTx.Rollback(context.Background())
			t.Fatal(err)
		}
		var reassignedPID int32
		if err := reassignedTx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&reassignedPID); err != nil {
			_ = reassignedTx.Rollback(context.Background())
			_ = blockerTx.Rollback(context.Background())
			t.Fatal(err)
		}
		_ = reassignedTx.Rollback(context.Background())
		_ = blockerTx.Rollback(context.Background())
		if reassignedPID == releasedPID {
			t.Fatalf("pooler assigned concurrently held backend PID %d", releasedPID)
		}
		return
	}
	t.Fatal("pooler did not reassign a released server connection to another client; verify transaction pooling and pool size >= 2")
}

func TestPostgresOutageRejectsNewDistributedAdmission(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, 100*time.Millisecond)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Acquire(context.Background(), admissionRequest(fixture, uuid.New())); err == nil {
		t.Fatal("new distributed admission succeeded after all PostgreSQL pools closed")
	}
	if coordinator.Status().Available {
		t.Fatalf("status=%+v", coordinator.Status())
	}
}

func TestPostgresHALosslessEndpointPreservesAcknowledgedHistory(t *testing.T) {
	runtimeURL := os.Getenv("LLMGW_POSTGRES_HA_TEST_DSN")
	if runtimeURL == "" {
		t.Skip("LLMGW_POSTGRES_HA_TEST_DSN is not set")
	}
	firstStore := openTestStore(t, runtimeURL)
	fixture := createFixture(t, firstStore, 0)
	coordinator := coordpostgres.NewAdmissionCoordinator(firstStore, time.Second)
	request := admissionRequest(fixture, uuid.New())
	decision, err := coordinator.Acquire(context.Background(), request)
	if err != nil || !decision.Admitted() {
		t.Fatalf("acknowledged acquire=%+v err=%v", decision, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	migrationURL := os.Getenv("LLMGW_POSTGRES_HA_MIGRATION_DSN")
	if migrationURL == "" {
		migrationURL = runtimeURL
	}
	secondStore, err := pgstore.Open(ctx, pgstore.Options{DatabaseURL: runtimeURL, MigrationURL: migrationURL, ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()
	if revision, err := secondStore.CurrentRevision(ctx); err != nil || revision < fixture.revision {
		t.Fatalf("revision after endpoint transition=%d err=%v, acknowledged=%d", revision, err, fixture.revision)
	}
	replayed, err := coordpostgres.NewAdmissionCoordinator(secondStore, time.Second).Acquire(ctx, request)
	if err != nil || !replayed.Admitted() || replayed.Lease.LeaseID != decision.Lease.LeaseID {
		t.Fatalf("acknowledged lease replay=%+v err=%v", replayed, err)
	}
}

func TestPostgresRegressedHistoryTriggersPermanentRevisionFault(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	guard := &registry.RevisionGuard{}
	guard.ObservePublished(fixture.revision)
	if _, err := store.ConfigPool().Exec(context.Background(), "UPDATE config_meta SET revision=$1::bigint WHERE singleton=1", fixture.revision-1); err != nil {
		t.Fatal(err)
	}
	fresh, err := store.CurrentRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.VerifyFresh(fixture.revision, fresh); err == nil || !guard.Faulted() {
		t.Fatalf("VerifyFresh()=%v faulted=%v", err, guard.Faulted())
	}
	if err := guard.VerifyFresh(fixture.revision, fixture.revision+1); err == nil || !guard.Faulted() {
		t.Fatalf("permanent fault unexpectedly cleared: err=%v faulted=%v", err, guard.Faulted())
	}
}
