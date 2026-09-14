package postgres_test

import (
	"context"
	"crypto/sha256"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
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
	_, err := store.ConfigPool().Exec(ctx, `TRUNCATE TABLE backend_circuit_failures,backend_circuit_failure_receipts,backend_circuit_probes,backend_circuit_state,coordination_rate_state,request_leases,admission_operations,coordination_replicas,client_admission_scopes,pool_admission_scopes,usage_requests,client_model_access,api_keys,backends,clients,model_pools RESTART IDENTITY CASCADE; UPDATE config_meta SET revision=0 WHERE singleton=1`)
	if err != nil {
		t.Fatalf("reset dedicated PostgreSQL test database: %v", err)
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

func TestPostgresCircuitCapacityIsSharedAcrossCoordinators(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: 20 * time.Millisecond, HalfOpenMaxProbes: 1}
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
	time.Sleep(25 * time.Millisecond)
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
	if snapshot.State != domain.CircuitOpen || snapshot.Generation <= probe.Snapshot.Generation {
		t.Fatalf("snapshot after expired probe=%+v", snapshot)
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

func TestPostgresBackendRevisionSupersedesUnfinishedProbe(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Millisecond, HalfOpenMaxProbes: 1}
	coordinator, identity, _, probe := openHalfOpenProbe(t, store, f, options, time.Minute)
	updated, err := store.UpdateBackend(context.Background(), f.backend.ID, basestore.UpdateBackendParams{
		ModelPoolID: f.backend.ModelPoolID, Name: f.backend.Name, BaseURL: f.backend.BaseURL,
		Enabled: f.backend.Enabled, Draining: f.backend.Draining, CapacityHint: f.backend.CapacityHint,
		RunningSoftLimit: f.backend.RunningSoftLimit, UpstreamAPIKeyEnv: f.backend.UpstreamAPIKeyEnv,
	})
	if err != nil {
		t.Fatal(err)
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
