package main

import (
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/config"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

func TestGatewayApplicationKeepsCoordinationAliveDuringHTTPDrain(t *testing.T) {
	dsn := isolatedApplicationPostgresURL(t)
	environment := validEnvironment("")
	delete(environment, "LLMGW_DATABASE_PATH")
	environment["LLMGW_DATABASE_DRIVER"] = "postgres"
	environment["LLMGW_DATABASE_URL"] = dsn
	environment["LLMGW_LEASE_TTL"] = "600ms"
	environment["LLMGW_LEASE_RENEW_INTERVAL"] = "50ms"
	environment["LLMGW_COORDINATION_TIMEOUT"] = "500ms"
	environment["LLMGW_SHUTDOWN_GRACE_PERIOD"] = "2s"
	cfg, err := config.Load(mapLookup(environment))
	if err != nil {
		t.Fatal(err)
	}
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer startupCancel()
	database, err := pgstore.Open(startupCtx, pgstore.Options{
		DatabaseURL: dsn, MigrationURL: dsn,
		ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	pool, err := database.CreatePool(startupCtx, store.CreatePoolParams{
		PublicModelName: "shutdown-model", UpstreamModelName: "upstream-model", Enabled: true, MaxGatewayInflight: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := database.CreateClient(startupCtx, store.CreateClientParams{
		Name: "draining-client", Enabled: true, PriorityClass: domain.PriorityHigh,
		VLLMPriority: -10, MaxConcurrency: 1, ModelPoolIDs: []int64{pool.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := apikey.Generate(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key, err := database.CreateAPIKey(startupCtx, store.CreateAPIKeyParams{
		ClientID: client.ID, Prefix: plain.Prefix, SecretHash: apikey.Digest(cfg.APIKeyHMACSecret, plain.Value),
	})
	if err != nil {
		t.Fatal(err)
	}
	revision, err := database.CurrentRevision(startupCtx)
	if err != nil {
		t.Fatal(err)
	}
	signalCtx, signalCancel := context.WithCancel(context.Background())
	defer signalCancel()
	application, err := newGatewayApplication(signalCtx, cfg, mapLookup(environment), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
		defer cancel()
		if err := application.Close(closeCtx); err != nil {
			t.Errorf("close application: %v", err)
		}
	})
	for _, endpoint := range []string{"/coordination-readyz", "/v1/load?model=shutdown-model"} {
		request := httptest.NewRequest(http.MethodGet, endpoint, nil)
		request.Header.Set("Authorization", "Bearer "+plain.Value)
		response := httptest.NewRecorder()
		application.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s returned %d: %s", endpoint, response.Code, response.Body.String())
		}
	}
	coordinator := coordpostgres.NewAdmissionCoordinator(database, cfg.CoordinationTimeout)
	decision, err := coordinator.Acquire(startupCtx, coordination.AdmissionRequest{
		LeaseID: uuid.New(), OperationStartedAt: time.Now().UTC(), RequestID: uuid.NewString(), ReplicaID: uuid.New(),
		ConfigurationRevision: revision, APIKeyID: key.ID, ClientID: client.ID, PoolID: pool.ID,
		ClientPolicyRevision: client.Revision, EffectiveClientLimit: 1, ConfiguredClientLimit: 1,
		PoolGatewayInflightLimit: 1, LeaseTTL: cfg.LeaseTTL,
	})
	if err != nil || !decision.Admitted() {
		t.Fatalf("acquire draining lease = %+v, error = %v", decision, err)
	}
	handle := application.leases.Track(decision.Lease.Identity())
	readExpiry := func() time.Time {
		t.Helper()
		var expiry time.Time
		if err := database.CoordinationPool().QueryRow(startupCtx,
			"SELECT expires_at FROM request_leases WHERE lease_id=$1", decision.Lease.LeaseID).Scan(&expiry); err != nil {
			t.Fatal(err)
		}
		return expiry
	}
	waitForGateway(t, time.Second, func() bool { return readExpiry().After(decision.Lease.ExpiresAt) })
	signalCancel()
	// HTTP shutdown drains active requests before application.Close. Renewal and
	// runtime workers must remain alive throughout that interval.
	select {
	case <-application.runtimeDone:
		t.Fatal("runtime coordination stopped on the shutdown signal before HTTP drain completed")
	case <-time.After(3 * cfg.LeaseRenewInterval):
	}
	expiryDuringDrain := readExpiry()
	waitForGateway(t, time.Second, func() bool { return readExpiry().After(expiryDuringDrain) })
	if !handle.Complete(nil) {
		t.Fatal("draining request completion was dropped")
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
	defer shutdownCancel()
	if err := application.Close(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	var completed bool
	var completionResult string
	var remainingLeases int
	if err := database.CoordinationPool().QueryRow(startupCtx, `SELECT completed_at IS NOT NULL,
		completion_result, (SELECT count(*) FROM request_leases WHERE lease_id=$1)
		FROM admission_operations WHERE lease_id=$1`, decision.Lease.LeaseID).
		Scan(&completed, &completionResult, &remainingLeases); err != nil {
		t.Fatal(err)
	}
	if !completed || completionResult != string(coordination.CompletionReleased) || remainingLeases != 0 {
		t.Fatalf("shutdown completion: completed=%t result=%q remaining leases=%d", completed, completionResult, remainingLeases)
	}
	if pending := application.leases.PendingCompletions(); pending != 0 {
		t.Fatalf("shutdown left %d pending completions", pending)
	}
	select {
	case <-application.runtimeDone:
	default:
		t.Fatal("runtime coordination remained alive after application.Close")
	}
}

// Every application test owns its schema; public tables and other test workers
// are unaffected by migrations, coordination state, and cleanup.
func isolatedApplicationPostgresURL(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("parse PostgreSQL test URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "application_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		_ = conn.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		defer conn.Close(cleanupCtx)
		if _, err := conn.Exec(cleanupCtx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("drop application test schema: %v", err)
		}
	})
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
