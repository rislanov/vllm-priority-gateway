package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/config"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

func coordinationResetFixture(t *testing.T) (context.Context, *pgstore.Store, map[string]string) {
	t.Helper()
	dsn := isolatedApplicationPostgresURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	db, err := pgstore.Open(ctx, pgstore.Options{
		DatabaseURL: dsn, ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	env := validEnvironment("")
	delete(env, "LLMGW_DATABASE_PATH")
	env["LLMGW_DATABASE_DRIVER"] = "postgres"
	env["LLMGW_DATABASE_URL"] = dsn
	env["LLMGW_COORDINATION_TIMEOUT"] = "5s"
	env["LLMGW_COORDINATION_STARTUP_WAIT_TIMEOUT"] = "0"
	return ctx, db, env
}

func seedOldCoordinationReplica(t *testing.T, ctx context.Context, db *pgstore.Store) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.CoordinationPool().Exec(ctx, `INSERT INTO coordination_replicas
		(replica_id,started_at,heartbeat_at,heartbeat_expires_at,binary_version,coordination_contract_version,policy_fingerprint)
		VALUES ($1,clock_timestamp(),clock_timestamp(),clock_timestamp()+interval '1 hour','old',3,$2)`, id, bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func closeResetTestApplication(t *testing.T, app *gatewayApplication) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.Close(ctx); err != nil {
			t.Errorf("close application: %v", err)
		}
	})
}

func TestGatewayCoordinationResetOnStart(t *testing.T) {
	ctx, db, env := coordinationResetFixture(t)
	old := seedOldCoordinationReplica(t, ctx, db)
	cfg, err := config.Load(mapLookup(env))
	if err != nil {
		t.Fatal(err)
	}
	app, err := newGatewayApplication(ctx, cfg, mapLookup(env), io.Discard)
	if err == nil {
		closeResetTestApplication(t, app)
		t.Fatal("default startup accepted incompatible replica")
	}
	var reason coordination.ReasonError
	if !errors.As(err, &reason) || reason.Reason != coordination.ReasonIdempotencyConflict {
		t.Fatalf("default startup error = %v", err)
	}

	// Retained lease and receipt must survive a registration reset.
	leaseID := uuid.New()
	_, err = db.CoordinationPool().Exec(ctx, `INSERT INTO admission_operations
		(lease_id,operation_started_at,input_fingerprint,request_id,replica_id,client_id,pool_id,decision,decided_at,retain_until)
		VALUES ($1,clock_timestamp(),$2,'reset-sentinel',$3,999,999,'admitted',clock_timestamp(),clock_timestamp()+interval '24 hours')`,
		leaseID, bytes.Repeat([]byte{2}, 32), old)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.CoordinationPool().Exec(ctx, `INSERT INTO request_leases
		(lease_id,request_id,replica_id,client_id,pool_id,acquired_at,expires_at,priority_class)
		VALUES ($1,'reset-sentinel',$2,999,999,clock_timestamp(),clock_timestamp()+interval '1 hour','normal')`, leaseID, old)
	if err != nil {
		t.Fatal(err)
	}

	env["LLMGW_COORDINATION_RESET_ON_START"] = "true"
	cfg, err = config.Load(mapLookup(env))
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	app, err = newGatewayApplication(ctx, cfg, mapLookup(env), &log)
	if err != nil {
		t.Fatalf("reset startup: %v", err)
	}
	closeResetTestApplication(t, app)
	if !strings.Contains(log.String(), "LLMGW_COORDINATION_RESET_ON_START") || !strings.Contains(log.String(), "WARN") {
		t.Fatalf("reset did not emit a warning: %s", log.String())
	}
	var oldExists, leaseExists, receiptExists bool
	err = db.CoordinationPool().QueryRow(ctx, `SELECT
		EXISTS(SELECT 1 FROM coordination_replicas WHERE replica_id=$1),
		EXISTS(SELECT 1 FROM request_leases WHERE lease_id=$2),
		EXISTS(SELECT 1 FROM admission_operations WHERE lease_id=$2)`, old, leaseID).
		Scan(&oldExists, &leaseExists, &receiptExists)
	if err != nil || oldExists || !leaseExists || !receiptExists {
		t.Fatalf("reset state: old=%t lease=%t receipt=%t err=%v", oldExists, leaseExists, receiptExists, err)
	}

	// A second reset and a normal start must preserve matching peers. The toggle
	// itself must not change the policy fingerprint.
	for _, reset := range []string{"true", "false"} {
		peerEnv := maps.Clone(env)
		peerEnv["LLMGW_COORDINATION_RESET_ON_START"] = reset
		peerCfg, err := config.Load(mapLookup(peerEnv))
		if err != nil {
			t.Fatal(err)
		}
		peer, err := newGatewayApplication(ctx, peerCfg, mapLookup(peerEnv), io.Discard)
		if err != nil {
			t.Fatalf("matching peer reset=%s: %v", reset, err)
		}
		closeResetTestApplication(t, peer)
		if err := app.replica.Check(ctx); err != nil {
			t.Fatalf("matching peer erased original registration: %v", err)
		}
	}
	var count int
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM coordination_replicas").Scan(&count); err != nil || count != 3 {
		t.Fatalf("matching registrations=%d err=%v", count, err)
	}
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/coordination-readyz", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("reset readiness=%d body=%s", response.Code, response.Body.String())
	}

	// A later recovery/heartbeat must enforce compatibility, never repeat reset.
	later := seedOldCoordinationReplica(t, ctx, db)
	if err := app.replica.Check(ctx); !errors.As(err, &reason) || reason.Reason != coordination.ReasonIdempotencyConflict {
		t.Fatalf("heartbeat bypassed incompatibility: %v", err)
	}
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM coordination_replicas WHERE replica_id=$1)", later).Scan(&oldExists); err != nil || !oldExists {
		t.Fatalf("heartbeat removed conflicting replica: exists=%t err=%v", oldExists, err)
	}
}

func TestGatewayCoordinationResetConcurrentStarts(t *testing.T) {
	ctx, db, env := coordinationResetFixture(t)
	seedOldCoordinationReplica(t, ctx, db)
	env["LLMGW_COORDINATION_RESET_ON_START"] = "true"
	cfg, err := config.Load(mapLookup(env))
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		app *gatewayApplication
		err error
	}
	const replicas = 4
	results := make(chan result, replicas)
	start := make(chan struct{})
	for i := 0; i < replicas; i++ {
		go func() {
			<-start
			app, err := newGatewayApplication(ctx, cfg, mapLookup(env), io.Discard)
			results <- result{app, err}
		}()
	}
	close(start)
	var apps []*gatewayApplication
	for i := 0; i < replicas; i++ {
		r := <-results
		if r.err != nil {
			t.Errorf("concurrent reset startup: %v", r.err)
		} else {
			closeResetTestApplication(t, r.app)
			apps = append(apps, r.app)
		}
	}
	for _, app := range apps {
		if err := app.replica.Check(ctx); err != nil {
			t.Errorf("concurrent reset erased a new replica: %v", err)
		}
	}
	var count int
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM coordination_replicas").Scan(&count); err != nil || count != replicas {
		t.Fatalf("registrations=%d want=%d err=%v", count, replicas, err)
	}
}

func TestGatewayCoordinationResetRollsBackOnRegistrationFailure(t *testing.T) {
	ctx, db, env := coordinationResetFixture(t)
	old := seedOldCoordinationReplica(t, ctx, db)
	_, err := db.CoordinationPool().Exec(ctx, `CREATE FUNCTION reject_replica_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'registration failure sentinel'; END $$;
		CREATE TRIGGER reject_replica_insert BEFORE INSERT ON coordination_replicas
		FOR EACH ROW EXECUTE FUNCTION reject_replica_insert()`)
	if err != nil {
		t.Fatal(err)
	}
	env["LLMGW_COORDINATION_RESET_ON_START"] = "true"
	cfg, err := config.Load(mapLookup(env))
	if err != nil {
		t.Fatal(err)
	}
	app, err := newGatewayApplication(ctx, cfg, mapLookup(env), io.Discard)
	if err == nil {
		closeResetTestApplication(t, app)
		t.Fatal("registration unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "registration failure sentinel") {
		t.Fatalf("did not reach registration after reset: %v", err)
	}
	var exists bool
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM coordination_replicas WHERE replica_id=$1)", old).Scan(&exists); err != nil || !exists {
		t.Fatalf("failed registration did not roll back reset: exists=%t err=%v", exists, err)
	}
}
