package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/config"
)

func TestGatewayWaitsForOldRegistrationExpiry(t *testing.T) {
	ctx, db, env := coordinationResetFixture(t)
	old := seedOldCoordinationReplica(t, ctx, db)
	if _, err := db.CoordinationPool().Exec(ctx, "UPDATE coordination_replicas SET heartbeat_expires_at=clock_timestamp()+interval '200 milliseconds' WHERE replica_id=$1", old); err != nil {
		t.Fatal(err)
	}
	delete(env, "LLMGW_COORDINATION_STARTUP_WAIT_TIMEOUT")
	// Default waiting must admit the new process once the old registration expires.
	cfg, err := config.Load(mapLookup(env))
	if err != nil {
		t.Fatal(err)
	}
	app, err := newGatewayApplication(ctx, cfg, mapLookup(env), io.Discard)
	if err != nil {
		t.Fatalf("startup should wait for expiry: %v", err)
	}
	closeResetTestApplication(t, app)
	if !app.replica.Status().Available || !app.replica.Compatible() {
		t.Fatal("startup conflict poisoned readiness")
	}
	response := httptest.NewRecorder()
	app.handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/coordination-readyz", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("coordination not ready after startup wait: %s", response.Body.String())
	}
	var exists bool
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM coordination_replicas WHERE replica_id=$1)", old).Scan(&exists); err != nil || !exists {
		t.Fatalf("waiting deleted old row: exists=%v err=%v", exists, err)
	}
}

func TestGatewayStartupWaitHonorsDeadline(t *testing.T) {
	ctx, db, env := coordinationResetFixture(t)
	seedOldCoordinationReplica(t, ctx, db)
	env["LLMGW_COORDINATION_STARTUP_WAIT_TIMEOUT"] = "100ms"
	cfg, err := config.Load(mapLookup(env))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = newGatewayApplication(ctx, cfg, mapLookup(env), io.Discard)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup error=%v, want deadline", err)
	}
	if time.Since(started) > 3*time.Second {
		t.Fatal("startup deadline not bounded")
	}
}

func TestGatewayGracefulCloseUnregistersOnlySelf(t *testing.T) {
	ctx, db, env := coordinationResetFixture(t)
	cfg, err := config.Load(mapLookup(env))
	if err != nil {
		t.Fatal(err)
	}
	app, err := newGatewayApplication(ctx, cfg, mapLookup(env), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	closeResetTestApplication(t, app)
	peer, err := newGatewayApplication(ctx, cfg, mapLookup(env), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	closeResetTestApplication(t, peer)
	if err := app.Close(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM coordination_replicas").Scan(&count); err != nil || count != 1 {
		t.Fatalf("registrations after close=%d want 1, err=%v", count, err)
	}
	if err := peer.replica.Check(ctx); err != nil {
		t.Fatalf("peer registration lost: %v", err)
	}
}
