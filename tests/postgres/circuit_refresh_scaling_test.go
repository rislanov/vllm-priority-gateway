package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

func TestPostgresRefreshManyHalfOpenCircuitsWithNetworkLatency(t *testing.T) {
	for _, scenario := range []string{"idle_half_open", "cooldown_elapsed", "expired_probes"} {
		t.Run(scenario, func(t *testing.T) { testRefreshManyCircuits(t, scenario) })
	}
}

func testRefreshManyCircuits(t *testing.T, scenario string) {
	raw := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	store := openTestStore(t, raw)
	ctx := context.Background()
	// Circuit state has no configuration foreign key: these are independent
	// existing identities, all waiting for their next real requesting probe.
	_, err := store.ConfigPool().Exec(ctx, `INSERT INTO backend_circuit_state
		(backend_id,backend_revision,state,generation,half_open_succeeded,updated_at)
		SELECT i,1,'half_open',1,false,clock_timestamp() FROM generate_series(1,100) i`)
	if err != nil {
		t.Fatal(err)
	}
	wantState := domain.CircuitHalfOpen
	wantGeneration := int64(1)
	if scenario == "cooldown_elapsed" {
		_, err = store.ConfigPool().Exec(ctx, "UPDATE backend_circuit_state SET state='open',opened_at=clock_timestamp()-interval '2 hours'")
		wantGeneration = 2
	}
	if scenario == "expired_probes" {
		_, err = store.ConfigPool().Exec(ctx, `INSERT INTO backend_circuit_probes
			(permit_id,acquisition_started_at,input_fingerprint,backend_id,backend_revision,circuit_generation,replica_id,expires_at)
			SELECT md5((i*10+p)::text)::uuid,clock_timestamp()-interval '1 minute',decode('00','hex'),i,1,1,
			md5('replica')::uuid,clock_timestamp()+CASE WHEN p=1 THEN interval '-1 second' ELSE interval '1 minute' END
			FROM generate_series(1,100) i CROSS JOIN generate_series(1,2) p`)
		wantState, wantGeneration = domain.CircuitOpen, 2
	}
	if err != nil {
		t.Fatal(err)
	}
	delayed, err := pgstore.Open(ctx, pgstore.Options{DatabaseURL: delayedPostgresDSN(t, raw, 2*time.Millisecond), MigrationURL: raw, ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = delayed.Close() })
	c, err := coordpostgres.NewCircuitCoordinator(delayed, 500*time.Millisecond, circuitbreaker.Options{FailureThreshold: 3, FailureWindow: time.Minute, OpenCooldown: time.Hour, HalfOpenMaxProbes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := c.Refresh(ctx); err != nil {
			t.Fatalf("refresh 100 half-open circuits: %v", err)
		}
		for id := int64(1); id <= 100; id++ {
			snapshot := c.Snapshot(id, time.Now())
			if snapshot.State != wantState || snapshot.Generation != wantGeneration || snapshot.Available != (wantState == domain.CircuitHalfOpen) {
				t.Fatalf("backend %d: %+v", id, snapshot)
			}
			if scenario == "expired_probes" && (snapshot.FailureCount != 1 || snapshot.ProbesInFlight != 0) {
				t.Fatalf("expiry state %d: %+v", id, snapshot)
			}
		}
	}
	if scenario == "expired_probes" {
		var expired, superseded, unfinished int
		if err := store.ConfigPool().QueryRow(ctx, `SELECT count(*) FILTER(WHERE outcome='expired'),count(*) FILTER(WHERE outcome='superseded'),count(*) FILTER(WHERE outcome IS NULL) FROM backend_circuit_probes`).Scan(&expired, &superseded, &unfinished); err != nil {
			t.Fatal(err)
		}
		if expired != 100 || superseded != 100 || unfinished != 0 {
			t.Fatalf("expired=%d superseded=%d unfinished=%d", expired, superseded, unfinished)
		}
	}
}
