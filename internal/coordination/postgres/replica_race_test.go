package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

func openReplicaRaceStore(t *testing.T) *pgstore.Store {
	t.Helper()
	dsn := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := pgstore.Open(ctx, pgstore.Options{
		DatabaseURL: dsn, ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CoordinationPool().Exec(ctx, "TRUNCATE coordination_replicas"); err != nil {
		t.Fatal(err)
	}
	return store
}

func replicaRacePolicy(ttl, renew time.Duration) ReplicaPolicy {
	return ReplicaPolicy{
		LeaseTTL: ttl, LeaseRenewInterval: renew,
		Circuit: circuitbreaker.Options{
			FailureThreshold: 5, FailureWindow: 30 * time.Second,
			OpenCooldown: 15 * time.Second, HalfOpenMaxProbes: 1,
		},
	}
}

func TestExpiredReplicaHeartbeatCannotRaceIncompatibleRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := openReplicaRaceStore(t)
	a := NewReplicaManager(store, uuid.New(), replicaRacePolicy(90*time.Second, 30*time.Second), 10*time.Second)
	b := NewReplicaManager(store, uuid.New(), replicaRacePolicy(100*time.Second, 30*time.Second), 10*time.Second)
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	if _, err := store.CoordinationPool().Exec(ctx, `UPDATE coordination_replicas
		SET heartbeat_at=clock_timestamp()-interval '1 hour', heartbeat_expires_at=clock_timestamp()-interval '1 hour'
		WHERE replica_id=$1`, a.replicaID); err != nil {
		t.Fatal(err)
	}

	blocker, err := store.CoordinationPool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Rollback(context.Background()) })
	if _, err := blocker.Exec(ctx, "SELECT replica_id FROM coordination_replicas WHERE replica_id=$1 FOR UPDATE", a.replicaID); err != nil {
		t.Fatal(err)
	}
	resumed := make(chan error, 1)
	go func() { resumed <- a.Check(ctx) }()
	for {
		var blocked bool
		if err := store.CoordinationPool().QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM pg_stat_activity
			WHERE datname=current_database() AND wait_event_type='Lock'
			AND query LIKE 'UPDATE coordination_replicas SET heartbeat_at%')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case err := <-resumed:
			t.Fatalf("heartbeat ended before update lock: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-resumed; err == nil {
		t.Fatal("expired incompatible replica heartbeat unexpectedly resumed")
	}
	if a.Status().Available || a.Compatible() {
		t.Fatalf("resumed replica status=%+v compatible=%v", a.Status(), a.Compatible())
	}
	if !b.Status().Available || !b.Compatible() {
		t.Fatalf("registered replica status=%+v compatible=%v", b.Status(), b.Compatible())
	}
}

func TestShorterReplicaTTLDoesNotExpireHealthyPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := openReplicaRaceStore(t)
	a := NewReplicaManager(store, uuid.New(), replicaRacePolicy(90*time.Second, 30*time.Second), 5*time.Second)
	b := NewReplicaManager(store, uuid.New(), replicaRacePolicy(15*time.Second, 5*time.Second), 5*time.Second)
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	if _, err := store.CoordinationPool().Exec(ctx, "UPDATE coordination_replicas SET heartbeat_at=clock_timestamp()-interval '20 seconds' WHERE replica_id=$1", a.replicaID); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err == nil {
		b.Close()
		t.Fatal("shorter-TTL replica accepted a healthy incompatible peer")
	}
	if !a.Status().Available || !a.Compatible() {
		t.Fatalf("healthy replica status=%+v compatible=%v", a.Status(), a.Compatible())
	}
}

func TestHeartbeatFailureLatchesRecoveryAcrossLaterSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := openReplicaRaceStore(t)
	recovery := coordination.NewRecoveryManager(coordination.RecoverySteps{}, time.Now)
	replica := NewReplicaManager(store, uuid.New(), replicaRacePolicy(time.Second, 20*time.Millisecond), 30*time.Millisecond)
	replica.SetFailureObserver(recovery)
	if err := replica.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(replica.Close)

	blocker, err := store.ConfigPool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Rollback(context.Background()) })
	if _, err := blocker.Exec(ctx, "LOCK TABLE coordination_replicas IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	for recovery.Status().State != coordination.RecoveryDegraded {
		select {
		case <-ctx.Done():
			t.Fatal("heartbeat failure was not published")
		case <-time.After(time.Millisecond):
		}
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for !replica.Status().Available {
		select {
		case <-ctx.Done():
			t.Fatal("heartbeat did not resume")
		case <-time.After(time.Millisecond):
		}
	}
	if status := recovery.Status(); status.State != coordination.RecoveryDegraded {
		t.Fatalf("successful heartbeat cleared recovery latch: %+v", status)
	}
}
