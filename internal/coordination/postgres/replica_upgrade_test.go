package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

func TestReplicaStartupWaitCancellationAndRuntimeConflict(t *testing.T) {
	store := openReplicaRaceStore(t)
	ctx := context.Background()
	old := NewReplicaManager(store, uuid.New(), replicaRacePolicy(time.Minute, 20*time.Second), time.Second)
	if err := old.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	next := NewReplicaManager(store, uuid.New(), replicaRacePolicy(2*time.Minute, 20*time.Second), time.Second, ReplicaManagerOptions{StartupWaitTimeout: time.Minute})
	startup, cancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() { result <- next.Start(startup) }()
	deadline := time.After(3 * time.Second)
	for next.Status().Reason != coordination.ReasonIdempotencyConflict {
		select {
		case <-deadline:
			t.Fatal("did not observe conflict")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled startup=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not stop waiting")
	}
	if err := old.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	// Startup conflicts must not latch; runtime conflicts still must.
	if err := next.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	_, err := store.CoordinationPool().Exec(ctx, `INSERT INTO coordination_replicas(replica_id,started_at,heartbeat_at,heartbeat_expires_at,binary_version,coordination_contract_version,policy_fingerprint) VALUES($1,now(),now(),now()+interval '1 minute','old',3,$2)`, old.replicaID, old.fingerprint[:])
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Check(ctx); err == nil {
		t.Fatal("runtime conflict was accepted")
	}
	if err := old.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := next.Check(ctx); err == nil || next.Compatible() {
		t.Fatal("runtime conflict did not latch")
	}
}

func TestReplicaCloseContextPreservesForeignFingerprintAndCanceledRegistration(t *testing.T) {
	store := openReplicaRaceStore(t)
	ctx := context.Background()
	replica := NewReplicaManager(store, uuid.New(), replicaRacePolicy(time.Minute, 20*time.Second), time.Second)
	if err := replica.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := replica.CloseContext(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled close=%v", err)
	}
	var count int
	if err := store.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM coordination_replicas WHERE replica_id=$1", replica.replicaID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("canceled cleanup removed row: count=%d err=%v", count, err)
	}
	if _, err := store.CoordinationPool().Exec(ctx, "UPDATE coordination_replicas SET policy_fingerprint=$2 WHERE replica_id=$1", replica.replicaID, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if err := replica.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM coordination_replicas WHERE replica_id=$1", replica.replicaID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("cleanup removed foreign fingerprint: count=%d err=%v", count, err)
	}
}

func TestReplicaCloseContextBoundsBlockedDelete(t *testing.T) {
	store := openReplicaRaceStore(t)
	ctx := context.Background()
	replica := NewReplicaManager(store, uuid.New(), replicaRacePolicy(time.Minute, 20*time.Second), 100*time.Millisecond)
	if err := replica.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	tx, err := store.CoordinationPool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SELECT replica_id FROM coordination_replicas WHERE replica_id=$1 FOR UPDATE", replica.replicaID); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := replica.CloseContext(ctx); err == nil {
		t.Fatal("blocked delete succeeded")
	}
	if time.Since(start) > time.Second {
		t.Fatal("cleanup exceeded coordination timeout")
	}
}

func TestReplicaStartupWaitDoesNotRetryDatabaseFailure(t *testing.T) {
	store := openReplicaRaceStore(t)
	ctx := context.Background()
	tx, err := store.CoordinationPool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "LOCK TABLE coordination_replicas IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	replica := NewReplicaManager(store, uuid.New(), replicaRacePolicy(time.Minute, 20*time.Second), 50*time.Millisecond, ReplicaManagerOptions{StartupWaitTimeout: time.Minute})
	start := time.Now()
	if err := replica.Start(ctx); err == nil {
		replica.Close()
		t.Fatal("blocked startup succeeded")
	}
	if time.Since(start) > time.Second {
		t.Fatal("retried database timeout as compatibility conflict")
	}
}

func TestReplicaStartupWaitResumesAfterGracefulUnregister(t *testing.T) {
	store := openReplicaRaceStore(t)
	ctx := context.Background()
	old := NewReplicaManager(store, uuid.New(), replicaRacePolicy(time.Minute, 20*time.Second), time.Second)
	if err := old.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	next := NewReplicaManager(store, uuid.New(), replicaRacePolicy(2*time.Minute, 20*time.Second), time.Second, ReplicaManagerOptions{StartupWaitTimeout: 5 * time.Second})
	result := make(chan error, 1)
	go func() { result <- next.Start(ctx) }()
	deadline := time.After(3 * time.Second)
	for next.Status().Reason != coordination.ReasonIdempotencyConflict {
		select {
		case <-deadline:
			t.Fatal("did not wait for old live replica")
		case <-time.After(time.Millisecond):
		}
	}
	if err := old.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("startup did not resume after unregister")
	}
	defer next.Close()
	if !next.Status().Available || !next.Compatible() {
		t.Fatal("waited startup did not become healthy")
	}
}

func TestReplicaCloseContextCleansAmbiguousStartupRegistration(t *testing.T) {
	store := openReplicaRaceStore(t)
	ctx := context.Background()
	replica := NewReplicaManager(store, uuid.New(), replicaRacePolicy(time.Minute, 20*time.Second), time.Second)
	// A registration can commit before its acknowledgement is lost: no runtime
	// goroutine was started, but application teardown must remove the owned row.
	if err := replica.heartbeat(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := replica.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM coordination_replicas WHERE replica_id=$1", replica.replicaID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("ambiguous registration remains: count=%d err=%v", count, err)
	}
}
