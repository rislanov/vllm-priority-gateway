package postgres

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

func TestRetryReplicaHeartbeatUsesOneRetryInsideSharedDeadline(t *testing.T) {
	serializationFailure := &pgconn.PgError{Code: "40001"}
	attempts := 0
	if err := retryReplicaHeartbeat(context.Background(), func(context.Context) error {
		attempts++
		return serializationFailure
	}); !errors.Is(err, serializationFailure) {
		t.Fatalf("retry error=%v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d, want 2", attempts)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	attempts = 0
	started := time.Now()
	err := retryReplicaHeartbeat(ctx, func(ctx context.Context) error {
		attempts++
		<-ctx.Done()
		return serializationFailure
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error=%v", err)
	}
	if attempts != 1 {
		t.Fatalf("deadline attempts=%d, want 1", attempts)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("shared deadline took %s", elapsed)
	}
}

func TestHeartbeatAttemptDoesNotPublishTransientFailure(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://postgres@127.0.0.1:1/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	pool.Close()

	manager := &ReplicaManager{
		pool:    pool,
		timeout: time.Second,
		status:  coordination.Status{Backend: "postgres", Available: true},
	}
	if err := manager.heartbeatOnce(context.Background(), false); err == nil {
		t.Fatal("heartbeat attempt unexpectedly succeeded")
	}
	if status := manager.Status(); !status.Available || status.Degraded {
		t.Fatalf("transient retry attempt changed externally visible status: %+v", status)
	}
}

func TestPolicyFingerprintIsStableAndCoversCoordinationSemantics(t *testing.T) {
	policy := ReplicaPolicy{LeaseTTL: 90 * time.Second, LeaseRenewInterval: 30 * time.Second, Circuit: circuitbreaker.Options{FailureThreshold: 5, FailureWindow: 30 * time.Second, OpenCooldown: 15 * time.Second, HalfOpenMaxProbes: 1}}
	first := PolicyFingerprint(policy)
	second := PolicyFingerprint(policy)
	if !bytes.Equal(first[:], second[:]) {
		t.Fatal("identical policy produced different fingerprints")
	}
	changed := policy
	changed.LeaseTTL++
	changedFingerprint := PolicyFingerprint(changed)
	if bytes.Equal(first[:], changedFingerprint[:]) {
		t.Fatal("lease semantic change did not alter fingerprint")
	}
}

func TestCoordinationErrorClassificationKeepsTransientAndSemanticErrorsNonPermanent(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		context.Canceled,
		errors.New("connection reset by peer"),
		coordination.ReasonError{Reason: coordination.ReasonStaleBackend},
		&pgconn.PgError{Code: "40001"},
		&pgconn.PgError{Code: "08006"},
	} {
		if permanentCoordinationError(err) {
			t.Fatalf("%T %v classified permanent", err, err)
		}
	}
	for _, err := range []error{
		&pgconn.PgError{Code: "23505"},
		&pgconn.PgError{Code: "42P01"},
		&pgconn.PgError{Code: "XX000"},
	} {
		if !permanentCoordinationError(err) {
			t.Fatalf("%T %v classified transient", err, err)
		}
	}
}

func TestPermanentCoordinatorStatusIsSticky(t *testing.T) {
	admission := &AdmissionCoordinator{status: coordination.Status{Backend: "postgres", Available: true}}
	admission.setPermanent()
	admission.setStatus(true, "")
	if status := admission.Status(); !status.Permanent || status.Available || status.Degraded {
		t.Fatalf("admission status=%+v", status)
	}

	circuit := &CircuitCoordinator{status: coordination.Status{Backend: "postgres", Available: true}}
	circuit.setPermanent()
	circuit.setStatus(true, "")
	if status := circuit.Status(); !status.Permanent || status.Available || status.Degraded {
		t.Fatalf("circuit status=%+v", status)
	}
}
