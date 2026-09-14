package postgres

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

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
