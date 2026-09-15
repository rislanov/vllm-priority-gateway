package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestCoordinatorCancellationPreservesHealth(t *testing.T) {
	// The lazy pool never needs a database: each operation's context is already
	// done before its first database call. In-flight HTTP cancellation is also
	// exercised by the opt-in PostgreSQL integration test.
	pool, err := pgxpool.New(context.Background(), "postgres://postgres@127.0.0.1:1/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	operations := []struct {
		name string
		run  func(context.Context, *AdmissionCoordinator, *CircuitCoordinator) error
	}{
		{"admission acquire", func(ctx context.Context, a *AdmissionCoordinator, _ *CircuitCoordinator) error {
			_, err := a.Acquire(ctx, coordination.AdmissionRequest{LeaseID: uuid.New()})
			return err
		}},
		{"admission renew", func(ctx context.Context, a *AdmissionCoordinator, _ *CircuitCoordinator) error {
			_, err := a.Renew(ctx, []coordination.LeaseIdentity{{LeaseID: uuid.New()}})
			return err
		}},
		{"admission complete", func(ctx context.Context, a *AdmissionCoordinator, _ *CircuitCoordinator) error {
			_, err := a.Complete(ctx, []coordination.LeaseCompletion{{Lease: coordination.LeaseIdentity{LeaseID: uuid.New()}}})
			return err
		}},
		{"admission refresh", func(ctx context.Context, a *AdmissionCoordinator, _ *CircuitCoordinator) error {
			return a.RefreshInflight(ctx)
		}},
		{"admission cleanup", func(ctx context.Context, a *AdmissionCoordinator, _ *CircuitCoordinator) error {
			return a.Cleanup(ctx, 1)
		}},
		{"circuit acquire", func(ctx context.Context, _ *AdmissionCoordinator, c *CircuitCoordinator) error {
			_, err := c.Acquire(ctx, coordination.CircuitAcquireRequest{AttemptID: uuid.New()})
			return err
		}},
		{"circuit complete", func(ctx context.Context, _ *AdmissionCoordinator, c *CircuitCoordinator) error {
			_, err := c.Complete(ctx, coordination.CircuitCompletion{AttemptID: uuid.New(), Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now()})
			return err
		}},
		{"circuit renew", func(ctx context.Context, _ *AdmissionCoordinator, c *CircuitCoordinator) error {
			_, err := c.RenewProbes(ctx, []coordination.ProbeIdentity{{PermitID: uuid.New()}})
			return err
		}},
		{"circuit reconcile", func(ctx context.Context, _ *AdmissionCoordinator, c *CircuitCoordinator) error {
			return c.Reconcile(ctx, nil)
		}},
		{"circuit refresh", func(ctx context.Context, _ *AdmissionCoordinator, c *CircuitCoordinator) error {
			return c.Refresh(ctx)
		}},
		{"circuit cleanup", func(ctx context.Context, _ *AdmissionCoordinator, c *CircuitCoordinator) error {
			return c.Cleanup(ctx, 1)
		}},
	}
	for _, operation := range operations {
		for _, mode := range []string{"caller cancellation", "caller deadline", "coordinator deadline"} {
			t.Run(operation.name+"/"+mode, func(t *testing.T) {
				parent := context.Background()
				timeout := time.Second
				wantErr := context.DeadlineExceeded
				switch mode {
				case "caller cancellation":
					var cancel context.CancelFunc
					parent, cancel = context.WithCancel(parent)
					cancel()
					wantErr = context.Canceled
				case "caller deadline":
					var cancel context.CancelFunc
					parent, cancel = context.WithDeadline(parent, time.Now().Add(-time.Second))
					defer cancel()
				case "coordinator deadline":
					timeout = -time.Second
				}
				ready := coordination.Status{Backend: "postgres", Available: true}
				admission := &AdmissionCoordinator{pool: pool, timeout: timeout, status: ready}
				circuit := &CircuitCoordinator{pool: pool, timeout: timeout, status: ready}
				recovery := coordination.NewRecoveryManager(coordination.RecoverySteps{}, time.Now)
				admission.SetFailureObserver(recovery)
				circuit.SetFailureObserver(recovery)
				if err := operation.run(parent, admission, circuit); !errors.Is(err, wantErr) {
					t.Fatalf("operation error=%v, want %v", err, wantErr)
				}
				if mode == "coordinator deadline" {
					if recovery.Status().State != coordination.RecoveryDegraded {
						t.Fatalf("coordinator timeout was not latched: %+v", recovery.Status())
					}
					return
				}
				if !admission.Status().Available || !circuit.Status().Available || recovery.Status().State != coordination.RecoveryReady {
					t.Fatalf("caller cancellation changed shared health: admission=%+v circuit=%+v recovery=%+v", admission.Status(), circuit.Status(), recovery.Status())
				}
			})
		}
	}
}
