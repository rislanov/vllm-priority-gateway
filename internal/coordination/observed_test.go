package coordination

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type observedTestAdmission struct {
	decision AdmissionDecision
	err      error
	refresh  error
	cleaned  bool
}

func (f *observedTestAdmission) Acquire(context.Context, AdmissionRequest) (AdmissionDecision, error) {
	return f.decision, f.err
}
func (*observedTestAdmission) Renew(context.Context, []LeaseIdentity) ([]RenewResult, error) {
	return nil, nil
}
func (*observedTestAdmission) Complete(context.Context, []LeaseCompletion) ([]CompleteResult, error) {
	return nil, nil
}
func (*observedTestAdmission) Status() Status { return Status{Backend: "postgres"} }
func (*observedTestAdmission) PoolInflight(int64) int {
	return 7
}
func (f *observedTestAdmission) RefreshInflight(context.Context) error { return f.refresh }
func (f *observedTestAdmission) Cleanup(context.Context, int) error {
	f.cleaned = true
	return nil
}

type observedCall struct {
	backend, operation, outcome string
	reason                      Reason
}

type observedTestObserver struct{ calls []observedCall }

func (o *observedTestObserver) CoordinationOperation(backend, operation, outcome string, _ time.Duration, reason Reason) {
	o.calls = append(o.calls, observedCall{backend: backend, operation: operation, outcome: outcome, reason: reason})
}

type coordinationFailureObserverStub struct{ failures []error }

type observedCleanupAdmission struct {
	AdmissionCoordinator
	err error
}

func (c observedCleanupAdmission) CleanupBatch(context.Context, int) (bool, error) {
	return true, c.err
}

type observedCleanupCircuit struct {
	CircuitCoordinator
	err error
}

func (c observedCleanupCircuit) CleanupBatch(context.Context, int) (bool, error) {
	return true, c.err
}

func TestObservedCleanupPreservesProgressAndFailure(t *testing.T) {
	want := errors.New("cleanup database failure")
	for _, fail := range []error{nil, want} {
		observer := &coordinationFailureObserverStub{}
		admission := ObserveAdmission(observedCleanupAdmission{err: fail}, nil, observer)
		circuit := ObserveCircuit(observedCleanupCircuit{err: fail}, nil, observer)
		for _, wrapped := range []any{admission, circuit} {
			cleaner, ok := wrapped.(CleanupBatcher)
			if !ok {
				t.Fatalf("wrapper lost batch cleanup: %T", wrapped)
			}
			more, err := cleaner.CleanupBatch(context.Background(), 256)
			if !more || !errors.Is(err, fail) {
				t.Fatalf("cleanup progress/error lost: more=%t err=%v want=%v", more, err, fail)
			}
		}
		if fail != nil && len(observer.failures) != 2 || fail == nil && len(observer.failures) != 0 {
			t.Fatalf("unexpected cleanup failures: %v", observer.failures)
		}
	}
}

func (o *coordinationFailureObserverStub) CoordinationFailure(err error) {
	o.failures = append(o.failures, err)
}

func TestObserveAdmissionReportsRuntimeFailureAtOccurrence(t *testing.T) {
	want := errors.New("database timeout")
	next := &observedTestAdmission{refresh: want}
	failures := &coordinationFailureObserverStub{}
	wrapped := ObserveAdmission(next, nil, failures)
	runtime, ok := wrapped.(AdmissionRuntime)
	if !ok {
		t.Fatalf("wrapped admission does not preserve runtime capability: %T", wrapped)
	}
	if err := runtime.RefreshInflight(context.Background()); !errors.Is(err, want) {
		t.Fatalf("RefreshInflight()=%v, want %v", err, want)
	}
	next.refresh = nil
	if err := runtime.RefreshInflight(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(failures.failures) != 1 || !errors.Is(failures.failures[0], want) {
		t.Fatalf("failures=%v, want [%v]", failures.failures, want)
	}
}

func TestObserveAdmissionRecordsBoundedOutcomeAndPreservesRuntimeCapabilities(t *testing.T) {
	next := &observedTestAdmission{decision: AdmissionDecision{Reason: ReasonRPMExhausted}}
	observer := &observedTestObserver{}
	wrapped := ObserveAdmission(next, observer)

	decision, err := wrapped.Acquire(context.Background(), AdmissionRequest{LeaseID: uuid.New()})
	if err != nil || decision.Reason != ReasonRPMExhausted {
		t.Fatalf("Acquire() = (%+v, %v)", decision, err)
	}
	if len(observer.calls) != 1 || observer.calls[0] != (observedCall{backend: "postgres", operation: "acquire", outcome: "rejected", reason: ReasonRPMExhausted}) {
		t.Fatalf("calls = %+v", observer.calls)
	}
	runtime, ok := wrapped.(AdmissionRuntime)
	if !ok || runtime.PoolInflight(1) != 7 {
		t.Fatalf("wrapped runtime = (%T, %d)", wrapped, runtime.PoolInflight(1))
	}
	cleaner, ok := wrapped.(interface {
		Cleanup(context.Context, int) error
	})
	if !ok {
		t.Fatalf("wrapped coordinator does not preserve cleanup")
	}
	if err := cleaner.Cleanup(context.Background(), 10); err != nil || !next.cleaned {
		t.Fatalf("Cleanup() = %v, cleaned=%v", err, next.cleaned)
	}
}

func TestObserveAdmissionDistinguishesTimeoutFromUnavailable(t *testing.T) {
	observer := &observedTestObserver{}
	next := &observedTestAdmission{err: context.DeadlineExceeded}
	wrapped := ObserveAdmission(next, observer)
	_, _ = wrapped.Acquire(context.Background(), AdmissionRequest{})
	if got := observer.calls[0].outcome; got != "timeout" {
		t.Fatalf("deadline outcome = %q", got)
	}

	observer.calls = nil
	next.err = errors.New("database offline")
	_, _ = wrapped.Acquire(context.Background(), AdmissionRequest{})
	if got := observer.calls[0].outcome; got != "unavailable" {
		t.Fatalf("database outcome = %q", got)
	}
}

func TestOperationOutcomeTreatsTypedSemanticErrorAsDecision(t *testing.T) {
	err := ReasonError{Reason: ReasonStaleOperation}
	if got := operationOutcome("", err); got != "stale" {
		t.Fatalf("outcome=%q", got)
	}
	if got := operationReason(err); got != ReasonStaleOperation {
		t.Fatalf("reason=%q", got)
	}
}

type observedTestCircuit struct {
	CircuitCoordinator
	err error
}

func (f *observedTestCircuit) Refresh(context.Context) error { return f.err }
func (*observedTestCircuit) Status() Status                  { return Status{Backend: "postgres", Available: true} }

func TestFailureObservationDistinguishesCallerCancellation(t *testing.T) {
	for _, operation := range []string{"admission acquire", "admission refresh", "circuit refresh"} {
		for _, scenario := range []struct {
			name         string
			cancelCaller bool
			err          error
			want         RecoveryState
		}{
			{"caller cancellation", true, context.Canceled, RecoveryReady},
			{"internal timeout", false, context.DeadlineExceeded, RecoveryDegraded},
			{"database error despite canceled caller", true, errors.New("connection reset"), RecoveryDegraded},
			{"permanent fault despite canceled caller", true, PermanentError{Err: context.Canceled}, RecoveryPermanentFault},
		} {
			t.Run(operation+"/"+scenario.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if scenario.cancelCaller {
					cancel()
				}
				recovery := NewRecoveryManager(RecoverySteps{}, time.Now)
				admission := ObserveAdmission(&observedTestAdmission{err: scenario.err, refresh: scenario.err}, nil, recovery)
				circuit := ObserveCircuit(&observedTestCircuit{err: scenario.err}, nil, recovery)
				var err error
				switch operation {
				case "admission acquire":
					_, err = admission.Acquire(ctx, AdmissionRequest{})
				case "admission refresh":
					err = admission.(AdmissionRuntime).RefreshInflight(ctx)
				case "circuit refresh":
					err = circuit.Refresh(ctx)
				}
				if !errors.Is(err, scenario.err) {
					t.Fatalf("returned %v, want %v", err, scenario.err)
				}
				if got := recovery.Status().State; got != scenario.want {
					t.Fatalf("recovery=%s, want %s", got, scenario.want)
				}
			})
		}
	}
}
