package coordination

import (
	"context"
	"errors"
	"time"
)

type OperationObserver interface {
	CoordinationOperation(backend, operation, outcome string, duration time.Duration, reason Reason)
}

type observedAdmission struct {
	next     AdmissionCoordinator
	observer OperationObserver
}

func ObserveAdmission(next AdmissionCoordinator, observer OperationObserver) AdmissionCoordinator {
	if observer == nil {
		return next
	}
	return &observedAdmission{next: next, observer: observer}
}

func (o *observedAdmission) Acquire(ctx context.Context, request AdmissionRequest) (AdmissionDecision, error) {
	started := time.Now()
	decision, err := o.next.Acquire(ctx, request)
	o.observer.CoordinationOperation(o.Status().Backend, "acquire", operationOutcome(decision.Reason, err), time.Since(started), decision.Reason)
	return decision, err
}

func (o *observedAdmission) Renew(ctx context.Context, leases []LeaseIdentity) ([]RenewResult, error) {
	started := time.Now()
	results, err := o.next.Renew(ctx, leases)
	o.observer.CoordinationOperation(o.Status().Backend, "renew", operationOutcome("", err), time.Since(started), "")
	return results, err
}

func (o *observedAdmission) Complete(ctx context.Context, completions []LeaseCompletion) ([]CompleteResult, error) {
	started := time.Now()
	results, err := o.next.Complete(ctx, completions)
	o.observer.CoordinationOperation(o.Status().Backend, "complete", operationOutcome("", err), time.Since(started), "")
	return results, err
}

func (o *observedAdmission) Status() Status { return o.next.Status() }

func (o *observedAdmission) PoolInflight(poolID int64) int {
	if runtime, ok := o.next.(AdmissionRuntime); ok {
		return runtime.PoolInflight(poolID)
	}
	return 0
}

func (o *observedAdmission) RefreshInflight(ctx context.Context) error {
	if runtime, ok := o.next.(AdmissionRuntime); ok {
		return runtime.RefreshInflight(ctx)
	}
	return nil
}

func (o *observedAdmission) Cleanup(ctx context.Context, batch int) error {
	if cleaner, ok := o.next.(interface {
		Cleanup(context.Context, int) error
	}); ok {
		return cleaner.Cleanup(ctx, batch)
	}
	return nil
}

type observedCircuit struct {
	next     CircuitCoordinator
	observer OperationObserver
}

func ObserveCircuit(next CircuitCoordinator, observer OperationObserver) CircuitCoordinator {
	if observer == nil {
		return next
	}
	return &observedCircuit{next: next, observer: observer}
}

func (o *observedCircuit) Reconcile(ctx context.Context, backends []BackendIdentity) error {
	started := time.Now()
	err := o.next.Reconcile(ctx, backends)
	o.observer.CoordinationOperation(o.Status().Backend, "reconcile", operationOutcome("", err), time.Since(started), "")
	return err
}

func (o *observedCircuit) Snapshot(backendID int64, at time.Time) CircuitSnapshot {
	return o.next.Snapshot(backendID, at)
}

func (o *observedCircuit) Acquire(ctx context.Context, request CircuitAcquireRequest) (CircuitDecision, error) {
	started := time.Now()
	decision, err := o.next.Acquire(ctx, request)
	o.observer.CoordinationOperation(o.Status().Backend, "acquire", operationOutcome(decision.Reason, err), time.Since(started), decision.Reason)
	return decision, err
}

func (o *observedCircuit) Complete(ctx context.Context, completion CircuitCompletion) (CircuitSnapshot, error) {
	started := time.Now()
	snapshot, err := o.next.Complete(ctx, completion)
	reason := operationReason(err)
	o.observer.CoordinationOperation(o.Status().Backend, "complete", operationOutcome(reason, err), time.Since(started), reason)
	return snapshot, err
}

func (o *observedCircuit) RenewProbes(ctx context.Context, probes []ProbeIdentity) ([]RenewResult, error) {
	started := time.Now()
	results, err := o.next.RenewProbes(ctx, probes)
	o.observer.CoordinationOperation(o.Status().Backend, "renew", operationOutcome("", err), time.Since(started), "")
	return results, err
}

func (o *observedCircuit) Refresh(ctx context.Context) error {
	started := time.Now()
	err := o.next.Refresh(ctx)
	o.observer.CoordinationOperation(o.Status().Backend, "refresh", operationOutcome("", err), time.Since(started), "")
	return err
}

func (o *observedCircuit) Status() Status { return o.next.Status() }

func (o *observedCircuit) Cleanup(ctx context.Context, batch int) error {
	if cleaner, ok := o.next.(interface {
		Cleanup(context.Context, int) error
	}); ok {
		return cleaner.Cleanup(ctx, batch)
	}
	return nil
}

func operationOutcome(reason Reason, err error) string {
	var reasonErr ReasonError
	if errors.As(err, &reasonErr) {
		reason = reasonErr.Reason
		err = nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	if err != nil {
		return "unavailable"
	}
	switch reason {
	case "":
		return "success"
	case ReasonStaleBackend, ReasonStaleConfiguration, ReasonStaleOperation:
		return "stale"
	case ReasonIdempotencyConflict:
		return "conflict"
	default:
		return "rejected"
	}
}

func operationReason(err error) Reason {
	var reason ReasonError
	if errors.As(err, &reason) {
		return reason.Reason
	}
	return ""
}
