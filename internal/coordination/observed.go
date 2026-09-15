package coordination

import (
	"context"
	"errors"
	"time"
)

type OperationObserver interface {
	CoordinationOperation(backend, operation, outcome string, duration time.Duration, reason Reason)
}

type CoordinationFailureObserver interface {
	CoordinationFailure(error)
}

type observedAdmission struct {
	next     AdmissionCoordinator
	observer OperationObserver
	failure  CoordinationFailureObserver
}

func ObserveAdmission(next AdmissionCoordinator, observer OperationObserver, failures ...CoordinationFailureObserver) AdmissionCoordinator {
	var failure CoordinationFailureObserver
	if len(failures) > 0 {
		failure = failures[0]
	}
	if observer == nil && failure == nil {
		return next
	}
	return &observedAdmission{next: next, observer: observer, failure: failure}
}

func (o *observedAdmission) Acquire(ctx context.Context, request AdmissionRequest) (AdmissionDecision, error) {
	started := time.Now()
	decision, err := o.next.Acquire(ctx, request)
	o.observe(ctx, "acquire", decision.Reason, err, started)
	return decision, err
}

func (o *observedAdmission) Renew(ctx context.Context, leases []LeaseIdentity) ([]RenewResult, error) {
	started := time.Now()
	results, err := o.next.Renew(ctx, leases)
	o.observe(ctx, "renew", "", err, started)
	return results, err
}

func (o *observedAdmission) Complete(ctx context.Context, completions []LeaseCompletion) ([]CompleteResult, error) {
	started := time.Now()
	results, err := o.next.Complete(ctx, completions)
	o.observe(ctx, "complete", "", err, started)
	return results, err
}

func (o *observedAdmission) observe(ctx context.Context, operation string, reason Reason, err error, started time.Time) {
	observeCoordinationFailure(ctx, o.failure, err)
	if o.observer != nil {
		o.observer.CoordinationOperation(o.Status().Backend, operation, operationOutcome(reason, err), time.Since(started), reason)
	}
}

func (o *observedAdmission) Status() Status { return o.next.Status() }

func (o *observedAdmission) ObserveClientRatePolicy(policy ClientRatePolicy) {
	if observer, ok := o.next.(AdmissionPolicyObserver); ok {
		observer.ObserveClientRatePolicy(policy)
	}
}

func (o *observedAdmission) PoolInflight(poolID int64) int {
	if runtime, ok := o.next.(AdmissionRuntime); ok {
		return runtime.PoolInflight(poolID)
	}
	return 0
}

func (o *observedAdmission) RefreshInflight(ctx context.Context) error {
	if runtime, ok := o.next.(AdmissionRuntime); ok {
		err := runtime.RefreshInflight(ctx)
		observeCoordinationFailure(ctx, o.failure, err)
		return err
	}
	return nil
}

func (o *observedAdmission) Cleanup(ctx context.Context, batch int) error {
	if cleaner, ok := o.next.(interface {
		Cleanup(context.Context, int) error
	}); ok {
		err := cleaner.Cleanup(ctx, batch)
		observeCoordinationFailure(ctx, o.failure, err)
		return err
	}
	return nil
}

func (o *observedAdmission) CleanupBatch(ctx context.Context, batch int) (bool, error) {
	if cleaner, ok := o.next.(CleanupBatcher); ok {
		more, err := cleaner.CleanupBatch(ctx, batch)
		observeCoordinationFailure(ctx, o.failure, err)
		return more, err
	}
	return false, o.Cleanup(ctx, batch)
}

type observedCircuit struct {
	next     CircuitCoordinator
	observer OperationObserver
	failure  CoordinationFailureObserver
}

func ObserveCircuit(next CircuitCoordinator, observer OperationObserver, failures ...CoordinationFailureObserver) CircuitCoordinator {
	var failure CoordinationFailureObserver
	if len(failures) > 0 {
		failure = failures[0]
	}
	if observer == nil && failure == nil {
		return next
	}
	return &observedCircuit{next: next, observer: observer, failure: failure}
}

func (o *observedCircuit) Reconcile(ctx context.Context, backends []BackendIdentity) error {
	started := time.Now()
	err := o.next.Reconcile(ctx, backends)
	o.observe(ctx, "reconcile", "", err, started)
	return err
}

func (o *observedCircuit) Snapshot(backendID int64, at time.Time) CircuitSnapshot {
	return o.next.Snapshot(backendID, at)
}

func (o *observedCircuit) Acquire(ctx context.Context, request CircuitAcquireRequest) (CircuitDecision, error) {
	started := time.Now()
	decision, err := o.next.Acquire(ctx, request)
	o.observe(ctx, "acquire", decision.Reason, err, started)
	return decision, err
}

func (o *observedCircuit) Complete(ctx context.Context, completion CircuitCompletion) (CircuitSnapshot, error) {
	started := time.Now()
	snapshot, err := o.next.Complete(ctx, completion)
	reason := operationReason(err)
	o.observe(ctx, "complete", reason, err, started)
	return snapshot, err
}

func (o *observedCircuit) RenewProbes(ctx context.Context, probes []ProbeIdentity) ([]RenewResult, error) {
	started := time.Now()
	results, err := o.next.RenewProbes(ctx, probes)
	o.observe(ctx, "renew", "", err, started)
	return results, err
}

func (o *observedCircuit) Refresh(ctx context.Context) error {
	started := time.Now()
	err := o.next.Refresh(ctx)
	o.observe(ctx, "refresh", "", err, started)
	return err
}

func (o *observedCircuit) observe(ctx context.Context, operation string, reason Reason, err error, started time.Time) {
	observeCoordinationFailure(ctx, o.failure, err)
	if o.observer != nil {
		o.observer.CoordinationOperation(o.Status().Backend, operation, operationOutcome(reason, err), time.Since(started), reason)
	}
}

func (o *observedCircuit) Status() Status { return o.next.Status() }

func (o *observedCircuit) Cleanup(ctx context.Context, batch int) error {
	if cleaner, ok := o.next.(interface {
		Cleanup(context.Context, int) error
	}); ok {
		err := cleaner.Cleanup(ctx, batch)
		observeCoordinationFailure(ctx, o.failure, err)
		return err
	}
	return nil
}

func (o *observedCircuit) CleanupBatch(ctx context.Context, batch int) (bool, error) {
	if cleaner, ok := o.next.(CleanupBatcher); ok {
		more, err := cleaner.CleanupBatch(ctx, batch)
		observeCoordinationFailure(ctx, o.failure, err)
		return more, err
	}
	return false, o.Cleanup(ctx, batch)
}

func observeCoordinationFailure(ctx context.Context, observer CoordinationFailureObserver, err error) {
	if observer == nil || err == nil || IsCallerCancellation(ctx, err) {
		return
	}
	var reason ReasonError
	if errors.As(err, &reason) {
		return
	}
	observer.CoordinationFailure(err)
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
