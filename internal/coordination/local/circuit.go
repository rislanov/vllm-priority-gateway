package local

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

const defaultLocalCircuitAttemptCapacity = 65_536

type circuitEntry struct {
	identity coordination.BackendIdentity
	breaker  *circuitbreaker.Breaker
}
type circuitAttempt struct {
	fingerprint [32]byte
	backend     coordination.BackendIdentity
	generation  int64
	permit      *coordination.ProbeIdentity
	complete    func(domain.InferenceOutcome, time.Time)
	ttl         time.Duration
	terminal    bool
	completion  *coordination.CircuitCompletion
	acquiredAt  time.Time
	retainUntil time.Time
}
type CircuitCoordinator struct {
	mu               sync.Mutex
	options          circuitbreaker.Options
	now              func() time.Time
	circuits         map[int64]*circuitEntry
	attempts         map[uuid.UUID]*circuitAttempt
	expiries         expiryQueue
	terminalExpiries expiryQueue
	attemptCapacity  int
}

func NewCircuitCoordinator(options circuitbreaker.Options, now func() time.Time) (*CircuitCoordinator, error) {
	return newCircuitCoordinator(options, now, defaultLocalCircuitAttemptCapacity)
}

func newCircuitCoordinator(options circuitbreaker.Options, now func() time.Time, attemptCapacity int) (*CircuitCoordinator, error) {
	if _, err := circuitbreaker.New(options); err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	if attemptCapacity <= 0 {
		attemptCapacity = defaultLocalCircuitAttemptCapacity
	}
	return &CircuitCoordinator{options: options, now: now, circuits: make(map[int64]*circuitEntry), attempts: make(map[uuid.UUID]*circuitAttempt), attemptCapacity: attemptCapacity}, nil
}
func (c *CircuitCoordinator) Status() coordination.Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	c.expire(now)
	available := len(c.attempts) < c.attemptCapacity || c.hasTerminalAttempt()
	return coordination.Status{Backend: "local", Available: available, LastSuccess: now}
}
func (c *CircuitCoordinator) Refresh(context.Context) error { return nil }
func (c *CircuitCoordinator) Reconcile(ctx context.Context, values []coordination.BackendIdentity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	c.expire(now)
	seen := make(map[int64]bool, len(values))
	for _, value := range values {
		if seen[value.ID] {
			return fmt.Errorf("duplicate backend ID %d", value.ID)
		}
		seen[value.ID] = true
		old := c.circuits[value.ID]
		if !value.Enabled || value.Draining {
			delete(c.circuits, value.ID)
			c.invalidate(value.ID, now)
			continue
		}
		if old != nil && old.identity == value {
			continue
		}
		b, err := circuitbreaker.New(c.options)
		if err != nil {
			return err
		}
		c.invalidate(value.ID, now)
		c.circuits[value.ID] = &circuitEntry{identity: value, breaker: b}
	}
	for id := range c.circuits {
		if !seen[id] {
			delete(c.circuits, id)
			c.invalidate(id, now)
		}
	}
	return nil
}
func (c *CircuitCoordinator) Snapshot(backendID int64, at time.Time) coordination.CircuitSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expire(at)
	return c.snapshot(backendID, at)
}
func (c *CircuitCoordinator) Acquire(ctx context.Context, req coordination.CircuitAcquireRequest) (coordination.CircuitDecision, error) {
	if err := ctx.Err(); err != nil {
		return coordination.CircuitDecision{}, err
	}
	fp := fingerprint(req)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	c.expire(now)
	if old := c.attempts[req.AttemptID]; old != nil {
		if old.fingerprint != fp {
			return coordination.CircuitDecision{Reason: coordination.ReasonIdempotencyConflict}, nil
		}
		if old.terminal {
			return coordination.CircuitDecision{Snapshot: c.snapshot(req.Backend.ID, now), Reason: coordination.ReasonLeaseLost}, nil
		}
		return coordination.CircuitDecision{Snapshot: c.snapshot(req.Backend.ID, now), Permit: old.permit}, nil
	}
	if !c.makeAttemptRoom() {
		return coordination.CircuitDecision{Snapshot: c.snapshot(req.Backend.ID, now), Reason: coordination.ReasonCoordinationUnavailable}, nil
	}
	entry := c.circuits[req.Backend.ID]
	if entry == nil || entry.identity != req.Backend || !req.Backend.Enabled || req.Backend.Draining {
		return coordination.CircuitDecision{Reason: coordination.ReasonStaleBackend}, nil
	}
	if req.AcquisitionStartedAt.After(now.Add(5*time.Minute)) || !req.AcquisitionStartedAt.After(now.Add(-receiptRetention)) {
		return coordination.CircuitDecision{Reason: coordination.ReasonStaleOperation}, nil
	}
	complete, ok := entry.breaker.Acquire(now)
	snap := toCircuitSnapshot(entry.identity, entry.breaker.Snapshot(now))
	if !ok {
		reason := coordination.ReasonCircuitOpen
		if snap.State == domain.CircuitHalfOpen {
			reason = coordination.ReasonProbeCapacityExhausted
		}
		return coordination.CircuitDecision{Snapshot: snap, Reason: reason}, nil
	}
	attempt := &circuitAttempt{fingerprint: fp, backend: req.Backend, generation: snap.Generation, complete: complete, acquiredAt: req.AcquisitionStartedAt.UTC()}
	if snap.State == domain.CircuitHalfOpen {
		permit := coordination.ProbeIdentity{PermitID: req.AttemptID, Backend: req.Backend, Generation: snap.Generation, ReplicaID: req.ReplicaID, ExpiresAt: now.Add(req.ProbeTTL), TTL: req.ProbeTTL}
		attempt.permit = &permit
		attempt.ttl = req.ProbeTTL
		c.expiries.add(circuitPermitExpiry, req.AttemptID, permit.ExpiresAt)
	}
	c.attempts[req.AttemptID] = attempt
	return coordination.CircuitDecision{Snapshot: snap, Permit: attempt.permit}, nil
}
func (c *CircuitCoordinator) Complete(ctx context.Context, value coordination.CircuitCompletion) (coordination.CircuitSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return coordination.CircuitSnapshot{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	c.expire(now)
	attempt := c.attempts[value.AttemptID]
	if attempt != nil && attempt.completion != nil {
		if *attempt.completion != value {
			return c.snapshot(value.Backend.ID, now), coordination.ReasonError{Reason: coordination.ReasonIdempotencyConflict}
		}
		return c.snapshot(value.Backend.ID, now), nil
	}
	if value.ReportedOutcomeAt.After(now.Add(5*time.Minute)) || !value.ReportedOutcomeAt.After(now.Add(-receiptRetention)) {
		return c.snapshot(value.Backend.ID, now), coordination.ReasonError{Reason: coordination.ReasonStaleOperation}
	}
	if attempt == nil {
		return c.snapshot(value.Backend.ID, now), nil
	}
	if attempt.backend != value.Backend || attempt.generation != value.Generation {
		return c.snapshot(value.Backend.ID, now), coordination.ReasonError{Reason: coordination.ReasonIdempotencyConflict}
	}
	copy := value
	attempt.completion = &copy
	if !attempt.terminal {
		attempt.complete(value.Outcome, value.ReportedOutcomeAt)
		attempt.terminal = true
	}
	attempt.retainUntil = circuitRetentionBoundary(now, attempt.acquiredAt, value.ReportedOutcomeAt)
	c.terminalExpiries.add(circuitAttemptExpiry, value.AttemptID, attempt.retainUntil)
	return c.snapshot(value.Backend.ID, now), nil
}
func (c *CircuitCoordinator) RenewProbes(ctx context.Context, probes []coordination.ProbeIdentity) ([]coordination.RenewResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	c.expire(now)
	out := make([]coordination.RenewResult, len(probes))
	for i, p := range probes {
		a := c.attempts[p.PermitID]
		if a == nil || a.terminal || a.permit == nil || a.permit.Generation != p.Generation {
			out[i].Reason = coordination.ReasonLeaseLost
			continue
		}
		ttl := a.ttl
		if ttl <= 0 {
			ttl = p.TTL
		}
		if ttl <= 0 {
			out[i] = coordination.RenewResult{Lease: coordination.LeaseIdentity{LeaseID: p.PermitID}, Reason: coordination.ReasonLeaseLost}
			continue
		}
		a.permit.ExpiresAt = now.Add(ttl)
		c.expiries.add(circuitPermitExpiry, p.PermitID, a.permit.ExpiresAt)
		out[i] = coordination.RenewResult{Lease: coordination.LeaseIdentity{
			LeaseID: p.PermitID, ExpiresAt: a.permit.ExpiresAt, TTL: ttl,
		}}
	}
	return out, nil
}
func (c *CircuitCoordinator) expire(now time.Time) {
	for {
		expiry, ok := c.expiries.due(now)
		if !ok {
			break
		}
		attempt := c.attempts[expiry.id]
		if attempt == nil {
			continue
		}
		switch expiry.kind {
		case circuitPermitExpiry:
			if attempt.terminal || attempt.permit == nil || !attempt.permit.ExpiresAt.Equal(expiry.at) || attempt.permit.ExpiresAt.After(now) {
				continue
			}
			attempt.complete(domain.InferenceFailure, attempt.permit.ExpiresAt)
			attempt.terminal = true
			attempt.retainUntil = circuitRetentionBoundary(now, attempt.acquiredAt, attempt.permit.ExpiresAt)
			c.terminalExpiries.add(circuitAttemptExpiry, expiry.id, attempt.retainUntil)
		}
	}
	for {
		expiry, ok := c.terminalExpiries.due(now)
		if !ok {
			return
		}
		attempt := c.attempts[expiry.id]
		if attempt != nil && attempt.terminal && attempt.retainUntil.Equal(expiry.at) && !attempt.retainUntil.After(now) {
			delete(c.attempts, expiry.id)
		}
	}
}

func (c *CircuitCoordinator) invalidate(id int64, now time.Time) {
	for attemptID, a := range c.attempts {
		if a.backend.ID == id && !a.terminal {
			a.terminal = true
			a.retainUntil = circuitRetentionBoundary(now, a.acquiredAt)
			c.terminalExpiries.add(circuitAttemptExpiry, attemptID, a.retainUntil)
		}
	}
}

func (c *CircuitCoordinator) makeAttemptRoom() bool {
	for len(c.attempts) >= c.attemptCapacity {
		if !c.hasTerminalAttempt() {
			return false
		}
		expiry, ok := c.terminalExpiries.popOldest()
		if !ok {
			return false
		}
		delete(c.attempts, expiry.id)
	}
	return true
}

func (c *CircuitCoordinator) hasTerminalAttempt() bool {
	for c.terminalExpiries.Len() > 0 {
		expiry := c.terminalExpiries[0]
		attempt := c.attempts[expiry.id]
		if attempt != nil && attempt.terminal && !attempt.retainUntil.IsZero() && attempt.retainUntil.Equal(expiry.at) {
			return true
		}
		_, _ = c.terminalExpiries.popOldest()
	}
	return false
}

func circuitRetentionBoundary(values ...time.Time) time.Time {
	latest := values[0]
	for _, value := range values[1:] {
		if value.After(latest) {
			latest = value
		}
	}
	return latest.Add(receiptRetention)
}
func (c *CircuitCoordinator) snapshot(id int64, at time.Time) coordination.CircuitSnapshot {
	e := c.circuits[id]
	if e == nil {
		return coordination.CircuitSnapshot{Backend: coordination.BackendIdentity{ID: id}, State: domain.CircuitOpen}
	}
	return toCircuitSnapshot(e.identity, e.breaker.Snapshot(at))
}
func toCircuitSnapshot(id coordination.BackendIdentity, s circuitbreaker.Snapshot) coordination.CircuitSnapshot {
	return coordination.CircuitSnapshot{Backend: id, State: s.State, Generation: int64(s.Generation), FailureCount: s.FailureCount, RetryAt: s.RetryAt, ProbesInFlight: s.ProbesInFlight, Available: s.Available}
}

var _ coordination.CircuitCoordinator = (*CircuitCoordinator)(nil)
