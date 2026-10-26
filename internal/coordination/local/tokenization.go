package local

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"sync"
	"time"
)

type tokenizationReceipt struct {
	fingerprint [32]byte
	decision    coordination.TokenizationDecision
	retainUntil time.Time
	completed   bool
}
type tokenizationRate struct {
	balance float64
	at      time.Time
}
type TokenizationCoordinator struct {
	mu       sync.Mutex
	policy   coordination.TokenizationPolicy
	now      func() time.Time
	receipts map[uuid.UUID]*tokenizationReceipt
	rates    map[int64]tokenizationRate
}

func NewTokenizationCoordinator(policy coordination.TokenizationPolicy, now func() time.Time) *TokenizationCoordinator {
	if now == nil {
		now = time.Now
	}
	return &TokenizationCoordinator{policy: policy, now: now, receipts: make(map[uuid.UUID]*tokenizationReceipt), rates: make(map[int64]tokenizationRate)}
}
func (c *TokenizationCoordinator) Status() coordination.Status {
	return coordination.Status{Backend: "local", Available: c.policy.Validate() == nil}
}
func (c *TokenizationCoordinator) Acquire(ctx context.Context, r coordination.TokenizationAcquireRequest) (coordination.TokenizationDecision, error) {
	if e := ctx.Err(); e != nil {
		return coordination.TokenizationDecision{}, e
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	if c.policy.Validate() != nil {
		return coordination.TokenizationDecision{Reason: coordination.TokenizationUnavailable}, nil
	}
	c.cleanup(now)
	fp := r.Fingerprint()
	if old := c.receipts[r.OperationID]; old != nil {
		if old.fingerprint != fp {
			return coordination.TokenizationDecision{Reason: coordination.TokenizationConflict}, nil
		}
		if old.completed || old.decision.Lease != nil && !old.decision.Lease.ExpiresAt.After(now) {
			return coordination.TokenizationDecision{Reason: coordination.TokenizationLeaseLost}, nil
		}
		return cloneTokenizationDecision(old.decision), nil
	}
	if !r.ValidAt(now) {
		return coordination.TokenizationDecision{Reason: coordination.TokenizationStaleOperation}, nil
	}
	if len(c.receipts) >= 65536 {
		return coordination.TokenizationDecision{Reason: coordination.TokenizationUnavailable}, nil
	}
	rec := &tokenizationReceipt{fingerprint: fp, retainUntil: maxTime(now, r.OperationStartedAt).Add(24 * time.Hour)}
	c.receipts[r.OperationID] = rec
	reject := func(reason string) (coordination.TokenizationDecision, error) {
		rec.decision.Reason = reason
		return cloneTokenizationDecision(rec.decision), nil
	}
	clientCount := 0
	counts := make(map[int64]int)
	for _, old := range c.receipts {
		if l := old.decision.Lease; l != nil && !old.completed && l.ExpiresAt.After(now) {
			counts[l.BackendID]++
			if l.ClientID == r.ClientID {
				clientCount++
			}
		}
	}
	if clientCount >= c.policy.ClientMaxInflight {
		return reject(coordination.TokenizationClientLimit)
	}
	var selected coordination.TokenizationCandidate
	for _, b := range r.Candidates {
		if b.BackendID > 0 && counts[b.BackendID] < c.policy.BackendMaxInflight && (selected.BackendID == 0 || counts[b.BackendID] < counts[selected.BackendID] || counts[b.BackendID] == counts[selected.BackendID] && b.BackendID < selected.BackendID) {
			selected = b
		}
	}
	if selected.BackendID == 0 {
		return reject(coordination.TokenizationBackendLimit)
	}
	state, ok := c.rates[r.ClientID]
	if !ok {
		state = tokenizationRate{c.policy.Burst(), now}
	}
	state.balance = min(c.policy.Burst(), state.balance+max(0, now.Sub(state.at).Seconds())*float64(c.policy.ClientRPM)/60)
	state.at = now
	if state.balance < 1 {
		retry := now.Add(time.Duration((1 - state.balance) * 60 / float64(c.policy.ClientRPM) * float64(time.Second)))
		rec.decision.RetryAt = &retry
		return reject(coordination.TokenizationRateLimit)
	}
	state.balance--
	c.rates[r.ClientID] = state
	rec.decision.Lease = &coordination.TokenizationLease{OperationID: r.OperationID, ReplicaID: r.ReplicaID, ClientID: r.ClientID, PoolID: r.PoolID, BackendID: selected.BackendID, BackendRevision: selected.BackendRevision, AcquiredAt: now, ExpiresAt: now.Add(c.policy.LeaseTTL())}
	return cloneTokenizationDecision(rec.decision), nil
}
func (c *TokenizationCoordinator) Complete(ctx context.Context, l coordination.TokenizationLease) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec := c.receipts[l.OperationID]
	if rec == nil {
		return errors.New("unknown tokenization lease")
	}
	if rec.decision.Lease == nil || *rec.decision.Lease != l {
		return errors.New("tokenization lease identity mismatch")
	}
	rec.completed = true
	return nil
}
func (c *TokenizationCoordinator) cleanup(now time.Time) {
	for id, r := range c.receipts {
		if !r.retainUntil.After(now) && (r.decision.Lease == nil || r.completed || !r.decision.Lease.ExpiresAt.After(now)) {
			delete(c.receipts, id)
		}
	}
	for id, r := range c.rates {
		if now.Sub(r.at) >= 24*time.Hour {
			delete(c.rates, id)
		}
	}
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func cloneTokenizationDecision(d coordination.TokenizationDecision) coordination.TokenizationDecision {
	if d.Lease != nil {
		l := *d.Lease
		d.Lease = &l
	}
	if d.RetryAt != nil {
		r := *d.RetryAt
		d.RetryAt = &r
	}
	return d
}
