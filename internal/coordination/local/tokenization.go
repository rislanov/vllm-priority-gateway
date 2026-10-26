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
	clientID    int64
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
	mu              sync.Mutex
	policy          coordination.TokenizationPolicy
	now             func() time.Time
	receipts        map[uuid.UUID]*tokenizationReceipt
	rates           map[int64]tokenizationRate
	receiptCounts   map[int64]int
	receiptExpiries expiryQueue
	leaseExpiries   expiryQueue
	active          map[uuid.UUID]coordination.TokenizationLease
	clientInflight  map[int64]int
	backendInflight map[int64]int
}

const tokenizationReceiptsPerClient = 65536

func NewTokenizationCoordinator(policy coordination.TokenizationPolicy, now func() time.Time) *TokenizationCoordinator {
	if now == nil {
		now = time.Now
	}
	return &TokenizationCoordinator{policy: policy, now: now, receipts: make(map[uuid.UUID]*tokenizationReceipt), rates: make(map[int64]tokenizationRate), receiptCounts: make(map[int64]int), active: make(map[uuid.UUID]coordination.TokenizationLease), clientInflight: make(map[int64]int), backendInflight: make(map[int64]int)}
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
	if e := ctx.Err(); e != nil {
		return coordination.TokenizationDecision{}, e
	}
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
	// Local operations have one bounded HTTP lifetime and no durable cross-process
	// recovery. Reject old input after that replay window, so deleting a terminal
	// receipt can never resurrect its operation or spend RPM a second time.
	if !r.ValidAt(now) || !r.OperationStartedAt.After(now.Add(-c.policy.LeaseTTL())) {
		return coordination.TokenizationDecision{Reason: coordination.TokenizationStaleOperation}, nil
	}
	if c.receiptCounts[r.ClientID] >= tokenizationReceiptsPerClient {
		return coordination.TokenizationDecision{Reason: coordination.TokenizationUnavailable}, nil
	}
	rec := &tokenizationReceipt{clientID: r.ClientID, fingerprint: fp, retainUntil: maxTime(now, r.OperationStartedAt).Add(c.policy.LeaseTTL())}
	c.receipts[r.OperationID] = rec
	c.receiptCounts[r.ClientID]++
	c.receiptExpiries.add(admissionReceiptExpiry, r.OperationID, rec.retainUntil)
	reject := func(reason string) (coordination.TokenizationDecision, error) {
		rec.decision.Reason = reason
		return cloneTokenizationDecision(rec.decision), nil
	}
	if c.clientInflight[r.ClientID] >= c.policy.ClientMaxInflight {
		return reject(coordination.TokenizationClientLimit)
	}
	var selected coordination.TokenizationCandidate
	for _, b := range r.Candidates {
		if b.BackendID > 0 && c.backendInflight[b.BackendID] < c.policy.BackendMaxInflight && (selected.BackendID == 0 || c.backendInflight[b.BackendID] < c.backendInflight[selected.BackendID] || c.backendInflight[b.BackendID] == c.backendInflight[selected.BackendID] && b.BackendID < selected.BackendID) {
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
	c.active[r.OperationID] = *rec.decision.Lease
	c.clientInflight[r.ClientID]++
	c.backendInflight[selected.BackendID]++
	c.leaseExpiries.add(admissionLeaseExpiry, r.OperationID, rec.decision.Lease.ExpiresAt)
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
	if rec.decision.Lease == nil || !rec.decision.Lease.Matches(l) {
		return errors.New("tokenization lease identity mismatch")
	}
	rec.completed = true
	c.releaseTokenization(l.OperationID)
	return nil
}
func (c *TokenizationCoordinator) cleanup(now time.Time) {
	for {
		entry, ok := c.leaseExpiries.due(now)
		if !ok {
			break
		}
		c.releaseTokenization(entry.id)
	}
	for {
		entry, ok := c.receiptExpiries.due(now)
		if !ok {
			break
		}
		rec := c.receipts[entry.id]
		if rec == nil {
			continue
		}
		delete(c.receipts, entry.id)
		decrementTokenizationCount(c.receiptCounts, rec.clientID)
	}
	for id, r := range c.rates {
		if now.Sub(r.at) >= 24*time.Hour {
			delete(c.rates, id)
		}
	}
}

func (c *TokenizationCoordinator) releaseTokenization(id uuid.UUID) {
	l, ok := c.active[id]
	if !ok {
		return
	}
	delete(c.active, id)
	decrementTokenizationCount(c.clientInflight, l.ClientID)
	decrementTokenizationCount(c.backendInflight, l.BackendID)
}
func decrementTokenizationCount(counts map[int64]int, id int64) {
	if counts[id] <= 1 {
		delete(counts, id)
	} else {
		counts[id]--
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
