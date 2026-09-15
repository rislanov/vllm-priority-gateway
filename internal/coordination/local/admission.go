package local

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

const receiptRetention = 24 * time.Hour
const defaultLocalAdmissionReceiptCapacity = 65_536

type admissionReceipt struct {
	fingerprint [32]byte
	decision    coordination.AdmissionDecision
	completed   bool
	completion  coordination.CompletionResult
	request     coordination.AdmissionRequest
	expired     bool
	retainUntil time.Time
}
type rateState struct {
	revision int64
	capacity int64
	balance  float64
	at       time.Time
}
type ratePolicy struct {
	revision int64
	rpm      int64
	tpm      int64
}

type AdmissionCoordinator struct {
	mu               sync.Mutex
	now              func() time.Time
	receipts         map[uuid.UUID]*admissionReceipt
	leases           map[uuid.UUID]coordination.LeaseIdentity
	expiries         expiryQueue
	terminalExpiries expiryQueue
	clientInflight   map[int64]int
	poolInflight     map[int64]int
	receiptCapacity  int
	rates            map[int64]map[string]*rateState
	policies         map[int64]ratePolicy
}

func NewAdmissionCoordinator(now func() time.Time) *AdmissionCoordinator {
	return newAdmissionCoordinator(now, defaultLocalAdmissionReceiptCapacity)
}

func newAdmissionCoordinator(now func() time.Time, receiptCapacity int) *AdmissionCoordinator {
	if now == nil {
		now = time.Now
	}
	if receiptCapacity <= 0 {
		receiptCapacity = defaultLocalAdmissionReceiptCapacity
	}
	return &AdmissionCoordinator{
		now: now, receipts: make(map[uuid.UUID]*admissionReceipt), leases: make(map[uuid.UUID]coordination.LeaseIdentity),
		clientInflight: make(map[int64]int), poolInflight: make(map[int64]int),
		receiptCapacity: receiptCapacity, rates: make(map[int64]map[string]*rateState), policies: make(map[int64]ratePolicy),
	}
}

func (c *AdmissionCoordinator) Status() coordination.Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	c.expire(now)
	available := len(c.receipts) < c.receiptCapacity || c.hasTerminalReceipt()
	return coordination.Status{Backend: "local", Available: available, LastSuccess: now}
}

func (c *AdmissionCoordinator) ObserveClientRatePolicy(policy coordination.ClientRatePolicy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observeClientRatePolicy(policy)
}
func (c *AdmissionCoordinator) RefreshInflight(context.Context) error { return nil }
func (c *AdmissionCoordinator) PoolInflight(poolID int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	c.expire(now)
	return c.poolInflight[poolID]
}

func (c *AdmissionCoordinator) Acquire(ctx context.Context, req coordination.AdmissionRequest) (coordination.AdmissionDecision, error) {
	if err := ctx.Err(); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	fp := fingerprint(req)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	c.expire(now)
	if old := c.receipts[req.LeaseID]; old != nil {
		if old.fingerprint != fp {
			return coordination.AdmissionDecision{Reason: coordination.ReasonIdempotencyConflict}, nil
		}
		if old.completed || old.expired {
			return coordination.AdmissionDecision{Reason: coordination.ReasonLeaseLost}, nil
		}
		if old.decision.Lease != nil {
			if lease, ok := c.leases[req.LeaseID]; ok && lease.ExpiresAt.After(now) {
				copy := lease
				return coordination.AdmissionDecision{Lease: &copy}, nil
			}
			return coordination.AdmissionDecision{Reason: coordination.ReasonLeaseLost}, nil
		}
		return old.decision, nil
	}
	if !c.makeReceiptRoom() {
		return coordination.AdmissionDecision{Reason: coordination.ReasonCoordinationUnavailable}, nil
	}
	receipt := &admissionReceipt{fingerprint: fp, request: req}
	c.receipts[req.LeaseID] = receipt
	reject := func(reason coordination.Reason, retry *time.Time) (coordination.AdmissionDecision, error) {
		d := coordination.AdmissionDecision{Reason: reason, RetryAt: retry}
		receipt.decision = d
		receipt.retainUntil = retentionBoundary(req.OperationStartedAt, now)
		c.terminalExpiries.add(admissionReceiptExpiry, req.LeaseID, receipt.retainUntil)
		return d, nil
	}
	if req.OperationStartedAt.After(now.Add(5*time.Minute)) || !req.OperationStartedAt.After(now.Add(-receiptRetention)) {
		return reject(coordination.ReasonStaleOperation, nil)
	}
	policy := c.observeRatePolicy(req)
	if req.EffectiveClientLimit <= 0 || req.EffectiveClientLimit > req.ConfiguredClientLimit {
		return reject(coordination.ReasonConcurrencyExhausted, nil)
	}
	clientCount, poolCount := c.clientInflight[req.ClientID], c.poolInflight[req.PoolID]
	if req.PoolGatewayInflightLimit > 0 && poolCount >= req.PoolGatewayInflightLimit {
		return reject(coordination.ReasonConcurrencyExhausted, nil)
	}
	if clientCount >= req.EffectiveClientLimit {
		return reject(coordination.ReasonConcurrencyExhausted, nil)
	}
	if policy.tpm > 0 {
		state := c.normalized(req.ClientID, "tpm", policy.revision, policy.tpm, now)
		if state.balance < 1 {
			retry := retryAt(state.balance, policy.tpm, now)
			return reject(coordination.ReasonTPMExhausted, &retry)
		}
	} else {
		c.normalized(req.ClientID, "tpm", policy.revision, 0, now)
	}
	if policy.rpm > 0 {
		state := c.normalized(req.ClientID, "rpm", policy.revision, policy.rpm, now)
		if state.balance < 1 {
			retry := retryAt(state.balance, policy.rpm, now)
			return reject(coordination.ReasonRPMExhausted, &retry)
		}
		state.balance--
	} else {
		c.normalized(req.ClientID, "rpm", policy.revision, 0, now)
	}
	lease := coordination.LeaseIdentity{LeaseID: req.LeaseID, ClientID: req.ClientID, PoolID: req.PoolID, RequestID: req.RequestID, ReplicaID: req.ReplicaID, ExpiresAt: now.Add(req.LeaseTTL), TTL: req.LeaseTTL}
	c.leases[req.LeaseID] = lease
	c.clientInflight[lease.ClientID]++
	c.poolInflight[lease.PoolID]++
	c.expiries.add(admissionLeaseExpiry, lease.LeaseID, lease.ExpiresAt)
	receipt.decision = coordination.AdmissionDecision{Lease: &lease}
	return receipt.decision, nil
}

func (c *AdmissionCoordinator) Renew(ctx context.Context, leases []coordination.LeaseIdentity) ([]coordination.RenewResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	c.expire(now)
	out := make([]coordination.RenewResult, len(leases))
	for i, want := range leases {
		got, ok := c.leases[want.LeaseID]
		if !ok || got.ClientID != want.ClientID || got.PoolID != want.PoolID || !got.ExpiresAt.After(now) {
			c.removeLease(want.LeaseID)
			out[i] = coordination.RenewResult{Lease: want, Reason: coordination.ReasonLeaseLost}
			continue
		}
		ttl := got.TTL
		if ttl <= 0 {
			ttl = want.TTL
		}
		if ttl <= 0 {
			out[i] = coordination.RenewResult{Lease: want, Reason: coordination.ReasonLeaseLost}
			continue
		}
		got.ExpiresAt = now.Add(ttl)
		c.leases[want.LeaseID] = got
		c.expiries.add(admissionLeaseExpiry, got.LeaseID, got.ExpiresAt)
		out[i] = coordination.RenewResult{Lease: got}
	}
	return out, nil
}

func (c *AdmissionCoordinator) Complete(ctx context.Context, completions []coordination.LeaseCompletion) ([]coordination.CompleteResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	c.expire(now)
	out := make([]coordination.CompleteResult, len(completions))
	for i, item := range completions {
		rec := c.receipts[item.Lease.LeaseID]
		if rec == nil {
			out[i] = coordination.CompleteResult{LeaseID: item.Lease.LeaseID, Reason: coordination.ReasonStaleOperation}
			continue
		}
		if rec.decision.Lease == nil {
			out[i] = coordination.CompleteResult{LeaseID: item.Lease.LeaseID, Result: coordination.CompletionLeaseLost, Reason: coordination.ReasonLeaseLost}
			continue
		}
		if rec.completed {
			out[i] = coordination.CompleteResult{LeaseID: item.Lease.LeaseID, Result: coordination.CompletionAlreadyCompleted, OriginalResult: rec.completion}
			continue
		}
		lease, active := c.leases[item.Lease.LeaseID]
		active = active && lease.ClientID == rec.request.ClientID && lease.PoolID == rec.request.PoolID && lease.ExpiresAt.After(now)
		c.removeLease(item.Lease.LeaseID)
		if item.Usage != nil && item.Usage.Valid() {
			policy, ok := c.policies[rec.request.ClientID]
			if !ok {
				policy = ratePolicy{revision: rec.request.ClientPolicyRevision, rpm: rec.request.RequestsPerMinute, tpm: rec.request.TokensPerMinute}
			}
			state := c.normalized(rec.request.ClientID, "tpm", policy.revision, policy.tpm, now)
			if policy.tpm > 0 {
				state.balance -= (float64(item.Usage.InputTokens) + float64(item.Usage.OutputTokens))
			}
		}
		result := coordination.CompletionLeaseLost
		if active {
			result = coordination.CompletionReleased
		}
		rec.completed = true
		rec.completion = result
		rec.retainUntil = retentionBoundary(rec.request.OperationStartedAt, now)
		c.terminalExpiries.add(admissionReceiptExpiry, item.Lease.LeaseID, rec.retainUntil)
		out[i] = coordination.CompleteResult{LeaseID: item.Lease.LeaseID, Result: result}
	}
	return out, nil
}

func (c *AdmissionCoordinator) normalized(clientID int64, kind string, revision, capacity int64, now time.Time) *rateState {
	if c.rates[clientID] == nil {
		c.rates[clientID] = make(map[string]*rateState)
	}
	s := c.rates[clientID][kind]
	if s == nil || s.revision != revision || s.capacity != capacity {
		s = &rateState{revision: revision, capacity: capacity, balance: float64(capacity), at: now}
		c.rates[clientID][kind] = s
		return s
	}
	elapsed := now.Sub(s.at).Seconds()
	if elapsed > 0 {
		s.balance += elapsed * float64(capacity) / 60
		if s.balance > float64(capacity) {
			s.balance = float64(capacity)
		}
		s.at = now
	}
	return s
}

func (c *AdmissionCoordinator) observeRatePolicy(request coordination.AdmissionRequest) ratePolicy {
	return c.observeClientRatePolicy(coordination.ClientRatePolicy{
		ClientID: request.ClientID, Revision: request.ClientPolicyRevision,
		RequestsPerMinute: request.RequestsPerMinute, TokensPerMinute: request.TokensPerMinute,
	})
}

func (c *AdmissionCoordinator) observeClientRatePolicy(candidate coordination.ClientRatePolicy) ratePolicy {
	policy, ok := c.policies[candidate.ClientID]
	if !ok || candidate.Revision > policy.revision {
		policy = ratePolicy{revision: candidate.Revision, rpm: candidate.RequestsPerMinute, tpm: candidate.TokensPerMinute}
		c.policies[candidate.ClientID] = policy
	}
	return policy
}
func (c *AdmissionCoordinator) expire(now time.Time) {
	for {
		expiry, ok := c.expiries.due(now)
		if !ok {
			break
		}
		switch expiry.kind {
		case admissionLeaseExpiry:
			lease, exists := c.leases[expiry.id]
			if !exists || !lease.ExpiresAt.Equal(expiry.at) || lease.ExpiresAt.After(now) {
				continue
			}
			c.removeLease(expiry.id)
			if receipt := c.receipts[expiry.id]; receipt != nil && !receipt.completed {
				receipt.expired = true
				receipt.retainUntil = retentionBoundary(receipt.request.OperationStartedAt, now)
				c.terminalExpiries.add(admissionReceiptExpiry, expiry.id, receipt.retainUntil)
			}
		}
	}
	for {
		expiry, ok := c.terminalExpiries.due(now)
		if !ok {
			return
		}
		receipt := c.receipts[expiry.id]
		if receipt != nil && receipt.retainUntil.Equal(expiry.at) && !receipt.retainUntil.After(now) {
			delete(c.receipts, expiry.id)
		}
	}
}

func (c *AdmissionCoordinator) makeReceiptRoom() bool {
	for len(c.receipts) >= c.receiptCapacity {
		if !c.hasTerminalReceipt() {
			return false
		}
		expiry, ok := c.terminalExpiries.popOldest()
		if !ok {
			return false
		}
		delete(c.receipts, expiry.id)
	}
	return true
}

func (c *AdmissionCoordinator) hasTerminalReceipt() bool {
	for c.terminalExpiries.Len() > 0 {
		expiry := c.terminalExpiries[0]
		receipt := c.receipts[expiry.id]
		if receipt != nil && !receipt.retainUntil.IsZero() && receipt.retainUntil.Equal(expiry.at) {
			return true
		}
		_, _ = c.terminalExpiries.popOldest()
	}
	return false
}

func (c *AdmissionCoordinator) removeLease(id uuid.UUID) {
	lease, exists := c.leases[id]
	if !exists {
		return
	}
	delete(c.leases, id)
	if c.clientInflight[lease.ClientID] <= 1 {
		delete(c.clientInflight, lease.ClientID)
	} else {
		c.clientInflight[lease.ClientID]--
	}
	if c.poolInflight[lease.PoolID] <= 1 {
		delete(c.poolInflight, lease.PoolID)
	} else {
		c.poolInflight[lease.PoolID]--
	}
}

func retentionBoundary(operationStarted, terminal time.Time) time.Time {
	if operationStarted.After(terminal) {
		terminal = operationStarted
	}
	return terminal.Add(receiptRetention)
}
func fingerprint(v any) [32]byte { encoded, _ := json.Marshal(v); return sha256.Sum256(encoded) }
func retryAt(balance float64, capacity int64, now time.Time) time.Time {
	return coordination.RateRetryAt(balance, capacity, now)
}

var _ coordination.AdmissionCoordinator = (*AdmissionCoordinator)(nil)
