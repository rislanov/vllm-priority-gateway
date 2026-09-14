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
	balance  float64
	at       time.Time
}

type AdmissionCoordinator struct {
	mu       sync.Mutex
	now      func() time.Time
	receipts map[uuid.UUID]*admissionReceipt
	leases   map[uuid.UUID]coordination.LeaseIdentity
	rates    map[int64]map[string]*rateState
}

func NewAdmissionCoordinator(now func() time.Time) *AdmissionCoordinator {
	if now == nil {
		now = time.Now
	}
	return &AdmissionCoordinator{now: now, receipts: make(map[uuid.UUID]*admissionReceipt), leases: make(map[uuid.UUID]coordination.LeaseIdentity), rates: make(map[int64]map[string]*rateState)}
}

func (c *AdmissionCoordinator) Status() coordination.Status {
	return coordination.Status{Backend: "local", Available: true, LastSuccess: c.now().UTC()}
}
func (c *AdmissionCoordinator) RefreshInflight(context.Context) error { return nil }
func (c *AdmissionCoordinator) PoolInflight(poolID int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	c.expire(now)
	count := 0
	for _, lease := range c.leases {
		if lease.PoolID == poolID && lease.ExpiresAt.After(now) {
			count++
		}
	}
	return count
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
	receipt := &admissionReceipt{fingerprint: fp, request: req}
	c.receipts[req.LeaseID] = receipt
	reject := func(reason coordination.Reason, retry *time.Time) (coordination.AdmissionDecision, error) {
		d := coordination.AdmissionDecision{Reason: reason, RetryAt: retry}
		receipt.decision = d
		receipt.retainUntil = retentionBoundary(req.OperationStartedAt, now)
		return d, nil
	}
	if req.OperationStartedAt.After(now.Add(5*time.Minute)) || !req.OperationStartedAt.After(now.Add(-receiptRetention)) {
		return reject(coordination.ReasonStaleOperation, nil)
	}
	if req.EffectiveClientLimit <= 0 || req.EffectiveClientLimit > req.ConfiguredClientLimit {
		return reject(coordination.ReasonConcurrencyExhausted, nil)
	}
	var clientCount, poolCount int
	for _, lease := range c.leases {
		if !lease.ExpiresAt.After(now) {
			continue
		}
		if lease.ClientID == req.ClientID {
			clientCount++
		}
		if lease.PoolID == req.PoolID {
			poolCount++
		}
	}
	if req.PoolGatewayInflightLimit > 0 && poolCount >= req.PoolGatewayInflightLimit {
		return reject(coordination.ReasonConcurrencyExhausted, nil)
	}
	if clientCount >= req.EffectiveClientLimit {
		return reject(coordination.ReasonConcurrencyExhausted, nil)
	}
	if req.TokensPerMinute > 0 {
		state := c.normalized(req.ClientID, "tpm", req.ClientPolicyRevision, req.TokensPerMinute, now)
		if state.balance < 1 {
			retry := retryAt(state.balance, req.TokensPerMinute, now)
			return reject(coordination.ReasonTPMExhausted, &retry)
		}
	}
	if req.RequestsPerMinute > 0 {
		state := c.normalized(req.ClientID, "rpm", req.ClientPolicyRevision, req.RequestsPerMinute, now)
		if state.balance < 1 {
			retry := retryAt(state.balance, req.RequestsPerMinute, now)
			return reject(coordination.ReasonRPMExhausted, &retry)
		}
		state.balance--
	}
	lease := coordination.LeaseIdentity{LeaseID: req.LeaseID, ClientID: req.ClientID, PoolID: req.PoolID, RequestID: req.RequestID, ReplicaID: req.ReplicaID, ExpiresAt: now.Add(req.LeaseTTL), TTL: req.LeaseTTL}
	c.leases[req.LeaseID] = lease
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
	out := make([]coordination.RenewResult, len(leases))
	for i, want := range leases {
		got, ok := c.leases[want.LeaseID]
		if !ok || got.ClientID != want.ClientID || got.PoolID != want.PoolID || !got.ExpiresAt.After(now) {
			delete(c.leases, want.LeaseID)
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
		delete(c.leases, item.Lease.LeaseID)
		if item.Usage != nil && item.Usage.Valid() && rec.request.TokensPerMinute > 0 {
			state := c.normalized(rec.request.ClientID, "tpm", rec.request.ClientPolicyRevision, rec.request.TokensPerMinute, now)
			state.balance -= float64(item.Usage.InputTokens + item.Usage.OutputTokens)
		}
		result := coordination.CompletionLeaseLost
		if active {
			result = coordination.CompletionReleased
		}
		rec.completed = true
		rec.completion = result
		rec.retainUntil = retentionBoundary(rec.request.OperationStartedAt, now)
		out[i] = coordination.CompleteResult{LeaseID: item.Lease.LeaseID, Result: result}
	}
	return out, nil
}

func (c *AdmissionCoordinator) normalized(clientID int64, kind string, revision, capacity int64, now time.Time) *rateState {
	if c.rates[clientID] == nil {
		c.rates[clientID] = make(map[string]*rateState)
	}
	s := c.rates[clientID][kind]
	if s == nil || s.revision != revision {
		s = &rateState{revision: revision, balance: float64(capacity), at: now}
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
func (c *AdmissionCoordinator) expire(now time.Time) {
	for id, l := range c.leases {
		if !l.ExpiresAt.After(now) {
			delete(c.leases, id)
			if receipt := c.receipts[id]; receipt != nil && !receipt.completed {
				receipt.expired = true
				receipt.retainUntil = retentionBoundary(receipt.request.OperationStartedAt, now)
			}
		}
	}
	for id, receipt := range c.receipts {
		if !receipt.retainUntil.IsZero() && !receipt.retainUntil.After(now) {
			delete(c.receipts, id)
		}
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
	missing := 1 - balance
	if missing < 0 {
		missing = 0
	}
	return now.Add(time.Duration(missing / float64(capacity) * float64(time.Minute)))
}

var _ coordination.AdmissionCoordinator = (*AdmissionCoordinator)(nil)
