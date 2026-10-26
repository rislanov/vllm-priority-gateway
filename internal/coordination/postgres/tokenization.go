package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

type TokenizationCoordinator struct {
	pool    *pgxpool.Pool
	policy  coordination.TokenizationPolicy
	timeout time.Duration
	mu      sync.RWMutex
	status  coordination.Status
}

func NewTokenizationCoordinator(s *pgstore.Store, p coordination.TokenizationPolicy, timeout time.Duration) *TokenizationCoordinator {
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	return &TokenizationCoordinator{pool: s.CoordinationPool(), policy: p, timeout: timeout, status: coordination.Status{Backend: "postgres", Available: p.Validate() == nil}}
}
func (c *TokenizationCoordinator) Status() coordination.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}
func (c *TokenizationCoordinator) observe(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status.Available = err == nil
	c.status.Degraded = err != nil
	if err == nil {
		c.status.LastSuccess = time.Now().UTC()
	}
}
func (c *TokenizationCoordinator) Acquire(parent context.Context, r coordination.TokenizationAcquireRequest) (coordination.TokenizationDecision, error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	if c.policy.Validate() != nil {
		return coordination.TokenizationDecision{Reason: coordination.TokenizationUnavailable}, nil
	}
	// Both attempts use the same durable receipt, including after a lost COMMIT response.
	var d coordination.TokenizationDecision
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		d, err = c.acquireTokenization(ctx, r)
		if err == nil || ctx.Err() != nil {
			break
		}
	}
	c.observe(err)
	return d, err
}
func (c *TokenizationCoordinator) acquireTokenization(ctx context.Context, r coordination.TokenizationAcquireRequest) (coordination.TokenizationDecision, error) {
	tx, e := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return coordination.TokenizationDecision{}, e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); e != nil {
		return coordination.TokenizationDecision{}, e
	}
	var now time.Time
	if e = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); e != nil {
		return coordination.TokenizationDecision{}, e
	}
	if !r.ValidAt(now) {
		return coordination.TokenizationDecision{Reason: coordination.TokenizationStaleOperation}, nil
	}
	fp := r.Fingerprint()
	retain := now
	if r.OperationStartedAt.After(retain) {
		retain = r.OperationStartedAt
	}
	retain = retain.Add(24 * time.Hour)
	tag, e := tx.Exec(ctx, `INSERT INTO tokenization_operations(operation_id,input_fingerprint,decision,retain_until) VALUES($1,$2,'{}',$3) ON CONFLICT DO NOTHING`, r.OperationID, fp[:], retain)
	if e != nil {
		return coordination.TokenizationDecision{}, e
	}
	if tag.RowsAffected() == 0 {
		var storedFP, encoded []byte
		var completed bool
		if e = tx.QueryRow(ctx, "SELECT input_fingerprint,decision,completed FROM tokenization_operations WHERE operation_id=$1 FOR UPDATE", r.OperationID).Scan(&storedFP, &encoded, &completed); e != nil {
			return coordination.TokenizationDecision{}, e
		}
		if !bytes.Equal(storedFP, fp[:]) {
			return coordination.TokenizationDecision{Reason: coordination.TokenizationConflict}, nil
		}
		var d coordination.TokenizationDecision
		if e = json.Unmarshal(encoded, &d); e != nil {
			return d, e
		}
		if e = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); e != nil {
			return d, e
		}
		if completed || d.Lease != nil && !d.Lease.ExpiresAt.After(now) {
			return coordination.TokenizationDecision{Reason: coordination.TokenizationLeaseLost}, nil
		}
		return d, tx.Commit(ctx)
	}
	finish := func(d coordination.TokenizationDecision) (coordination.TokenizationDecision, error) {
		b, e := json.Marshal(d)
		if e != nil {
			return d, e
		}
		if _, e = tx.Exec(ctx, "UPDATE tokenization_operations SET decision=$2::jsonb WHERE operation_id=$1", r.OperationID, string(b)); e != nil {
			return d, e
		}
		return d, tx.Commit(ctx)
	}
	reject := func(reason string) (coordination.TokenizationDecision, error) {
		return finish(coordination.TokenizationDecision{Reason: reason})
	}
	if _, e = tx.Exec(ctx, "INSERT INTO tokenization_client_scopes VALUES($1) ON CONFLICT DO NOTHING", r.ClientID); e != nil {
		return coordination.TokenizationDecision{}, e
	}
	if _, e = tx.Exec(ctx, "SELECT client_id FROM tokenization_client_scopes WHERE client_id=$1 FOR UPDATE", r.ClientID); e != nil {
		return coordination.TokenizationDecision{}, e
	}
	candidates := append([]coordination.TokenizationCandidate(nil), r.Candidates...)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].BackendID < candidates[j].BackendID })
	for _, b := range candidates {
		if _, e = tx.Exec(ctx, "INSERT INTO tokenization_backend_scopes VALUES($1) ON CONFLICT DO NOTHING", b.BackendID); e != nil {
			return coordination.TokenizationDecision{}, e
		}
		if _, e = tx.Exec(ctx, "SELECT backend_id FROM tokenization_backend_scopes WHERE backend_id=$1 FOR UPDATE", b.BackendID); e != nil {
			return coordination.TokenizationDecision{}, e
		}
	}
	// Lock authoritative entities in configuration mutation order: pool, client,
	// key, access, then backends. No inference scopes or circuits are acquired.
	var poolRevision int64
	var enabled bool
	e = tx.QueryRow(ctx, "SELECT revision,enabled FROM model_pools WHERE id=$1 FOR SHARE", r.PoolID).Scan(&poolRevision, &enabled)
	if errors.Is(e, pgx.ErrNoRows) {
		return reject(coordination.TokenizationStaleConfiguration)
	}
	if e != nil {
		return coordination.TokenizationDecision{}, e
	}
	if !enabled || poolRevision != r.PoolRevision {
		return reject(coordination.TokenizationStaleConfiguration)
	}
	e = tx.QueryRow(ctx, "SELECT enabled FROM clients WHERE id=$1 FOR SHARE", r.ClientID).Scan(&enabled)
	if errors.Is(e, pgx.ErrNoRows) {
		return reject(coordination.TokenizationStaleConfiguration)
	}
	if e != nil {
		return coordination.TokenizationDecision{}, e
	}
	if !enabled {
		return reject(coordination.TokenizationStaleConfiguration)
	}
	var keyClient int64
	var revoked, expires *time.Time
	e = tx.QueryRow(ctx, "SELECT client_id,revoked_at,expires_at FROM api_keys WHERE id=$1 FOR SHARE", r.APIKeyID).Scan(&keyClient, &revoked, &expires)
	if errors.Is(e, pgx.ErrNoRows) {
		return reject(coordination.TokenizationStaleConfiguration)
	}
	if e != nil {
		return coordination.TokenizationDecision{}, e
	}
	e = tx.QueryRow(ctx, "SELECT enabled FROM client_model_access WHERE client_id=$1 AND model_pool_id=$2 FOR SHARE", r.ClientID, r.PoolID).Scan(&enabled)
	if errors.Is(e, pgx.ErrNoRows) {
		return reject(coordination.TokenizationStaleConfiguration)
	}
	if e != nil {
		return coordination.TokenizationDecision{}, e
	}
	if !enabled || revoked != nil || keyClient != r.ClientID {
		return reject(coordination.TokenizationStaleConfiguration)
	}
	eligible := make([]coordination.TokenizationCandidate, 0, len(candidates))
	for _, b := range candidates {
		var revision, pool int64
		var active, drain bool
		e = tx.QueryRow(ctx, "SELECT revision,model_pool_id,enabled,draining FROM backends WHERE id=$1 FOR SHARE", b.BackendID).Scan(&revision, &pool, &active, &drain)
		if errors.Is(e, pgx.ErrNoRows) {
			continue
		}
		if e != nil {
			return coordination.TokenizationDecision{}, e
		}
		if revision == b.BackendRevision && pool == r.PoolID && active && !drain {
			eligible = append(eligible, b)
		}
	}
	var revision int64
	if e = tx.QueryRow(ctx, "SELECT revision,clock_timestamp() FROM config_meta WHERE singleton=1").Scan(&revision, &now); e != nil {
		return coordination.TokenizationDecision{}, e
	}
	if revision != r.ConfigurationRevision || expires != nil && !expires.After(now) {
		return reject(coordination.TokenizationStaleConfiguration)
	}
	if !r.ValidAt(now) {
		return reject(coordination.TokenizationStaleOperation)
	}
	var clientCount int
	if e = tx.QueryRow(ctx, "SELECT count(*) FROM tokenization_leases WHERE client_id=$1 AND expires_at>$2", r.ClientID, now).Scan(&clientCount); e != nil {
		return coordination.TokenizationDecision{}, e
	}
	if clientCount >= c.policy.ClientMaxInflight {
		return reject(coordination.TokenizationClientLimit)
	}
	var selected coordination.TokenizationCandidate
	best := math.MaxInt
	for _, b := range eligible {
		var n int
		if e = tx.QueryRow(ctx, "SELECT count(*) FROM tokenization_leases WHERE backend_id=$1 AND expires_at>$2", b.BackendID, now).Scan(&n); e != nil {
			return coordination.TokenizationDecision{}, e
		}
		if n < c.policy.BackendMaxInflight && n < best {
			selected = b
			best = n
		}
	}
	if selected.BackendID == 0 {
		return reject(coordination.TokenizationBackendLimit)
	}
	if _, e = tx.Exec(ctx, "INSERT INTO tokenization_rate_state VALUES($1,$2,$3) ON CONFLICT DO NOTHING", r.ClientID, c.policy.Burst(), now); e != nil {
		return coordination.TokenizationDecision{}, e
	}
	var balance float64
	var refill time.Time
	if e = tx.QueryRow(ctx, "SELECT balance,last_refill_at FROM tokenization_rate_state WHERE client_id=$1 FOR UPDATE", r.ClientID).Scan(&balance, &refill); e != nil {
		return coordination.TokenizationDecision{}, e
	}
	balance = min(c.policy.Burst(), balance+max(0, now.Sub(refill).Seconds())*float64(c.policy.ClientRPM)/60)
	if balance < 1 {
		retry := now.Add(time.Duration((1 - balance) * 60 / float64(c.policy.ClientRPM) * float64(time.Second)))
		return finish(coordination.TokenizationDecision{Reason: coordination.TokenizationRateLimit, RetryAt: &retry})
	}
	if _, e = tx.Exec(ctx, "UPDATE tokenization_rate_state SET balance=$2,last_refill_at=$3 WHERE client_id=$1", r.ClientID, balance-1, now); e != nil {
		return coordination.TokenizationDecision{}, e
	}
	l := &coordination.TokenizationLease{OperationID: r.OperationID, ReplicaID: r.ReplicaID, ClientID: r.ClientID, PoolID: r.PoolID, BackendID: selected.BackendID, BackendRevision: selected.BackendRevision, AcquiredAt: now, ExpiresAt: now.Add(c.policy.LeaseTTL())}
	if _, e = tx.Exec(ctx, "INSERT INTO tokenization_leases VALUES($1,$2,$3,$4)", r.OperationID, r.ClientID, l.BackendID, l.ExpiresAt); e != nil {
		return coordination.TokenizationDecision{}, e
	}
	return finish(coordination.TokenizationDecision{Lease: l})
}
func (c *TokenizationCoordinator) Complete(parent context.Context, l coordination.TokenizationLease) (err error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	defer func() { c.observe(err) }()
	tx, e := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); e != nil {
		return e
	}
	var encoded []byte
	if e = tx.QueryRow(ctx, "SELECT decision FROM tokenization_operations WHERE operation_id=$1 FOR UPDATE", l.OperationID).Scan(&encoded); e != nil {
		return e
	}
	var d coordination.TokenizationDecision
	if e = json.Unmarshal(encoded, &d); e != nil {
		return e
	}
	if d.Lease == nil || *d.Lease != l {
		return errors.New("tokenization lease identity mismatch")
	}
	if _, e = tx.Exec(ctx, "DELETE FROM tokenization_leases WHERE operation_id=$1", l.OperationID); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "UPDATE tokenization_operations SET completed=true WHERE operation_id=$1", l.OperationID); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
