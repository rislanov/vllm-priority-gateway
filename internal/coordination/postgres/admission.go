package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

type AdmissionCoordinator struct {
	pool     *pgxpool.Pool
	timeout  time.Duration
	mu       sync.RWMutex
	status   coordination.Status
	inflight map[int64]int
	failures failurePublisher
}

func NewAdmissionCoordinator(store *pgstore.Store, timeout time.Duration) *AdmissionCoordinator {
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	return &AdmissionCoordinator{pool: store.CoordinationPool(), timeout: timeout, status: coordination.Status{Backend: "postgres", Available: true}, inflight: make(map[int64]int)}
}
func (c *AdmissionCoordinator) PoolInflight(poolID int64) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.inflight[poolID]
}
func (c *AdmissionCoordinator) RefreshInflight(parent context.Context) (resultErr error) {
	defer func() {
		if resultErr == nil {
			c.setStatus(true, "")
			return
		}
		resultErr = recordCoordinationError(parent, resultErr, c)
	}()
	ctx, cancel := c.deadline(parent)
	defer cancel()
	rows, err := c.pool.Query(ctx, "SELECT pool_id,count(*) FROM request_leases WHERE expires_at>clock_timestamp() GROUP BY pool_id")
	if err != nil {
		return err
	}
	defer rows.Close()
	next := make(map[int64]int)
	for rows.Next() {
		var id int64
		var count int
		if err := rows.Scan(&id, &count); err != nil {
			return err
		}
		next[id] = count
	}
	if err := rows.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.inflight = next
	c.mu.Unlock()
	return nil
}
func (c *AdmissionCoordinator) Status() coordination.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}
func (c *AdmissionCoordinator) SetFailureObserver(observer coordination.CoordinationFailureObserver) {
	c.failures.set(observer)
}
func (c *AdmissionCoordinator) setStatus(ok bool, reason coordination.Reason) {
	c.mu.Lock()
	if c.status.Permanent {
		c.mu.Unlock()
		return
	}
	c.status.Available = ok
	c.status.Degraded = !ok
	c.status.Reason = reason
	if ok {
		c.status.LastSuccess = time.Now().UTC()
	}
	c.mu.Unlock()
	if !ok {
		c.failures.unavailable()
	}
}
func (c *AdmissionCoordinator) setPermanent() {
	c.mu.Lock()
	c.status.Available = false
	c.status.Degraded = false
	c.status.Permanent = true
	c.status.Reason = coordination.ReasonCoordinationUnavailable
	c.mu.Unlock()
	c.failures.permanent()
}
func (c *AdmissionCoordinator) deadline(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *AdmissionCoordinator) Acquire(parent context.Context, r coordination.AdmissionRequest) (coordination.AdmissionDecision, error) {
	ctx, cancel := c.deadline(parent)
	defer cancel()
	var d coordination.AdmissionDecision
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		d, err = c.acquire(ctx, r)
		if err == nil || !retryableCoordinationError(err) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		err = recordCoordinationError(parent, err, c)
		return coordination.AdmissionDecision{Reason: coordination.ReasonCoordinationUnavailable}, err
	}
	c.setStatus(true, "")
	_ = c.RefreshInflight(parent)
	return d, nil
}
func (c *AdmissionCoordinator) acquire(ctx context.Context, r coordination.AdmissionRequest) (coordination.AdmissionDecision, error) {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return coordination.AdmissionDecision{}, err
	}
	defer tx.Rollback(ctx)
	fp := admissionFingerprint(r)
	initial := &pgx.Batch{}
	initial.Queue("SET LOCAL synchronous_commit=on")
	initial.Queue(`INSERT INTO admission_operations(lease_id,operation_started_at,input_fingerprint,request_id,replica_id,client_id,pool_id,decision) VALUES($1::uuid,$2::timestamptz,$3::bytea,$4::text,$5::uuid,$6::bigint,$7::bigint,'pending') ON CONFLICT DO NOTHING`, r.LeaseID, r.OperationStartedAt.UTC(), fp[:], r.RequestID, r.ReplicaID, r.ClientID, r.PoolID)
	initialResults := tx.SendBatch(ctx, initial)
	if _, err = initialResults.Exec(); err != nil {
		_ = initialResults.Close()
		return coordination.AdmissionDecision{}, err
	}
	tag, err := initialResults.Exec()
	closeErr := initialResults.Close()
	if err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if closeErr != nil {
		return coordination.AdmissionDecision{}, closeErr
	}
	created := tag.RowsAffected() == 1
	if !created {
		return c.resolveAcquire(ctx, tx, r, fp)
	}
	var revision, clientRevision int64
	var keyClient int64
	var clientEnabled, poolEnabled bool
	var configuredClient, poolLimit int
	var rpm, tpm int64
	var keyExpires *time.Time
	var now time.Time
	var poolCount, clientCount int
	var configurationMissing bool
	// Send ordered statements together to avoid network waits while holding the
	// shared pool scope. Locks and counts must remain separate SQL statements:
	// READ COMMITTED needs a fresh snapshot after a contended lock is acquired.
	checks := &pgx.Batch{}
	checks.Queue("SELECT pool_id FROM pool_admission_scopes WHERE pool_id=$1::bigint FOR UPDATE", r.PoolID)
	checks.Queue("SELECT client_id FROM client_admission_scopes WHERE client_id=$1::bigint FOR UPDATE", r.ClientID)
	checks.Queue(`SELECT m.revision,c.revision,c.enabled,c.max_concurrency,c.requests_per_minute,c.tokens_per_minute,k.client_id,k.expires_at,p.enabled,p.max_gateway_inflight FROM config_meta m JOIN clients c ON c.id=$1::bigint JOIN api_keys k ON k.id=$2::bigint JOIN model_pools p ON p.id=$3::bigint WHERE m.singleton=1 AND k.revoked_at IS NULL FOR SHARE OF c,k,p`, r.ClientID, r.APIKeyID, r.PoolID).QueryRow(func(row pgx.Row) error {
		err := row.Scan(&revision, &clientRevision, &clientEnabled, &configuredClient, &rpm, &tpm, &keyClient, &keyExpires, &poolEnabled, &poolLimit)
		configurationMissing = errors.Is(err, pgx.ErrNoRows)
		if configurationMissing {
			return nil
		}
		return err
	})
	// Seed using the locked database policy, never the request's policy. Both
	// rate kinds share the client scope, including with lease completion.
	checks.Queue(`INSERT INTO coordination_rate_state(client_id,rate_kind,policy_revision,balance,last_refill_at)
		SELECT c.id,v.kind,c.revision,v.capacity::double precision,$2::timestamptz
		FROM clients c CROSS JOIN LATERAL (VALUES ('rpm',c.requests_per_minute),('tpm',c.tokens_per_minute)) v(kind,capacity)
		WHERE c.id=$1::bigint AND v.capacity>0 ON CONFLICT DO NOTHING`, r.ClientID, time.Unix(0, 0).UTC())
	checks.Queue("SELECT client_id FROM coordination_rate_state WHERE client_id=$1::bigint ORDER BY rate_kind FOR UPDATE", r.ClientID)
	checks.Queue(`WITH at AS MATERIALIZED (SELECT clock_timestamp() AS now)
		SELECT at.now,
		(SELECT count(*) FROM request_leases WHERE pool_id=$1::bigint AND expires_at>at.now),
		(SELECT count(*) FROM request_leases WHERE client_id=$2::bigint AND expires_at>at.now) FROM at`, r.PoolID, r.ClientID).QueryRow(func(row pgx.Row) error {
		return row.Scan(&now, &poolCount, &clientCount)
	})
	if err = tx.SendBatch(ctx, checks).Close(); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if configurationMissing {
		return rejectAcquire(ctx, tx, r.LeaseID, coordination.ReasonStaleConfiguration, nil, now)
	}
	if revision != r.ConfigurationRevision || keyClient != r.ClientID || !clientEnabled || !poolEnabled || clientRevision != r.ClientPolicyRevision || configuredClient != r.ConfiguredClientLimit || poolLimit != r.PoolGatewayInflightLimit || rpm != r.RequestsPerMinute || tpm != r.TokensPerMinute || (keyExpires != nil && !keyExpires.After(now)) {
		return rejectAcquire(ctx, tx, r.LeaseID, coordination.ReasonStaleConfiguration, nil, now)
	}
	if r.EffectiveClientLimit <= 0 || r.EffectiveClientLimit > configuredClient {
		return rejectAcquire(ctx, tx, r.LeaseID, coordination.ReasonConcurrencyExhausted, nil, now)
	}
	if r.OperationStartedAt.After(now.Add(5*time.Minute)) || !r.OperationStartedAt.After(now.Add(-24*time.Hour)) {
		return rejectAcquire(ctx, tx, r.LeaseID, coordination.ReasonStaleOperation, nil, now)
	}
	if poolLimit > 0 && poolCount >= poolLimit {
		return rejectAcquire(ctx, tx, r.LeaseID, coordination.ReasonConcurrencyExhausted, nil, now, coordination.AdmissionPoolScope)
	}
	if clientCount >= r.EffectiveClientLimit {
		return rejectAcquire(ctx, tx, r.LeaseID, coordination.ReasonConcurrencyExhausted, nil, now)
	}
	if tpm > 0 {
		ok, retry, err := normalizeRate(ctx, tx, r.ClientID, "tpm", clientRevision, tpm, now, false)
		if err != nil {
			return coordination.AdmissionDecision{}, err
		}
		if !ok {
			return rejectAcquire(ctx, tx, r.LeaseID, coordination.ReasonTPMExhausted, &retry, now)
		}
	}
	if rpm > 0 {
		ok, retry, err := normalizeRate(ctx, tx, r.ClientID, "rpm", clientRevision, rpm, now, true)
		if err != nil {
			return coordination.AdmissionDecision{}, err
		}
		if !ok {
			return rejectAcquire(ctx, tx, r.LeaseID, coordination.ReasonRPMExhausted, &retry, now)
		}
	}
	lease := coordination.LeaseIdentity{LeaseID: r.LeaseID, ClientID: r.ClientID, PoolID: r.PoolID, RequestID: r.RequestID, ReplicaID: r.ReplicaID, ExpiresAt: now.Add(r.LeaseTTL), TTL: r.LeaseTTL}
	writes := &pgx.Batch{}
	writes.Queue(`INSERT INTO request_leases(lease_id,request_id,replica_id,client_id,pool_id,acquired_at,expires_at) VALUES($1::uuid,$2::text,$3::uuid,$4::bigint,$5::bigint,$6::timestamptz,$7::timestamptz)`, lease.LeaseID, lease.RequestID, lease.ReplicaID, lease.ClientID, lease.PoolID, now, lease.ExpiresAt)
	writes.Queue("UPDATE admission_operations SET decision='admitted',decided_at=$2::timestamptz WHERE lease_id=$1::uuid", r.LeaseID, now)
	if err = tx.SendBatch(ctx, writes).Close(); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	return coordination.AdmissionDecision{Lease: &lease}, nil
}

func (c *AdmissionCoordinator) resolveAcquire(ctx context.Context, tx pgx.Tx, r coordination.AdmissionRequest, fp [32]byte) (coordination.AdmissionDecision, error) {
	var trustedClient, trustedPool int64
	var initialFingerprint []byte
	if err := tx.QueryRow(ctx, "SELECT client_id,pool_id,input_fingerprint FROM admission_operations WHERE lease_id=$1::uuid", r.LeaseID).Scan(&trustedClient, &trustedPool, &initialFingerprint); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if !bytes.Equal(initialFingerprint, fp[:]) {
		return commitDecision(ctx, tx, coordination.AdmissionDecision{Reason: coordination.ReasonIdempotencyConflict})
	}
	if _, err := tx.Exec(ctx, "SELECT pool_id FROM pool_admission_scopes WHERE pool_id=$1::bigint FOR UPDATE", trustedPool); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if _, err := tx.Exec(ctx, "SELECT client_id FROM client_admission_scopes WHERE client_id=$1::bigint FOR UPDATE", trustedClient); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	var lease coordination.LeaseIdentity
	leaseErr := tx.QueryRow(ctx, `SELECT lease_id,client_id,pool_id,request_id,replica_id,expires_at FROM request_leases WHERE lease_id=$1::uuid FOR UPDATE`, r.LeaseID).Scan(&lease.LeaseID, &lease.ClientID, &lease.PoolID, &lease.RequestID, &lease.ReplicaID, &lease.ExpiresAt)
	var stored []byte
	var decision string
	var reason *string
	var retry *time.Time
	var completed, expired *time.Time
	var scope coordination.AdmissionScope
	if err := tx.QueryRow(ctx, "SELECT input_fingerprint,decision,rejection_reason,retry_at,completed_at,lease_expired_at,concurrency_scope FROM admission_operations WHERE lease_id=$1::uuid FOR UPDATE", r.LeaseID).Scan(&stored, &decision, &reason, &retry, &completed, &expired, &scope); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if !bytes.Equal(stored, fp[:]) {
		return commitDecision(ctx, tx, coordination.AdmissionDecision{Reason: coordination.ReasonIdempotencyConflict})
	}
	if decision == "rejected" {
		if reason == nil {
			return coordination.AdmissionDecision{}, coordination.PermanentError{Err: errors.New("invalid rejected admission receipt")}
		}
		return commitDecision(ctx, tx, coordination.AdmissionDecision{Reason: coordination.Reason(*reason), RetryAt: retry, ConcurrencyScope: scope})
	}
	if completed != nil || expired != nil {
		return commitDecision(ctx, tx, coordination.AdmissionDecision{Reason: coordination.ReasonLeaseLost})
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if errors.Is(leaseErr, pgx.ErrNoRows) {
		return markLost(ctx, tx, r.LeaseID, now)
	}
	if leaseErr != nil {
		return coordination.AdmissionDecision{}, leaseErr
	}
	if !lease.ExpiresAt.After(now) {
		return markLost(ctx, tx, r.LeaseID, now)
	}
	lease.TTL = r.LeaseTTL
	return commitDecision(ctx, tx, coordination.AdmissionDecision{Lease: &lease})
}
func markLost(ctx context.Context, tx pgx.Tx, id uuid.UUID, now time.Time) (coordination.AdmissionDecision, error) {
	_, err := tx.Exec(ctx, "UPDATE admission_operations SET lease_expired_at=$2::timestamptz,retain_until=GREATEST(operation_started_at,$2::timestamptz)+interval '24 hours' WHERE lease_id=$1::uuid", id, now)
	if err != nil {
		return coordination.AdmissionDecision{}, err
	}
	return commitDecision(ctx, tx, coordination.AdmissionDecision{Reason: coordination.ReasonLeaseLost})
}
func rejectAcquire(ctx context.Context, tx pgx.Tx, id uuid.UUID, reason coordination.Reason, retry *time.Time, now time.Time, scopes ...coordination.AdmissionScope) (coordination.AdmissionDecision, error) {
	var scope coordination.AdmissionScope
	if reason == coordination.ReasonConcurrencyExhausted {
		scope = coordination.AdmissionClientScope
		if len(scopes) > 0 {
			scope = scopes[0]
		}
	}
	_, err := tx.Exec(ctx, "UPDATE admission_operations SET decision='rejected',rejection_reason=$2::text,retry_at=$3::timestamptz,decided_at=$4::timestamptz,retain_until=GREATEST(operation_started_at,$4::timestamptz)+interval '24 hours',concurrency_scope=$5::text WHERE lease_id=$1::uuid", id, string(reason), retry, now, string(scope))
	if err != nil {
		return coordination.AdmissionDecision{}, err
	}
	return commitDecision(ctx, tx, coordination.AdmissionDecision{Reason: reason, RetryAt: retry, ConcurrencyScope: scope})
}
func commitDecision(ctx context.Context, tx pgx.Tx, d coordination.AdmissionDecision) (coordination.AdmissionDecision, error) {
	if err := tx.Commit(ctx); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	return d, nil
}
func normalizeRate(ctx context.Context, tx pgx.Tx, client int64, kind string, revision, capacity int64, now time.Time, debit bool) (bool, time.Time, error) {
	var oldRev int64
	var balance float64
	var at time.Time
	if err := tx.QueryRow(ctx, "SELECT policy_revision,balance,last_refill_at FROM coordination_rate_state WHERE client_id=$1::bigint AND rate_kind=$2::text", client, kind).Scan(&oldRev, &balance, &at); err != nil {
		return false, time.Time{}, err
	}
	if oldRev != revision {
		balance = float64(capacity)
		at = now
	} else if now.After(at) {
		balance += (now.Sub(at).Seconds() * float64(capacity)) / 60
		if balance > float64(capacity) {
			balance = float64(capacity)
		}
	}
	if balance < 1 {
		retry := coordination.RateRetryAt(balance, capacity, now)
		_, err := tx.Exec(ctx, "UPDATE coordination_rate_state SET policy_revision=$3::bigint,balance=$4::double precision,last_refill_at=$5::timestamptz WHERE client_id=$1::bigint AND rate_kind=$2::text", client, kind, revision, balance, now)
		return false, retry, err
	}
	if debit {
		balance--
	}
	_, err := tx.Exec(ctx, "UPDATE coordination_rate_state SET policy_revision=$3::bigint,balance=$4::double precision,last_refill_at=$5::timestamptz WHERE client_id=$1::bigint AND rate_kind=$2::text", client, kind, revision, balance, now)
	return true, time.Time{}, err
}

func admissionFingerprint(v coordination.AdmissionRequest) [32]byte {
	b, _ := json.Marshal(v)
	return sha256.Sum256(b)
}

// Renewal transactions are bounded so a large stream population does not turn
// one coordination timeout into a rollback of every lease renewal.
const renewalBatchSize = 256

func (c *AdmissionCoordinator) Renew(parent context.Context, leases []coordination.LeaseIdentity) ([]coordination.RenewResult, error) {
	out := make([]coordination.RenewResult, 0, len(leases))
	for start := 0; start < len(leases); start += renewalBatchSize {
		end := min(start+renewalBatchSize, len(leases))
		ctx, cancel := c.deadline(parent)
		results, err := c.renew(ctx, leases[start:end])
		cancel()
		if err != nil {
			// Only committed transactions form the returned prefix. Callers can
			// apply these results even when a later transaction cannot finish.
			return out, recordCoordinationError(parent, err, c)
		}
		out = append(out, results...)
	}
	c.setStatus(true, "")
	_ = c.RefreshInflight(parent)
	return out, nil
}

func (c *AdmissionCoordinator) renew(ctx context.Context, leases []coordination.LeaseIdentity) ([]coordination.RenewResult, error) {
	out := make([]coordination.RenewResult, len(leases))
	if len(leases) == 0 {
		return out, nil
	}
	ids := make([]string, len(leases))
	for i, lease := range leases {
		ids[i] = lease.LeaseID.String()
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return nil, err
	}
	// Read immutable ownership from storage rather than trusting a supplied
	// identity to choose the scopes protecting expiry and concurrency counts.
	rows, err := tx.Query(ctx, "SELECT pool_id,client_id FROM request_leases WHERE lease_id=ANY($1::uuid[])", ids)
	if err != nil {
		return nil, err
	}
	var pools, clients []int64
	for rows.Next() {
		var pool, client int64
		if err := rows.Scan(&pool, &client); err != nil {
			rows.Close()
			return nil, err
		}
		pools = append(pools, pool)
		clients = append(clients, client)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	// Consume each ordered lock category before acquiring the next one. The
	// number of round trips is independent of the number of leases in a batch.
	if _, err = tx.Exec(ctx, "SELECT pool_id FROM pool_admission_scopes WHERE pool_id=ANY($1::bigint[]) ORDER BY pool_id FOR UPDATE", pools); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, "SELECT client_id FROM client_admission_scopes WHERE client_id=ANY($1::bigint[]) ORDER BY client_id FOR UPDATE", clients); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, "SELECT lease_id FROM request_leases WHERE lease_id=ANY($1::uuid[]) ORDER BY lease_id FOR UPDATE", ids); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, "SELECT lease_id FROM admission_operations WHERE lease_id=ANY($1::uuid[]) ORDER BY lease_id FOR UPDATE", ids); err != nil {
		return nil, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return nil, err
	}
	rows, err = tx.Query(ctx, "SELECT lease_id,client_id,pool_id,request_id,replica_id,expires_at FROM request_leases WHERE lease_id=ANY($1::uuid[])", ids)
	if err != nil {
		return nil, err
	}
	current, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (v coordination.LeaseIdentity, err error) {
		err = row.Scan(&v.LeaseID, &v.ClientID, &v.PoolID, &v.RequestID, &v.ReplicaID, &v.ExpiresAt)
		return
	})
	if err != nil {
		return nil, err
	}
	byID := make(map[uuid.UUID]coordination.LeaseIdentity, len(current))
	for _, lease := range current {
		byID[lease.LeaseID] = lease
	}
	updates := make(map[uuid.UUID]time.Time, len(leases))
	for i, lease := range leases {
		out[i] = coordination.RenewResult{Lease: lease, Reason: coordination.ReasonLeaseLost}
		stored, exists := byID[lease.LeaseID]
		if !exists || stored.ClientID != lease.ClientID || stored.PoolID != lease.PoolID || stored.RequestID != lease.RequestID || stored.ReplicaID != lease.ReplicaID || !stored.ExpiresAt.After(now) || lease.TTL <= 0 {
			continue
		}
		stored.TTL = lease.TTL
		stored.ExpiresAt = now.Add(lease.TTL)
		out[i] = coordination.RenewResult{Lease: stored}
		if stored.ExpiresAt.After(updates[lease.LeaseID]) {
			updates[lease.LeaseID] = stored.ExpiresAt
		}
	}
	if len(updates) > 0 {
		updateIDs := make([]string, 0, len(updates))
		expires := make([]time.Time, 0, len(updates))
		for id, at := range updates {
			updateIDs = append(updateIDs, id.String())
			expires = append(expires, at)
		}
		if _, err = tx.Exec(ctx, `UPDATE request_leases l SET expires_at=v.expires_at
			FROM unnest($1::uuid[],$2::timestamptz[]) AS v(lease_id,expires_at)
			WHERE l.lease_id=v.lease_id`, updateIDs, expires); err != nil {
			return nil, err
		}
	}
	// Only actual expiry changes the receipt; an identity mismatch must not
	// mark another replica's still-live admission as expired.
	if _, err = tx.Exec(ctx, `WITH expired AS (
		DELETE FROM request_leases WHERE lease_id=ANY($1::uuid[]) AND expires_at<=$2::timestamptz RETURNING lease_id
	) UPDATE admission_operations SET lease_expired_at=COALESCE(lease_expired_at,$2::timestamptz),
		retain_until=GREATEST(operation_started_at,$2::timestamptz)+interval '24 hours'
		WHERE lease_id IN (SELECT lease_id FROM expired) AND completed_at IS NULL`, ids, now); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *AdmissionCoordinator) Complete(parent context.Context, items []coordination.LeaseCompletion) ([]coordination.CompleteResult, error) {
	ctx, cancel := c.deadline(parent)
	defer cancel()
	out := make([]coordination.CompleteResult, len(items))
	for i, item := range items {
		r, err := c.completeOne(ctx, item)
		if err != nil {
			err = recordCoordinationError(parent, err, c)
			return out[:i], err
		}
		out[i] = r
	}
	c.setStatus(true, "")
	_ = c.RefreshInflight(parent)
	return out, nil
}
func (c *AdmissionCoordinator) completeOne(ctx context.Context, item coordination.LeaseCompletion) (coordination.CompleteResult, error) {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return coordination.CompleteResult{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return coordination.CompleteResult{}, err
	}
	var client, pool int64
	var decision string
	var completed *time.Time
	var stored *string
	var started time.Time
	if err = tx.QueryRow(ctx, "SELECT client_id,pool_id,decision,completed_at,completion_result,operation_started_at FROM admission_operations WHERE lease_id=$1::uuid", item.Lease.LeaseID).Scan(&client, &pool, &decision, &completed, &stored, &started); errors.Is(err, pgx.ErrNoRows) {
		return commitComplete(ctx, tx, coordination.CompleteResult{LeaseID: item.Lease.LeaseID, Reason: coordination.ReasonStaleOperation})
	}
	if err != nil {
		return coordination.CompleteResult{}, err
	}
	if decision != "admitted" {
		return commitComplete(ctx, tx, coordination.CompleteResult{LeaseID: item.Lease.LeaseID, Result: coordination.CompletionLeaseLost, Reason: coordination.ReasonLeaseLost})
	}
	if _, err = tx.Exec(ctx, "SELECT pool_id FROM pool_admission_scopes WHERE pool_id=$1::bigint FOR UPDATE", pool); err != nil {
		return coordination.CompleteResult{}, err
	}
	if _, err = tx.Exec(ctx, "SELECT client_id FROM client_admission_scopes WHERE client_id=$1::bigint FOR UPDATE", client); err != nil {
		return coordination.CompleteResult{}, err
	}
	var expires *time.Time
	leaseErr := tx.QueryRow(ctx, "SELECT expires_at FROM request_leases WHERE lease_id=$1::uuid FOR UPDATE", item.Lease.LeaseID).Scan(&expires)
	if leaseErr != nil && !errors.Is(leaseErr, pgx.ErrNoRows) {
		return coordination.CompleteResult{}, leaseErr
	}
	if err = tx.QueryRow(ctx, "SELECT decision,completed_at,completion_result,operation_started_at FROM admission_operations WHERE lease_id=$1::uuid FOR UPDATE", item.Lease.LeaseID).Scan(&decision, &completed, &stored, &started); err != nil {
		return coordination.CompleteResult{}, err
	}
	if completed != nil {
		if stored == nil {
			return coordination.CompleteResult{}, coordination.PermanentError{Err: errors.New("invalid completed admission receipt")}
		}
		orig := coordination.CompletionResult(*stored)
		return commitComplete(ctx, tx, coordination.CompleteResult{LeaseID: item.Lease.LeaseID, Result: coordination.CompletionAlreadyCompleted, OriginalResult: orig})
	}
	var tpmRevision, tpmLimit int64
	chargeTPM := item.Usage != nil && item.Usage.Valid()
	if chargeTPM {
		clientErr := tx.QueryRow(ctx, "SELECT revision,tokens_per_minute FROM clients WHERE id=$1::bigint", client).Scan(&tpmRevision, &tpmLimit)
		if clientErr != nil && !errors.Is(clientErr, pgx.ErrNoRows) {
			return coordination.CompleteResult{}, clientErr
		}
		if errors.Is(clientErr, pgx.ErrNoRows) {
			chargeTPM = false
		} else {
			chargeTPM = tpmLimit > 0
		}
	}
	if chargeTPM {
		if _, err = tx.Exec(ctx, `INSERT INTO coordination_rate_state(client_id,rate_kind,policy_revision,balance,last_refill_at) VALUES($1::bigint,'tpm',$2::bigint,$3::double precision,$4::timestamptz) ON CONFLICT DO NOTHING`, client, tpmRevision, float64(tpmLimit), time.Unix(0, 0).UTC()); err != nil {
			return coordination.CompleteResult{}, err
		}
		if _, err = tx.Exec(ctx, "SELECT client_id FROM coordination_rate_state WHERE client_id=$1::bigint AND rate_kind='tpm' FOR UPDATE", client); err != nil {
			return coordination.CompleteResult{}, err
		}
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return coordination.CompleteResult{}, err
	}
	result := coordination.CompletionLeaseLost
	if expires != nil && expires.After(now) {
		result = coordination.CompletionReleased
	}
	if chargeTPM {
		ok, _, normErr := normalizeRate(ctx, tx, client, "tpm", tpmRevision, tpmLimit, now, false)
		if normErr != nil {
			return coordination.CompleteResult{}, normErr
		}
		_ = ok
		_, err = tx.Exec(ctx, "UPDATE coordination_rate_state SET balance=balance-$2::double precision,last_refill_at=$3::timestamptz WHERE client_id=$1::bigint AND rate_kind='tpm'", client, (float64(item.Usage.InputTokens) + float64(item.Usage.OutputTokens)), now)
		if err != nil {
			return coordination.CompleteResult{}, err
		}
	}
	_, err = tx.Exec(ctx, "DELETE FROM request_leases WHERE lease_id=$1::uuid AND client_id=$2::bigint AND pool_id=$3::bigint", item.Lease.LeaseID, client, pool)
	if err != nil {
		return coordination.CompleteResult{}, err
	}
	_, err = tx.Exec(ctx, "UPDATE admission_operations SET completed_at=$2::timestamptz,completion_result=$3::text,retain_until=GREATEST(operation_started_at,$2::timestamptz)+interval '24 hours' WHERE lease_id=$1::uuid", item.Lease.LeaseID, now, string(result))
	if err != nil {
		return coordination.CompleteResult{}, err
	}
	return commitComplete(ctx, tx, coordination.CompleteResult{LeaseID: item.Lease.LeaseID, Result: result})
}
func commitComplete(ctx context.Context, tx pgx.Tx, r coordination.CompleteResult) (coordination.CompleteResult, error) {
	return r, tx.Commit(ctx)
}
func sortedUnique(values []int64) []int64 {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	out := values[:0]
	for _, v := range values {
		if len(out) == 0 || out[len(out)-1] != v {
			out = append(out, v)
		}
	}
	return out
}

var _ coordination.AdmissionCoordinator = (*AdmissionCoordinator)(nil)
