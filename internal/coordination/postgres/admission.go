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
}

func NewAdmissionCoordinator(store *pgstore.Store, timeout time.Duration) *AdmissionCoordinator {
	if timeout <= 0 {
		timeout = 50 * time.Millisecond
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
		if resultErr == nil || !permanentCoordinationError(resultErr) {
			return
		}
		c.setPermanent()
		var permanent coordination.PermanentError
		if !errors.As(resultErr, &permanent) {
			resultErr = coordination.PermanentError{Err: resultErr}
		}
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
}
func (c *AdmissionCoordinator) setPermanent() {
	c.mu.Lock()
	c.status.Available = false
	c.status.Degraded = false
	c.status.Permanent = true
	c.status.Reason = coordination.ReasonCoordinationUnavailable
	c.mu.Unlock()
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
		if permanentCoordinationError(err) {
			c.setPermanent()
			var permanent coordination.PermanentError
			if !errors.As(err, &permanent) {
				err = coordination.PermanentError{Err: err}
			}
		} else {
			c.setStatus(false, coordination.ReasonCoordinationUnavailable)
		}
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
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	fp := admissionFingerprint(r)
	tag, err := tx.Exec(ctx, `INSERT INTO admission_operations(lease_id,operation_started_at,input_fingerprint,request_id,replica_id,client_id,pool_id,decision) VALUES($1::uuid,$2::timestamptz,$3::bytea,$4::text,$5::uuid,$6::bigint,$7::bigint,'pending') ON CONFLICT DO NOTHING`, r.LeaseID, r.OperationStartedAt.UTC(), fp[:], r.RequestID, r.ReplicaID, r.ClientID, r.PoolID)
	if err != nil {
		return coordination.AdmissionDecision{}, err
	}
	created := tag.RowsAffected() == 1
	if !created {
		return c.resolveAcquire(ctx, tx, r, fp)
	}
	if _, err = tx.Exec(ctx, "SELECT pool_id FROM pool_admission_scopes WHERE pool_id=$1::bigint FOR UPDATE", r.PoolID); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if _, err = tx.Exec(ctx, "SELECT client_id FROM client_admission_scopes WHERE client_id=$1::bigint FOR UPDATE", r.ClientID); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	var revision, clientRevision int64
	var keyClient int64
	var clientEnabled, poolEnabled bool
	var configuredClient, poolLimit int
	var rpm, tpm int64
	var keyExpires *time.Time
	err = tx.QueryRow(ctx, `SELECT m.revision,c.revision,c.enabled,c.max_concurrency,c.requests_per_minute,c.tokens_per_minute,k.client_id,k.expires_at,p.enabled,p.max_gateway_inflight FROM config_meta m JOIN clients c ON c.id=$1::bigint JOIN api_keys k ON k.id=$2::bigint JOIN model_pools p ON p.id=$3::bigint WHERE m.singleton=1 AND k.revoked_at IS NULL FOR SHARE OF c,k,p`, r.ClientID, r.APIKeyID, r.PoolID).Scan(&revision, &clientRevision, &clientEnabled, &configuredClient, &rpm, &tpm, &keyClient, &keyExpires, &poolEnabled, &poolLimit)
	if errors.Is(err, pgx.ErrNoRows) {
		var now time.Time
		if scanErr := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); scanErr != nil {
			return coordination.AdmissionDecision{}, scanErr
		}
		return rejectAcquire(ctx, tx, r.LeaseID, coordination.ReasonStaleConfiguration, nil, now)
	}
	if err != nil {
		return coordination.AdmissionDecision{}, err
	}
	for _, kind := range []string{"rpm", "tpm"} {
		limit := rpm
		if kind == "tpm" {
			limit = tpm
		}
		if limit <= 0 {
			continue
		}
		if _, err = tx.Exec(ctx, `INSERT INTO coordination_rate_state(client_id,rate_kind,policy_revision,balance,last_refill_at) VALUES($1::bigint,$2::text,$3::bigint,$4::double precision,$5::timestamptz) ON CONFLICT DO NOTHING`, r.ClientID, kind, clientRevision, float64(limit), time.Unix(0, 0).UTC()); err != nil {
			return coordination.AdmissionDecision{}, err
		}
		if _, err = tx.Exec(ctx, "SELECT client_id FROM coordination_rate_state WHERE client_id=$1::bigint AND rate_kind=$2::text FOR UPDATE", r.ClientID, kind); err != nil {
			return coordination.AdmissionDecision{}, err
		}
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return coordination.AdmissionDecision{}, err
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
	var poolCount, clientCount int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM request_leases WHERE pool_id=$1::bigint AND expires_at>$2::timestamptz", r.PoolID, now).Scan(&poolCount); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM request_leases WHERE client_id=$1::bigint AND expires_at>$2::timestamptz", r.ClientID, now).Scan(&clientCount); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if poolLimit > 0 && poolCount >= poolLimit {
		return rejectAcquire(ctx, tx, r.LeaseID, coordination.ReasonConcurrencyExhausted, nil, now)
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
	if _, err = tx.Exec(ctx, `INSERT INTO request_leases(lease_id,request_id,replica_id,client_id,pool_id,acquired_at,expires_at) VALUES($1::uuid,$2::text,$3::uuid,$4::bigint,$5::bigint,$6::timestamptz,$7::timestamptz)`, lease.LeaseID, lease.RequestID, lease.ReplicaID, lease.ClientID, lease.PoolID, now, lease.ExpiresAt); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if _, err = tx.Exec(ctx, "UPDATE admission_operations SET decision='admitted',decided_at=$2::timestamptz WHERE lease_id=$1::uuid", r.LeaseID, now); err != nil {
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
	if err := tx.QueryRow(ctx, "SELECT input_fingerprint,decision,rejection_reason,retry_at,completed_at,lease_expired_at FROM admission_operations WHERE lease_id=$1::uuid FOR UPDATE", r.LeaseID).Scan(&stored, &decision, &reason, &retry, &completed, &expired); err != nil {
		return coordination.AdmissionDecision{}, err
	}
	if !bytes.Equal(stored, fp[:]) {
		return commitDecision(ctx, tx, coordination.AdmissionDecision{Reason: coordination.ReasonIdempotencyConflict})
	}
	if decision == "rejected" {
		if reason == nil {
			return coordination.AdmissionDecision{}, coordination.PermanentError{Err: errors.New("invalid rejected admission receipt")}
		}
		return commitDecision(ctx, tx, coordination.AdmissionDecision{Reason: coordination.Reason(*reason), RetryAt: retry})
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
func rejectAcquire(ctx context.Context, tx pgx.Tx, id uuid.UUID, reason coordination.Reason, retry *time.Time, now time.Time) (coordination.AdmissionDecision, error) {
	_, err := tx.Exec(ctx, "UPDATE admission_operations SET decision='rejected',rejection_reason=$2::text,retry_at=$3::timestamptz,decided_at=$4::timestamptz,retain_until=GREATEST(operation_started_at,$4::timestamptz)+interval '24 hours' WHERE lease_id=$1::uuid", id, string(reason), retry, now)
	if err != nil {
		return coordination.AdmissionDecision{}, err
	}
	return commitDecision(ctx, tx, coordination.AdmissionDecision{Reason: reason, RetryAt: retry})
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
		retry := now.Add(time.Duration((1 - balance) / float64(capacity) * float64(time.Minute)))
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

func (c *AdmissionCoordinator) Renew(parent context.Context, leases []coordination.LeaseIdentity) ([]coordination.RenewResult, error) {
	ctx, cancel := c.deadline(parent)
	defer cancel()
	out, err := c.renew(ctx, leases)
	if err != nil {
		if permanentCoordinationError(err) {
			c.setPermanent()
			var permanent coordination.PermanentError
			if !errors.As(err, &permanent) {
				err = coordination.PermanentError{Err: err}
			}
		} else {
			c.setStatus(false, coordination.ReasonCoordinationUnavailable)
		}
	} else {
		c.setStatus(true, "")
		_ = c.RefreshInflight(parent)
	}
	return out, err
}
func (c *AdmissionCoordinator) renew(ctx context.Context, leases []coordination.LeaseIdentity) ([]coordination.RenewResult, error) {
	out := make([]coordination.RenewResult, len(leases))
	if len(leases) == 0 {
		return out, nil
	}
	ordered := append([]coordination.LeaseIdentity(nil), leases...)
	sort.Slice(ordered, func(i, j int) bool { return bytes.Compare(ordered[i].LeaseID[:], ordered[j].LeaseID[:]) < 0 })
	pools := make([]int64, 0, len(ordered))
	clients := make([]int64, 0, len(ordered))
	for _, lease := range ordered {
		pools = append(pools, lease.PoolID)
		clients = append(clients, lease.ClientID)
	}
	pools = sortedUnique(pools)
	clients = sortedUnique(clients)
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return nil, err
	}
	for _, id := range pools {
		if _, err = tx.Exec(ctx, "SELECT pool_id FROM pool_admission_scopes WHERE pool_id=$1::bigint FOR UPDATE", id); err != nil {
			return nil, err
		}
	}
	for _, id := range clients {
		if _, err = tx.Exec(ctx, "SELECT client_id FROM client_admission_scopes WHERE client_id=$1::bigint FOR UPDATE", id); err != nil {
			return nil, err
		}
	}
	for _, lease := range ordered {
		if _, err = tx.Exec(ctx, "SELECT lease_id FROM request_leases WHERE lease_id=$1::uuid FOR UPDATE", lease.LeaseID); err != nil {
			return nil, err
		}
	}
	for _, lease := range ordered {
		if _, err = tx.Exec(ctx, "SELECT lease_id FROM admission_operations WHERE lease_id=$1::uuid FOR UPDATE", lease.LeaseID); err != nil {
			return nil, err
		}
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return nil, err
	}
	byID := make(map[uuid.UUID]coordination.RenewResult, len(ordered))
	for _, lease := range ordered {
		result := coordination.RenewResult{Lease: lease}
		var current coordination.LeaseIdentity
		scanErr := tx.QueryRow(ctx, "SELECT lease_id,client_id,pool_id,request_id,replica_id,expires_at FROM request_leases WHERE lease_id=$1::uuid", lease.LeaseID).Scan(&current.LeaseID, &current.ClientID, &current.PoolID, &current.RequestID, &current.ReplicaID, &current.ExpiresAt)
		if scanErr != nil || current.ClientID != lease.ClientID || current.PoolID != lease.PoolID || current.RequestID != lease.RequestID || current.ReplicaID != lease.ReplicaID || !current.ExpiresAt.After(now) || lease.TTL <= 0 {
			result.Reason = coordination.ReasonLeaseLost
			if _, err = tx.Exec(ctx, "DELETE FROM request_leases WHERE lease_id=$1::uuid AND expires_at<=$2::timestamptz", lease.LeaseID, now); err != nil {
				return nil, err
			}
			if _, err = tx.Exec(ctx, "UPDATE admission_operations SET lease_expired_at=COALESCE(lease_expired_at,$2::timestamptz),retain_until=GREATEST(operation_started_at,$2::timestamptz)+interval '24 hours' WHERE lease_id=$1::uuid AND completed_at IS NULL", lease.LeaseID, now); err != nil {
				return nil, err
			}
			byID[lease.LeaseID] = result
			continue
		}
		current.TTL = lease.TTL
		current.ExpiresAt = now.Add(lease.TTL)
		if _, err = tx.Exec(ctx, "UPDATE request_leases SET expires_at=$2::timestamptz WHERE lease_id=$1::uuid", lease.LeaseID, current.ExpiresAt); err != nil {
			return nil, err
		}
		result.Lease = current
		byID[lease.LeaseID] = result
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	for i, lease := range leases {
		out[i] = byID[lease.LeaseID]
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
			if permanentCoordinationError(err) {
				c.setPermanent()
				var permanent coordination.PermanentError
				if !errors.As(err, &permanent) {
					err = coordination.PermanentError{Err: err}
				}
			} else {
				c.setStatus(false, coordination.ReasonCoordinationUnavailable)
			}
			return nil, err
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
		_, err = tx.Exec(ctx, "UPDATE coordination_rate_state SET balance=balance-$2::double precision,last_refill_at=$3::timestamptz WHERE client_id=$1::bigint AND rate_kind='tpm'", client, float64(item.Usage.InputTokens+item.Usage.OutputTokens), now)
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
