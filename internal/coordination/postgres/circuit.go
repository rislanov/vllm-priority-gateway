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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

const circuitCompletionRetention = 24 * time.Hour

type CircuitCoordinator struct {
	pool    *pgxpool.Pool
	timeout time.Duration
	options circuitbreaker.Options

	mu       sync.RWMutex
	cache    map[int64]coordination.CircuitSnapshot
	status   coordination.Status
	failures failurePublisher
}

func NewCircuitCoordinator(store *pgstore.Store, timeout time.Duration, options circuitbreaker.Options) (*CircuitCoordinator, error) {
	if _, err := circuitbreaker.New(options); err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = 50 * time.Millisecond
	}
	return &CircuitCoordinator{
		pool: store.CoordinationPool(), timeout: timeout, options: options,
		cache:  make(map[int64]coordination.CircuitSnapshot),
		status: coordination.Status{Backend: "postgres", Available: true},
	}, nil
}

func (c *CircuitCoordinator) Status() coordination.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

func (c *CircuitCoordinator) SetFailureObserver(observer coordination.CoordinationFailureObserver) {
	c.failures.set(observer)
}

func (c *CircuitCoordinator) setStatus(ok bool, reason coordination.Reason) {
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
func (c *CircuitCoordinator) setPermanent() {
	c.mu.Lock()
	c.status.Available = false
	c.status.Degraded = false
	c.status.Permanent = true
	c.status.Reason = coordination.ReasonCoordinationUnavailable
	c.mu.Unlock()
	c.failures.permanent()
}

func (c *CircuitCoordinator) Snapshot(backendID int64, _ time.Time) coordination.CircuitSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if snapshot, ok := c.cache[backendID]; ok {
		return snapshot
	}
	return coordination.CircuitSnapshot{Backend: coordination.BackendIdentity{ID: backendID}, State: domain.CircuitOpen}
}

func (c *CircuitCoordinator) Reconcile(parent context.Context, values []coordination.BackendIdentity) (resultErr error) {
	defer func() {
		resultErr = recordCoordinationError(parent, resultErr, c)
	}()
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return err
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
	for _, value := range values {
		var revision int64
		err = tx.QueryRow(ctx, "SELECT backend_revision FROM backend_circuit_state WHERE backend_id=$1::bigint FOR UPDATE", value.ID).Scan(&revision)
		stateExists := err == nil
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		authoritative, err := lockBackendIdentity(ctx, tx, value.ID)
		if err != nil {
			return err
		}
		if !stateExists {
			_, err = tx.Exec(ctx, "INSERT INTO backend_circuit_state(backend_id,backend_revision,state,generation,half_open_succeeded,updated_at) VALUES($1::bigint,$2::bigint,'closed',0,false,clock_timestamp())", value.ID, authoritative.Revision)
			if err != nil {
				return err
			}
		}
		if stateExists && revision != authoritative.Revision {
			if err := terminalizeProbes(ctx, tx, value.ID, "superseded"); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, "DELETE FROM backend_circuit_active_failures WHERE backend_id=$1::bigint", value.ID); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "UPDATE backend_circuit_state SET backend_revision=$2::bigint,state='closed',generation=generation+1,opened_at=NULL,half_open_succeeded=false,updated_at=clock_timestamp() WHERE backend_id=$1::bigint", value.ID, authoritative.Revision)
			if err != nil {
				return err
			}
		}
		if !authoritative.Enabled || authoritative.Draining {
			if err := terminalizeProbes(ctx, tx, value.ID, "superseded"); err != nil {
				return err
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if err = c.Refresh(parent); err != nil {
		return err
	}
	return nil
}

func terminalizeProbes(ctx context.Context, tx pgx.Tx, backendID int64, outcome string) error {
	if _, err := tx.Exec(ctx, "SELECT permit_id FROM backend_circuit_probes WHERE backend_id=$1::bigint AND outcome IS NULL ORDER BY permit_id FOR UPDATE", backendID); err != nil {
		return err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "UPDATE backend_circuit_probes SET outcome=$2::text,processed_at=$3::timestamptz,retain_until=GREATEST($3::timestamptz,acquisition_started_at)+interval '24 hours' WHERE backend_id=$1::bigint AND outcome IS NULL", backendID, outcome, now)
	return err
}

func (c *CircuitCoordinator) Refresh(parent context.Context) (resultErr error) {
	defer func() {
		resultErr = recordCoordinationError(parent, resultErr, c)
	}()
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	ids, err := c.reconcilableBackendIDs(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := c.reconcileBackendState(ctx, id); err != nil {
			return err
		}
	}
	rows, err := c.pool.Query(ctx, `SELECT s.backend_id,s.backend_revision,s.state,s.generation,s.opened_at,s.half_open_succeeded,
		(SELECT count(*) FROM backend_circuit_active_failures f WHERE f.backend_id=s.backend_id AND f.backend_revision=s.backend_revision AND (s.state<>'closed' OR f.event_at>=clock_timestamp()-$1::interval)),
		(SELECT count(*) FROM backend_circuit_probes p WHERE p.backend_id=s.backend_id AND p.backend_revision=s.backend_revision AND p.circuit_generation=s.generation AND p.outcome IS NULL AND p.expires_at>clock_timestamp())
		FROM backend_circuit_state s`, c.options.FailureWindow)
	if err != nil {
		return err
	}
	defer rows.Close()
	next := make(map[int64]coordination.CircuitSnapshot)
	for rows.Next() {
		var id coordination.BackendIdentity
		var state string
		var generation int64
		var opened *time.Time
		var succeeded bool
		var failures, probes int
		if err := rows.Scan(&id.ID, &id.Revision, &state, &generation, &opened, &succeeded, &failures, &probes); err != nil {
			return err
		}
		snapshot := coordination.CircuitSnapshot{Backend: id, State: domain.CircuitState(state), Generation: generation, FailureCount: failures, ProbesInFlight: probes}
		switch snapshot.State {
		case domain.CircuitClosed:
			snapshot.Available = true
		case domain.CircuitOpen:
			if opened != nil {
				snapshot.RetryAt = opened.Add(c.options.OpenCooldown)
			}
		case domain.CircuitHalfOpen:
			snapshot.Available = !succeeded && probes < c.options.HalfOpenMaxProbes
		}
		next[id.ID] = snapshot
	}
	if err := rows.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.cache = next
	c.status.Available = true
	c.status.Degraded = false
	c.status.Reason = ""
	c.status.LastSuccess = time.Now().UTC()
	c.mu.Unlock()
	return nil
}

func (c *CircuitCoordinator) reconcilableBackendIDs(ctx context.Context) ([]int64, error) {
	rows, err := c.pool.Query(ctx, "SELECT backend_id FROM backend_circuit_state WHERE state='half_open' OR (state='open' AND opened_at IS NOT NULL AND opened_at+$1::interval<=clock_timestamp()) ORDER BY backend_id", c.options.OpenCooldown)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (id int64, err error) { err = row.Scan(&id); return })
}

func (c *CircuitCoordinator) reconcileBackendState(ctx context.Context, backendID int64) error {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return err
	}
	state, err := lockCircuitState(ctx, tx, backendID)
	if err != nil {
		return err
	}
	if state.State == domain.CircuitHalfOpen {
		if _, err = tx.Exec(ctx, "SELECT permit_id FROM backend_circuit_probes WHERE backend_id=$1::bigint AND outcome IS NULL ORDER BY permit_id FOR UPDATE", backendID); err != nil {
			return err
		}
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return err
	}
	state, expired, err := c.reconcileExpired(ctx, tx, state, now)
	if err != nil {
		return err
	}
	if !expired && state.State == domain.CircuitOpen && !state.RetryAt.IsZero() && !now.Before(state.RetryAt) {
		state.State = domain.CircuitHalfOpen
		state.Generation++
		state.RetryAt = time.Time{}
		state.ProbesInFlight = 0
		state.Available = true
		if _, err = tx.Exec(ctx, "UPDATE backend_circuit_state SET state='half_open',generation=$2::bigint,opened_at=NULL,half_open_succeeded=false,updated_at=$3::timestamptz WHERE backend_id=$1::bigint", backendID, state.Generation, now); err != nil {
			return err
		}
		_, _ = tx.Exec(ctx, "SELECT pg_notify('llmgw_circuit_changed',$1::text)", backendID)
	}
	return tx.Commit(ctx)
}

func (c *CircuitCoordinator) Acquire(parent context.Context, request coordination.CircuitAcquireRequest) (coordination.CircuitDecision, error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	var decision coordination.CircuitDecision
	var handled bool
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		decision, handled, err = c.acquireClosed(ctx, request)
		if err == nil && !handled {
			decision, err = c.acquire(ctx, request)
		}
		if err == nil || !retryableCoordinationError(err) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		var reason coordination.ReasonError
		if errors.As(err, &reason) {
			if !handled {
				c.setStatus(true, "")
			}
			return decision, err
		}
		err = recordCoordinationError(parent, err, c)
		return coordination.CircuitDecision{Reason: coordination.ReasonCoordinationUnavailable}, err
	}
	if handled {
		return decision, nil
	}
	c.setStatus(true, "")
	_ = c.Refresh(parent)
	return decision, nil
}

func (c *CircuitCoordinator) acquire(ctx context.Context, request coordination.CircuitAcquireRequest) (coordination.CircuitDecision, error) {
	fingerprint := circuitAcquireFingerprint(request)
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return coordination.CircuitDecision{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return coordination.CircuitDecision{}, err
	}
	state, err := lockCircuitState(ctx, tx, request.Backend.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return commitCircuitDecision(ctx, tx, coordination.CircuitDecision{Reason: coordination.ReasonStaleBackend})
	}
	if err != nil {
		return coordination.CircuitDecision{}, err
	}
	authoritative, err := lockBackendIdentity(ctx, tx, request.Backend.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return commitCircuitDecision(ctx, tx, coordination.CircuitDecision{Reason: coordination.ReasonStaleBackend})
	}
	if err != nil {
		return coordination.CircuitDecision{}, err
	}
	state.Backend.Enabled = authoritative.Enabled
	state.Backend.Draining = authoritative.Draining
	if _, err = tx.Exec(ctx, "SELECT permit_id FROM backend_circuit_probes WHERE backend_id=$1::bigint AND outcome IS NULL ORDER BY permit_id FOR UPDATE", request.Backend.ID); err != nil {
		return coordination.CircuitDecision{}, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return coordination.CircuitDecision{}, err
	}
	state, changed, err := c.reconcileExpired(ctx, tx, state, now)
	if err != nil {
		return coordination.CircuitDecision{}, err
	}
	if state.State == domain.CircuitClosed {
		state.FailureCount, err = currentClosedFailureCount(ctx, tx, state, now, c.options.FailureWindow)
		if err != nil {
			return coordination.CircuitDecision{}, err
		}
	}
	var stored []byte
	var backendRevision, generation int64
	var expires time.Time
	var outcome *string
	err = tx.QueryRow(ctx, "SELECT input_fingerprint,backend_revision,circuit_generation,expires_at,outcome FROM backend_circuit_probes WHERE permit_id=$1::uuid FOR UPDATE", request.AttemptID).Scan(&stored, &backendRevision, &generation, &expires, &outcome)
	if err == nil {
		if !bytes.Equal(stored, fingerprint[:]) {
			return commitCircuitDecision(ctx, tx, coordination.CircuitDecision{Snapshot: state, Reason: coordination.ReasonIdempotencyConflict})
		}
		if outcome != nil || backendRevision != request.Backend.Revision || generation != state.Generation || !expires.After(now) {
			return commitCircuitDecision(ctx, tx, coordination.CircuitDecision{Snapshot: state, Reason: coordination.ReasonLeaseLost})
		}
		permit := coordination.ProbeIdentity{PermitID: request.AttemptID, Backend: request.Backend, Generation: generation, ReplicaID: request.ReplicaID, ExpiresAt: expires, TTL: request.ProbeTTL}
		return commitCircuitDecision(ctx, tx, coordination.CircuitDecision{Snapshot: state, Permit: &permit})
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return coordination.CircuitDecision{}, err
	}
	if state.Backend.Revision != authoritative.Revision || authoritative.Revision != request.Backend.Revision || !authoritative.Enabled || authoritative.Draining {
		return commitCircuitDecision(ctx, tx, coordination.CircuitDecision{Snapshot: state, Reason: coordination.ReasonStaleBackend})
	}
	if request.AcquisitionStartedAt.After(now.Add(5*time.Minute)) || !request.AcquisitionStartedAt.After(now.Add(-circuitCompletionRetention)) {
		return commitCircuitDecision(ctx, tx, coordination.CircuitDecision{Snapshot: state, Reason: coordination.ReasonStaleOperation})
	}
	if state.State == domain.CircuitClosed {
		state.Available = true
		return commitCircuitDecision(ctx, tx, coordination.CircuitDecision{Snapshot: state})
	}
	if state.State == domain.CircuitOpen {
		if now.Before(state.RetryAt) {
			return commitCircuitDecision(ctx, tx, coordination.CircuitDecision{Snapshot: state, Reason: coordination.ReasonCircuitOpen})
		}
		state.State = domain.CircuitHalfOpen
		state.Generation++
		state.RetryAt = time.Time{}
		state.ProbesInFlight = 0
		state.Available = true
		_, err = tx.Exec(ctx, "UPDATE backend_circuit_state SET state='half_open',generation=$2::bigint,opened_at=NULL,half_open_succeeded=false,updated_at=$3::timestamptz WHERE backend_id=$1::bigint", request.Backend.ID, state.Generation, now)
		if err != nil {
			return coordination.CircuitDecision{}, err
		}
		changed = true
	}
	var succeeded bool
	if err = tx.QueryRow(ctx, "SELECT half_open_succeeded FROM backend_circuit_state WHERE backend_id=$1::bigint", request.Backend.ID).Scan(&succeeded); err != nil {
		return coordination.CircuitDecision{}, err
	}
	if succeeded || state.ProbesInFlight >= c.options.HalfOpenMaxProbes {
		state.Available = false
		return commitCircuitDecision(ctx, tx, coordination.CircuitDecision{Snapshot: state, Reason: coordination.ReasonProbeCapacityExhausted})
	}
	permit := coordination.ProbeIdentity{PermitID: request.AttemptID, Backend: request.Backend, Generation: state.Generation, ReplicaID: request.ReplicaID, ExpiresAt: now.Add(request.ProbeTTL), TTL: request.ProbeTTL}
	_, err = tx.Exec(ctx, `INSERT INTO backend_circuit_probes(permit_id,acquisition_started_at,input_fingerprint,backend_id,backend_revision,circuit_generation,replica_id,expires_at)
		VALUES($1::uuid,$2::timestamptz,$3::bytea,$4::bigint,$5::bigint,$6::bigint,$7::uuid,$8::timestamptz)`, permit.PermitID, request.AcquisitionStartedAt.UTC(), fingerprint[:], request.Backend.ID, request.Backend.Revision, permit.Generation, request.ReplicaID, permit.ExpiresAt)
	if err != nil {
		return coordination.CircuitDecision{}, err
	}
	state.ProbesInFlight++
	state.Available = state.ProbesInFlight < c.options.HalfOpenMaxProbes
	if changed {
		_, _ = tx.Exec(ctx, "SELECT pg_notify('llmgw_circuit_changed',$1::text)", request.Backend.ID)
	}
	return commitCircuitDecision(ctx, tx, coordination.CircuitDecision{Snapshot: state, Permit: &permit})
}

func (c *CircuitCoordinator) acquireClosed(ctx context.Context, request coordination.CircuitAcquireRequest) (coordination.CircuitDecision, bool, error) {
	state, authoritative, now, retainedPermit, err := c.readCommittedCircuit(ctx, request.Backend.ID, request.AttemptID)
	if errors.Is(err, pgx.ErrNoRows) {
		return coordination.CircuitDecision{Reason: coordination.ReasonStaleBackend}, true, nil
	}
	if err != nil {
		return coordination.CircuitDecision{}, false, err
	}
	if retainedPermit {
		return coordination.CircuitDecision{}, false, nil
	}
	if state.Backend.Revision != authoritative.Revision || authoritative.Revision != request.Backend.Revision || !authoritative.Enabled || authoritative.Draining {
		return coordination.CircuitDecision{Snapshot: state, Reason: coordination.ReasonStaleBackend}, true, nil
	}
	if !validCircuitOperationTime(request.AcquisitionStartedAt, now) {
		return coordination.CircuitDecision{Snapshot: state, Reason: coordination.ReasonStaleOperation}, true, nil
	}
	if state.State != domain.CircuitClosed {
		return coordination.CircuitDecision{}, false, nil
	}
	state.Available = true
	return coordination.CircuitDecision{Snapshot: state}, true, nil
}

func (c *CircuitCoordinator) readCommittedCircuit(ctx context.Context, backendID int64, attemptID uuid.UUID) (coordination.CircuitSnapshot, coordination.BackendIdentity, time.Time, bool, error) {
	var snapshot coordination.CircuitSnapshot
	authoritative := coordination.BackendIdentity{ID: backendID}
	var state string
	var opened *time.Time
	var now time.Time
	var retainedPermit bool
	err := c.pool.QueryRow(ctx, `WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS now)
		SELECT s.backend_id,s.backend_revision,s.state,s.generation,s.opened_at,
		(SELECT count(*) FROM backend_circuit_active_failures f, observed o
		 WHERE f.backend_id=s.backend_id AND f.backend_revision=s.backend_revision AND f.event_at>=o.now-$3::interval),
		b.revision,b.enabled,b.draining,
		EXISTS(SELECT 1 FROM backend_circuit_probes p WHERE p.permit_id=$2::uuid),o.now
		FROM backend_circuit_state s
		JOIN backends b ON b.id=s.backend_id
		CROSS JOIN observed o
		WHERE s.backend_id=$1::bigint`, backendID, attemptID, c.options.FailureWindow).Scan(
		&snapshot.Backend.ID, &snapshot.Backend.Revision, &state, &snapshot.Generation, &opened, &snapshot.FailureCount,
		&authoritative.Revision, &authoritative.Enabled, &authoritative.Draining, &retainedPermit, &now,
	)
	if err != nil {
		return snapshot, authoritative, time.Time{}, false, err
	}
	snapshot.Backend.Enabled = authoritative.Enabled
	snapshot.Backend.Draining = authoritative.Draining
	snapshot.State = domain.CircuitState(state)
	if opened != nil {
		snapshot.RetryAt = opened.Add(c.options.OpenCooldown)
	}
	return snapshot, authoritative, now, retainedPermit, nil
}

func lockCircuitState(ctx context.Context, tx pgx.Tx, backendID int64) (coordination.CircuitSnapshot, error) {
	var snapshot coordination.CircuitSnapshot
	var state string
	var opened *time.Time
	var succeeded bool
	err := tx.QueryRow(ctx, `SELECT s.backend_id,s.backend_revision,s.state,s.generation,s.opened_at,
		(SELECT count(*) FROM backend_circuit_active_failures f WHERE f.backend_id=s.backend_id AND f.backend_revision=s.backend_revision),s.half_open_succeeded
		FROM backend_circuit_state s WHERE s.backend_id=$1::bigint FOR UPDATE`, backendID).Scan(&snapshot.Backend.ID, &snapshot.Backend.Revision, &state, &snapshot.Generation, &opened, &snapshot.FailureCount, &succeeded)
	if err != nil {
		return snapshot, err
	}
	snapshot.State = domain.CircuitState(state)
	if opened != nil {
		snapshot.RetryAt = opened.Add(0)
	}
	return snapshot, nil
}

func lockBackendIdentity(ctx context.Context, tx pgx.Tx, backendID int64) (coordination.BackendIdentity, error) {
	identity := coordination.BackendIdentity{ID: backendID}
	err := tx.QueryRow(ctx, "SELECT revision,enabled,draining FROM backends WHERE id=$1::bigint FOR UPDATE", backendID).Scan(&identity.Revision, &identity.Enabled, &identity.Draining)
	return identity, err
}

func currentClosedFailureCount(ctx context.Context, tx pgx.Tx, state coordination.CircuitSnapshot, now time.Time, window time.Duration) (int, error) {
	var count int
	err := tx.QueryRow(ctx, "SELECT count(*) FROM backend_circuit_active_failures WHERE backend_id=$1::bigint AND backend_revision=$2::bigint AND event_at>=$3::timestamptz", state.Backend.ID, state.Backend.Revision, now.Add(-window)).Scan(&count)
	return count, err
}

func recordActiveFailure(ctx context.Context, tx pgx.Tx, state coordination.CircuitSnapshot, attemptID uuid.UUID, eventAt time.Time, window time.Duration) (int, time.Time, error) {
	if _, err := tx.Exec(ctx, `INSERT INTO backend_circuit_active_failures(attempt_id,backend_id,backend_revision,event_at)
		VALUES($1::uuid,$2::bigint,$3::bigint,$4::timestamptz) ON CONFLICT (attempt_id) DO NOTHING`, attemptID, state.Backend.ID, state.Backend.Revision, eventAt); err != nil {
		return 0, time.Time{}, err
	}
	var latest time.Time
	if err := tx.QueryRow(ctx, "SELECT max(event_at) FROM backend_circuit_active_failures WHERE backend_id=$1::bigint AND backend_revision=$2::bigint", state.Backend.ID, state.Backend.Revision).Scan(&latest); err != nil {
		return 0, time.Time{}, err
	}
	if _, err := tx.Exec(ctx, "DELETE FROM backend_circuit_active_failures WHERE backend_id=$1::bigint AND backend_revision=$2::bigint AND event_at<$3::timestamptz", state.Backend.ID, state.Backend.Revision, latest.Add(-window)); err != nil {
		return 0, time.Time{}, err
	}
	var count int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM backend_circuit_active_failures WHERE backend_id=$1::bigint AND backend_revision=$2::bigint", state.Backend.ID, state.Backend.Revision).Scan(&count); err != nil {
		return 0, time.Time{}, err
	}
	return count, latest, nil
}

func (c *CircuitCoordinator) reconcileExpired(ctx context.Context, tx pgx.Tx, state coordination.CircuitSnapshot, now time.Time) (coordination.CircuitSnapshot, bool, error) {
	if state.State != domain.CircuitHalfOpen {
		if state.State == domain.CircuitOpen && !state.RetryAt.IsZero() {
			state.RetryAt = state.RetryAt.Add(c.options.OpenCooldown)
		}
		return state, false, nil
	}
	var permitID uuid.UUID
	var expiredAt time.Time
	err := tx.QueryRow(ctx, "SELECT permit_id,expires_at FROM backend_circuit_probes WHERE backend_id=$1::bigint AND backend_revision=$2::bigint AND circuit_generation=$3::bigint AND outcome IS NULL AND expires_at<=$4::timestamptz ORDER BY expires_at,permit_id LIMIT 1 FOR UPDATE", state.Backend.ID, state.Backend.Revision, state.Generation, now).Scan(&permitID, &expiredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM backend_circuit_probes WHERE backend_id=$1::bigint AND backend_revision=$2::bigint AND circuit_generation=$3::bigint AND outcome IS NULL AND expires_at>$4::timestamptz", state.Backend.ID, state.Backend.Revision, state.Generation, now).Scan(&state.ProbesInFlight); err != nil {
			return state, false, err
		}
		return state, false, nil
	}
	if err != nil {
		return state, false, err
	}
	_, err = tx.Exec(ctx, "UPDATE backend_circuit_probes SET outcome=CASE WHEN permit_id=$4::uuid THEN 'expired' ELSE 'superseded' END,processed_at=$5::timestamptz,retain_until=GREATEST($5::timestamptz,acquisition_started_at)+interval '24 hours' WHERE backend_id=$1::bigint AND backend_revision=$2::bigint AND circuit_generation=$3::bigint AND outcome IS NULL", state.Backend.ID, state.Backend.Revision, state.Generation, permitID, now)
	if err != nil {
		return state, false, err
	}
	state.State = domain.CircuitOpen
	state.Generation++
	state.FailureCount, expiredAt, err = recordActiveFailure(ctx, tx, state, permitID, expiredAt, c.options.FailureWindow)
	if err != nil {
		return state, false, err
	}
	state.RetryAt = expiredAt.Add(c.options.OpenCooldown)
	state.ProbesInFlight = 0
	state.Available = false
	_, err = tx.Exec(ctx, "UPDATE backend_circuit_state SET state='open',generation=$2::bigint,opened_at=$3::timestamptz,half_open_succeeded=false,updated_at=$4::timestamptz WHERE backend_id=$1::bigint", state.Backend.ID, state.Generation, expiredAt, now)
	if err == nil {
		_, _ = tx.Exec(ctx, "SELECT pg_notify('llmgw_circuit_changed',$1::text)", state.Backend.ID)
	}
	return state, true, err
}

func commitCircuitDecision(ctx context.Context, tx pgx.Tx, decision coordination.CircuitDecision) (coordination.CircuitDecision, error) {
	return decision, tx.Commit(ctx)
}

func (c *CircuitCoordinator) Complete(parent context.Context, completion coordination.CircuitCompletion) (coordination.CircuitSnapshot, error) {
	completion.ReportedOutcomeAt = canonicalPostgresTimestamp(completion.ReportedOutcomeAt)
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	var snapshot coordination.CircuitSnapshot
	var handled bool
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		snapshot, handled, err = c.completeClosedNonFailure(ctx, completion)
		if err == nil && !handled {
			snapshot, err = c.complete(ctx, completion)
		}
		if err == nil || !retryableCoordinationError(err) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		var reason coordination.ReasonError
		if errors.As(err, &reason) {
			if !handled {
				c.setStatus(true, "")
			}
			return snapshot, err
		}
		err = recordCoordinationError(parent, err, c)
		return snapshot, err
	}
	if handled {
		return snapshot, nil
	}
	c.setStatus(true, "")
	_ = c.Refresh(parent)
	return snapshot, nil
}

func canonicalPostgresTimestamp(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func (c *CircuitCoordinator) complete(ctx context.Context, completion coordination.CircuitCompletion) (coordination.CircuitSnapshot, error) {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return coordination.CircuitSnapshot{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return coordination.CircuitSnapshot{}, err
	}
	state, err := lockCircuitState(ctx, tx, completion.Backend.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return coordination.CircuitSnapshot{}, coordination.ReasonError{Reason: coordination.ReasonStaleBackend}
	}
	if err != nil {
		return coordination.CircuitSnapshot{}, err
	}
	if _, err = tx.Exec(ctx, "SELECT permit_id FROM backend_circuit_probes WHERE backend_id=$1::bigint AND outcome IS NULL ORDER BY permit_id FOR UPDATE", completion.Backend.ID); err != nil {
		return state, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return state, err
	}
	state, _, err = c.reconcileExpired(ctx, tx, state, now)
	if err != nil {
		return state, err
	}
	if state.State == domain.CircuitClosed {
		state.FailureCount, err = currentClosedFailureCount(ctx, tx, state, now, c.options.FailureWindow)
		if err != nil {
			return state, err
		}
	}
	var acquisition time.Time
	var revision, generation int64
	var reported *time.Time
	var outcome *string
	err = tx.QueryRow(ctx, "SELECT acquisition_started_at,backend_revision,circuit_generation,reported_outcome_at,outcome FROM backend_circuit_probes WHERE permit_id=$1::uuid FOR UPDATE", completion.AttemptID).Scan(&acquisition, &revision, &generation, &reported, &outcome)
	if err == nil {
		if revision != completion.Backend.Revision || generation != completion.Generation {
			return state, coordination.ReasonError{Reason: coordination.ReasonIdempotencyConflict}
		}
		if outcome != nil {
			if reported != nil && (!reported.Equal(completion.ReportedOutcomeAt) || *outcome != string(completion.Outcome)) && *outcome != "expired" && *outcome != "superseded" {
				return state, coordination.ReasonError{Reason: coordination.ReasonIdempotencyConflict}
			}
			return state, tx.Commit(ctx)
		}
		if !validCircuitOperationTime(completion.ReportedOutcomeAt, now) {
			return state, coordination.ReasonError{Reason: coordination.ReasonStaleOperation}
		}
		eventAt := completion.ReportedOutcomeAt.UTC()
		if eventAt.After(now) {
			eventAt = now
		}
		retainBase := now
		for _, candidate := range []time.Time{completion.ReportedOutcomeAt, acquisition} {
			if candidate.After(retainBase) {
				retainBase = candidate
			}
		}
		_, err = tx.Exec(ctx, "UPDATE backend_circuit_probes SET reported_outcome_at=$2::timestamptz,event_at=$3::timestamptz,processed_at=$4::timestamptz,outcome=$5::text,retain_until=$6::timestamptz WHERE permit_id=$1::uuid", completion.AttemptID, completion.ReportedOutcomeAt.UTC(), eventAt, now, string(completion.Outcome), retainBase.Add(circuitCompletionRetention))
		if err != nil {
			return state, err
		}
		if state.Generation != completion.Generation || state.Backend.Revision != completion.Backend.Revision || state.State != domain.CircuitHalfOpen {
			return state, tx.Commit(ctx)
		}
		switch completion.Outcome {
		case domain.InferenceFailure:
			if err := terminalizeGeneration(ctx, tx, completion.Backend.ID, completion.Generation, completion.AttemptID, now); err != nil {
				return state, err
			}
			state.FailureCount, eventAt, err = recordActiveFailure(ctx, tx, state, completion.AttemptID, eventAt, c.options.FailureWindow)
			if err != nil {
				return state, err
			}
			state.State = domain.CircuitOpen
			state.Generation++
			state.RetryAt = eventAt.Add(c.options.OpenCooldown)
			state.ProbesInFlight = 0
			_, err = tx.Exec(ctx, "UPDATE backend_circuit_state SET state='open',generation=$2::bigint,opened_at=$3::timestamptz,half_open_succeeded=false,updated_at=$4::timestamptz WHERE backend_id=$1::bigint", completion.Backend.ID, state.Generation, eventAt, now)
		case domain.InferenceSuccess:
			_, err = tx.Exec(ctx, "UPDATE backend_circuit_state SET half_open_succeeded=true,updated_at=$2::timestamptz WHERE backend_id=$1::bigint", completion.Backend.ID, now)
		}
		if err != nil {
			return state, err
		}
		var remaining int
		var succeeded bool
		if err = tx.QueryRow(ctx, "SELECT count(*) FROM backend_circuit_probes WHERE backend_id=$1::bigint AND backend_revision=$2::bigint AND circuit_generation=$3::bigint AND outcome IS NULL", completion.Backend.ID, completion.Backend.Revision, completion.Generation).Scan(&remaining); err != nil {
			return state, err
		}
		if err = tx.QueryRow(ctx, "SELECT half_open_succeeded FROM backend_circuit_state WHERE backend_id=$1::bigint", completion.Backend.ID).Scan(&succeeded); err != nil {
			return state, err
		}
		if state.State == domain.CircuitHalfOpen && succeeded && remaining == 0 {
			state.State = domain.CircuitClosed
			state.Generation++
			state.FailureCount = 0
			state.Available = true
			if _, err = tx.Exec(ctx, "DELETE FROM backend_circuit_active_failures WHERE backend_id=$1::bigint", completion.Backend.ID); err == nil {
				_, err = tx.Exec(ctx, "UPDATE backend_circuit_state SET state='closed',generation=$2::bigint,opened_at=NULL,half_open_succeeded=false,updated_at=$3::timestamptz WHERE backend_id=$1::bigint", completion.Backend.ID, state.Generation, now)
			}
		}
		if err == nil {
			_, _ = tx.Exec(ctx, "SELECT pg_notify('llmgw_circuit_changed',$1::text)", completion.Backend.ID)
		}
		return state, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return state, err
	}
	if completion.Outcome != domain.InferenceFailure {
		if !validCircuitOperationTime(completion.ReportedOutcomeAt, now) {
			return state, coordination.ReasonError{Reason: coordination.ReasonStaleOperation}
		}
		return state, tx.Commit(ctx)
	}
	var oldRevision, oldGeneration int64
	var oldReported time.Time
	err = tx.QueryRow(ctx, "SELECT backend_revision,circuit_generation,reported_outcome_at FROM backend_circuit_failure_receipts WHERE attempt_id=$1::uuid FOR UPDATE", completion.AttemptID).Scan(&oldRevision, &oldGeneration, &oldReported)
	if err == nil {
		if oldRevision != completion.Backend.Revision || oldGeneration != completion.Generation || !oldReported.Equal(completion.ReportedOutcomeAt) {
			return state, coordination.ReasonError{Reason: coordination.ReasonIdempotencyConflict}
		}
		return state, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return state, err
	}
	if !validCircuitOperationTime(completion.ReportedOutcomeAt, now) {
		return state, coordination.ReasonError{Reason: coordination.ReasonStaleOperation}
	}
	eventAt := completion.ReportedOutcomeAt.UTC()
	if eventAt.After(now) {
		eventAt = now
	}
	applied := state.State == domain.CircuitClosed && state.Backend.Revision == completion.Backend.Revision && state.Generation == completion.Generation && !eventAt.Before(now.Add(-c.options.FailureWindow))
	retainBase := now
	if completion.ReportedOutcomeAt.After(retainBase) {
		retainBase = completion.ReportedOutcomeAt
	}
	_, err = tx.Exec(ctx, "INSERT INTO backend_circuit_failure_receipts(attempt_id,backend_id,backend_revision,circuit_generation,reported_outcome_at,event_at,first_processed_at,applied,retain_until) VALUES($1::uuid,$2::bigint,$3::bigint,$4::bigint,$5::timestamptz,$6::timestamptz,$7::timestamptz,$8::boolean,$9::timestamptz)", completion.AttemptID, completion.Backend.ID, completion.Backend.Revision, completion.Generation, completion.ReportedOutcomeAt.UTC(), eventAt, now, applied, retainBase.Add(circuitCompletionRetention))
	if err != nil {
		return state, err
	}
	if applied {
		_, err = tx.Exec(ctx, "INSERT INTO backend_circuit_failures(attempt_id,backend_id,backend_revision,circuit_generation,event_at) VALUES($1::uuid,$2::bigint,$3::bigint,$4::bigint,$5::timestamptz)", completion.AttemptID, completion.Backend.ID, completion.Backend.Revision, completion.Generation, eventAt)
		if err != nil {
			return state, err
		}
		var count int
		var latest time.Time
		count, latest, err = recordActiveFailure(ctx, tx, state, completion.AttemptID, eventAt, c.options.FailureWindow)
		if err != nil {
			return state, err
		}
		_, err = tx.Exec(ctx, "DELETE FROM backend_circuit_failures WHERE backend_id=$1::bigint AND backend_revision=$2::bigint AND circuit_generation=$3::bigint AND event_at<$4::timestamptz", completion.Backend.ID, completion.Backend.Revision, completion.Generation, latest.Add(-c.options.FailureWindow))
		if err != nil {
			return state, err
		}
		state.FailureCount = count
		if count >= c.options.FailureThreshold {
			state.State = domain.CircuitOpen
			state.Generation++
			state.RetryAt = latest.Add(c.options.OpenCooldown)
			state.Available = false
			_, err = tx.Exec(ctx, "UPDATE backend_circuit_state SET state='open',generation=$2::bigint,opened_at=$3::timestamptz,half_open_succeeded=false,updated_at=$4::timestamptz WHERE backend_id=$1::bigint", completion.Backend.ID, state.Generation, latest, now)
			if err == nil {
				_, _ = tx.Exec(ctx, "SELECT pg_notify('llmgw_circuit_changed',$1::text)", completion.Backend.ID)
			}
		}
	}
	return state, tx.Commit(ctx)
}

func (c *CircuitCoordinator) completeClosedNonFailure(ctx context.Context, completion coordination.CircuitCompletion) (coordination.CircuitSnapshot, bool, error) {
	if completion.Outcome == domain.InferenceFailure {
		return coordination.CircuitSnapshot{}, false, nil
	}
	state, _, now, retainedPermit, err := c.readCommittedCircuit(ctx, completion.Backend.ID, completion.AttemptID)
	if errors.Is(err, pgx.ErrNoRows) {
		return coordination.CircuitSnapshot{}, false, nil
	}
	if err != nil {
		return coordination.CircuitSnapshot{}, false, err
	}
	if retainedPermit || state.State != domain.CircuitClosed {
		return coordination.CircuitSnapshot{}, false, nil
	}
	if !validCircuitOperationTime(completion.ReportedOutcomeAt, now) {
		return state, true, coordination.ReasonError{Reason: coordination.ReasonStaleOperation}
	}
	state.Available = true
	return state, true, nil
}

func validCircuitOperationTime(at, now time.Time) bool {
	return !at.After(now.Add(5*time.Minute)) && at.After(now.Add(-circuitCompletionRetention))
}

func terminalizeGeneration(ctx context.Context, tx pgx.Tx, backendID, generation int64, triggering uuid.UUID, now time.Time) error {
	_, err := tx.Exec(ctx, "UPDATE backend_circuit_probes SET outcome='superseded',processed_at=$4::timestamptz,retain_until=GREATEST($4::timestamptz,acquisition_started_at)+interval '24 hours' WHERE backend_id=$1::bigint AND circuit_generation=$2::bigint AND permit_id<>$3::uuid AND outcome IS NULL", backendID, generation, triggering, now)
	return err
}

func (c *CircuitCoordinator) RenewProbes(parent context.Context, probes []coordination.ProbeIdentity) ([]coordination.RenewResult, error) {
	results, err := c.renewProbes(parent, probes)
	if err != nil {
		err = recordCoordinationError(parent, err, c)
	} else {
		c.setStatus(true, "")
		_ = c.Refresh(parent)
	}
	return results, err
}

func (c *CircuitCoordinator) renewProbes(parent context.Context, probes []coordination.ProbeIdentity) ([]coordination.RenewResult, error) {
	if len(probes) == 0 {
		return []coordination.RenewResult{}, nil
	}
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	ordered := append([]coordination.ProbeIdentity(nil), probes...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Backend.ID == ordered[j].Backend.ID {
			return bytes.Compare(ordered[i].PermitID[:], ordered[j].PermitID[:]) < 0
		}
		return ordered[i].Backend.ID < ordered[j].Backend.ID
	})
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return nil, err
	}
	backendIDs := make([]int64, 0, len(ordered))
	for _, probe := range ordered {
		if len(backendIDs) == 0 || backendIDs[len(backendIDs)-1] != probe.Backend.ID {
			backendIDs = append(backendIDs, probe.Backend.ID)
		}
	}
	states := make(map[int64]coordination.CircuitSnapshot, len(backendIDs))
	for _, id := range backendIDs {
		state, lockErr := lockCircuitState(ctx, tx, id)
		if lockErr != nil {
			return nil, lockErr
		}
		states[id] = state
	}
	permitIDs := make([]string, len(ordered))
	for i, probe := range ordered {
		permitIDs[i] = probe.PermitID.String()
	}
	rows, err := tx.Query(ctx, `SELECT permit_id FROM backend_circuit_probes
		WHERE backend_id=ANY($1::bigint[]) AND (outcome IS NULL OR permit_id=ANY($2::uuid[]))
		ORDER BY backend_id,permit_id FOR UPDATE`, backendIDs, permitIDs)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var permitID uuid.UUID
		if err = rows.Scan(&permitID); err != nil {
			rows.Close()
			return nil, err
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return nil, err
	}
	for _, id := range backendIDs {
		state, _, reconcileErr := c.reconcileExpired(ctx, tx, states[id], now)
		if reconcileErr != nil {
			return nil, reconcileErr
		}
		states[id] = state
	}
	resultByID := make(map[uuid.UUID]coordination.RenewResult, len(ordered))
	for _, probe := range ordered {
		result := coordination.RenewResult{Lease: coordination.LeaseIdentity{LeaseID: probe.PermitID, ExpiresAt: probe.ExpiresAt, TTL: probe.TTL}}
		if probe.TTL <= 0 {
			result.Reason = coordination.ReasonLeaseLost
			resultByID[probe.PermitID] = result
			continue
		}
		tag, execErr := tx.Exec(ctx, "UPDATE backend_circuit_probes SET expires_at=$2::timestamptz WHERE permit_id=$1::uuid AND backend_id=$3::bigint AND backend_revision=$4::bigint AND circuit_generation=$5::bigint AND replica_id=$6::uuid AND outcome IS NULL AND expires_at>$7::timestamptz", probe.PermitID, now.Add(probe.TTL), probe.Backend.ID, probe.Backend.Revision, probe.Generation, probe.ReplicaID, now)
		if execErr != nil {
			return nil, execErr
		}
		if tag.RowsAffected() == 0 {
			result.Reason = coordination.ReasonLeaseLost
		} else {
			result.Lease.ExpiresAt = now.Add(probe.TTL)
		}
		resultByID[probe.PermitID] = result
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	out := make([]coordination.RenewResult, len(probes))
	for i, probe := range probes {
		out[i] = resultByID[probe.PermitID]
	}
	return out, nil
}

func circuitAcquireFingerprint(value coordination.CircuitAcquireRequest) [32]byte {
	encoded, _ := json.Marshal(value)
	return sha256.Sum256(encoded)
}

func retryableCoordinationError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01" || len(pgErr.Code) >= 2 && pgErr.Code[:2] == "08"
	}
	return pgconn.SafeToRetry(err)
}

func permanentCoordinationError(err error) bool {
	var permanent coordination.PermanentError
	if errors.As(err, &permanent) {
		return true
	}
	var reason coordination.ReasonError
	if errors.As(err, &reason) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || len(pgErr.Code) < 2 {
		return false
	}
	switch pgErr.Code[:2] {
	case "22", "23", "42", "XX":
		return true
	default:
		return false
	}
}

var _ coordination.CircuitCoordinator = (*CircuitCoordinator)(nil)
