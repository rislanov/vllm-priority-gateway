package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

const (
	CoordinationContractVersion = 4
	RateAlgorithmVersion        = 1
)

type ReplicaPolicy struct {
	LeaseTTL              time.Duration
	LeaseRenewInterval    time.Duration
	Circuit               circuitbreaker.Options
	RateAlgorithm         int
	ContractVersion       int
	Tokenization          coordination.TokenizationPolicy
	EmergencyTokenization coordination.TokenizationPolicy
}

func PolicyFingerprint(policy ReplicaPolicy) [32]byte {
	if policy.RateAlgorithm == 0 {
		policy.RateAlgorithm = RateAlgorithmVersion
	}
	if policy.ContractVersion == 0 {
		policy.ContractVersion = CoordinationContractVersion
	}
	encoded, _ := json.Marshal(policy)
	return sha256.Sum256(encoded)
}

type ReplicaManager struct {
	pool               *pgxpool.Pool
	replicaID          uuid.UUID
	startedAt          time.Time
	binary             string
	policy             ReplicaPolicy
	fingerprint        [32]byte
	interval           time.Duration
	timeout            time.Duration
	resetOnStart       bool
	startupWaitTimeout time.Duration
	logger             *slog.Logger

	mu           sync.RWMutex
	status       coordination.Status
	active       int
	incompatible bool
	cancel       context.CancelFunc
	done         chan struct{}
	failures     failurePublisher
}

// ReplicaManagerOptions controls startup operations, not coordination policy.
type ReplicaManagerOptions struct {
	// ResetOnStart removes incompatible registrations atomically with initial
	// registration. Operators must drain and stop all old processes first.
	ResetOnStart bool
	// StartupWaitTimeout bounds initial compatibility waiting; zero fails fast.
	StartupWaitTimeout time.Duration
	Logger             *slog.Logger
}

func NewReplicaManager(store *pgstore.Store, replicaID uuid.UUID, policy ReplicaPolicy, timeout time.Duration, options ...ReplicaManagerOptions) *ReplicaManager {
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	interval := policy.LeaseRenewInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	m := &ReplicaManager{
		pool: store.CoordinationPool(), replicaID: replicaID, startedAt: time.Now().UTC(),
		binary: binaryVersion(), policy: policy, fingerprint: PolicyFingerprint(policy),
		interval: interval, timeout: timeout, status: coordination.Status{Backend: "postgres"}, done: make(chan struct{}),
		resetOnStart: len(options) > 0 && options[0].ResetOnStart,
	}
	if len(options) > 0 {
		m.startupWaitTimeout = options[0].StartupWaitTimeout
		m.logger = options[0].Logger
	}
	return m
}

func binaryVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
		return info.Main.Version
	}
	return "unknown"
}

func (m *ReplicaManager) Start(parent context.Context) error {
	return m.StartRuntime(parent, parent)
}

// StartRuntime keeps startup registration cancellable independently of the
// heartbeat lifetime, which must cover the HTTP shutdown drain.
func (m *ReplicaManager) StartRuntime(startup, runtime context.Context) error {
	if err := m.register(startup); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(runtime)
	m.cancel = cancel
	go m.run(ctx)
	return nil
}

// register retries only a live incompatible registration, never database errors.
func (m *ReplicaManager) register(ctx context.Context) (resultErr error) {
	if m.startupWaitTimeout < 0 {
		return errors.New("coordination startup wait timeout must be non-negative")
	}
	if m.startupWaitTimeout == 0 {
		return m.heartbeat(ctx, true)
	}
	ctx, cancel := context.WithTimeout(ctx, m.startupWaitTimeout)
	defer cancel()
	waiting := false
	started := time.Now()
	defer func() {
		if waiting && m.logger != nil {
			if resultErr != nil {
				m.logger.Warn("coordination startup compatibility wait ended", "elapsed", time.Since(started), "error", resultErr)
			} else {
				m.logger.Info("coordination startup compatibility wait completed", "elapsed", time.Since(started))
			}
		}
	}()
	for {
		err := m.heartbeat(ctx, true)
		if err == nil {
			return nil
		}
		var reason coordination.ReasonError
		if !errors.As(err, &reason) || reason.Reason != coordination.ReasonIdempotencyConflict {
			return err
		}
		if !waiting && m.logger != nil {
			m.logger.Info("waiting for incompatible coordination replica registrations to expire or unregister", "timeout", m.startupWaitTimeout)
		}
		waiting = true
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("waiting for incompatible coordination replica registrations: %w: %w", ctx.Err(), err)
		case <-timer.C:
		}
	}
}

func isSerializationFailure(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == "40001"
}

func retryReplicaHeartbeat(ctx context.Context, operation func(context.Context) error) error {
	// Each retry starts a fresh transaction but shares the caller's deadline.
	// Back off so concurrent replica checks can finish instead of colliding again.
	for delay := 5 * time.Millisecond; ; delay = min(delay*2, 50*time.Millisecond) {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation(ctx)
		if err == nil || !isSerializationFailure(err) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *ReplicaManager) run(ctx context.Context) {
	defer close(m.done)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = m.heartbeat(ctx, false)
		}
	}
}

func (m *ReplicaManager) Close() {
	if m.cancel == nil {
		return
	}
	m.cancel()
	<-m.done
}

// CloseContext stops heartbeats and removes only this process's registration.
// Call after HTTP drain and lease completion, before closing the database. A
// failed or canceled cleanup leaves the heartbeat TTL as the fallback. It also
// cleans up a registration whose startup commit outcome was ambiguous.
func (m *ReplicaManager) CloseContext(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, m.timeout)
	defer cancel()
	if m.cancel != nil {
		m.cancel()
		select {
		case <-m.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := m.pool.Exec(ctx, "DELETE FROM coordination_replicas WHERE replica_id=$1::uuid AND policy_fingerprint=$2::bytea", m.replicaID, m.fingerprint[:])
	return err
}

func (m *ReplicaManager) Status() coordination.Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

func (m *ReplicaManager) ActiveCompatibleReplicas() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active
}
func (m *ReplicaManager) Compatible() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return !m.incompatible
}

func (m *ReplicaManager) SetFailureObserver(observer coordination.CoordinationFailureObserver) {
	m.failures.set(observer)
}

func (m *ReplicaManager) Check(ctx context.Context) error { return m.heartbeat(ctx, false) }

func (m *ReplicaManager) heartbeat(parent context.Context, registration bool) (resultErr error) {
	ctx, cancel := context.WithTimeout(parent, m.timeout)
	defer cancel()
	resultErr = retryReplicaHeartbeat(ctx, func(attemptCtx context.Context) error {
		return m.heartbeatOnce(attemptCtx, registration)
	})
	if resultErr != nil {
		var reason coordination.ReasonError
		if !permanentCoordinationError(resultErr) && !errors.As(resultErr, &reason) {
			m.failed()
		}
		if !registration {
			m.failures.report(resultErr)
		}
	}
	return resultErr
}

func (m *ReplicaManager) heartbeatOnce(parent context.Context, registration bool) (resultErr error) {
	defer func() {
		if resultErr == nil || !permanentCoordinationError(resultErr) {
			return
		}
		m.mu.Lock()
		m.status = coordination.Status{Backend: "postgres", Permanent: true, Reason: coordination.ReasonCoordinationUnavailable}
		m.mu.Unlock()
		var permanent coordination.PermanentError
		if !errors.As(resultErr, &permanent) {
			resultErr = coordination.PermanentError{Err: resultErr}
		}
	}()
	reset := registration && m.resetOnStart
	isolation := pgx.Serializable
	if reset {
		// The table writer lock below serializes reset/registration transactions.
		// READ COMMITTED gives waiting startups a fresh snapshot after the lock,
		// so matching new replicas are preserved without serialization retries.
		isolation = pgx.ReadCommitted
	}
	tx, err := m.pool.BeginTx(parent, pgx.TxOptions{IsoLevel: isolation})
	if err != nil {
		return err
	}
	defer tx.Rollback(parent)
	if _, err = tx.Exec(parent, "SET LOCAL synchronous_commit=on"); err != nil {
		return err
	}
	if reset {
		if _, err = tx.Exec(parent, "LOCK TABLE coordination_replicas IN SHARE ROW EXCLUSIVE MODE"); err != nil {
			return err
		}
		if _, err = tx.Exec(parent, "DELETE FROM coordination_replicas WHERE policy_fingerprint<>$1::bytea", m.fingerprint[:]); err != nil {
			return err
		}
	}
	var now time.Time
	if err = tx.QueryRow(parent, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return err
	}
	ttl := m.policy.LeaseTTL
	if ttl <= 0 {
		ttl = 90 * time.Second
	}
	expiresAt := now.Add(ttl)
	var incompatible int
	if err = tx.QueryRow(parent, "SELECT count(*) FROM coordination_replicas WHERE replica_id<>$1::uuid AND heartbeat_expires_at>$2::timestamptz AND policy_fingerprint<>$3::bytea", m.replicaID, now, m.fingerprint[:]).Scan(&incompatible); err != nil {
		return err
	}
	if incompatible > 0 {
		m.mu.Lock()
		if !registration {
			m.incompatible = true
		}
		m.status = coordination.Status{Backend: "postgres", Available: false, Reason: coordination.ReasonIdempotencyConflict}
		m.mu.Unlock()
		return coordination.ReasonError{Reason: coordination.ReasonIdempotencyConflict}
	}
	if registration {
		_, err = tx.Exec(parent, `INSERT INTO coordination_replicas(replica_id,started_at,heartbeat_at,heartbeat_expires_at,binary_version,coordination_contract_version,policy_fingerprint)
			VALUES($1::uuid,$2::timestamptz,$3::timestamptz,$4::timestamptz,$5::text,$6::integer,$7::bytea)
			ON CONFLICT(replica_id) DO UPDATE SET started_at=excluded.started_at,heartbeat_at=excluded.heartbeat_at,heartbeat_expires_at=excluded.heartbeat_expires_at,binary_version=excluded.binary_version,coordination_contract_version=excluded.coordination_contract_version,policy_fingerprint=excluded.policy_fingerprint`, m.replicaID, m.startedAt, now, expiresAt, m.binary, CoordinationContractVersion, m.fingerprint[:])
	} else {
		tag, updateErr := tx.Exec(parent, "UPDATE coordination_replicas SET heartbeat_at=$2::timestamptz,heartbeat_expires_at=$3::timestamptz WHERE replica_id=$1::uuid AND policy_fingerprint=$4::bytea", m.replicaID, now, expiresAt, m.fingerprint[:])
		err = updateErr
		if err == nil && tag.RowsAffected() == 0 {
			err = errors.New("replica registration was lost")
		}
	}
	if err != nil {
		return err
	}
	var active int
	if err = tx.QueryRow(parent, "SELECT count(*) FROM coordination_replicas WHERE heartbeat_expires_at>$1::timestamptz AND policy_fingerprint=$2::bytea", now, m.fingerprint[:]).Scan(&active); err != nil {
		return err
	}
	if err = tx.Commit(parent); err != nil {
		return err
	}
	m.mu.Lock()
	m.active = active
	// Incompatibility is permanent for this process. A full stop/start is
	// required before coordination-critical policy can change.
	if m.status.Permanent {
		m.mu.Unlock()
		return coordination.PermanentError{Err: errors.New("replica coordination fault is permanent for this process")}
	}
	if m.incompatible {
		m.mu.Unlock()
		return coordination.ReasonError{Reason: coordination.ReasonIdempotencyConflict}
	}
	m.status = coordination.Status{Backend: "postgres", Available: true, LastSuccess: time.Now().UTC()}
	m.mu.Unlock()
	return nil
}

func (m *ReplicaManager) failed() {
	m.mu.Lock()
	m.status.Available = false
	m.status.Degraded = true
	m.status.Reason = coordination.ReasonCoordinationUnavailable
	m.mu.Unlock()
}
