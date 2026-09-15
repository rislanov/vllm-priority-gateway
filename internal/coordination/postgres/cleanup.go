package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type expiredLease struct {
	id     uuid.UUID
	pool   int64
	client int64
}

// Cleanup removes only bounded terminal history. Correctness never depends on
// physical deletion: every admission and renewal still compares expires_at.
func (c *AdmissionCoordinator) Cleanup(parent context.Context, batch int) error {
	_, err := c.CleanupBatch(parent, batch)
	return err
}

// CleanupBatch reports whether any candidate category filled its batch, so a
// bounded maintenance pass can continue draining without waiting for a tick.
func (c *AdmissionCoordinator) CleanupBatch(parent context.Context, batch int) (more bool, resultErr error) {
	defer func() { resultErr = recordCoordinationError(parent, resultErr, c) }()
	if batch <= 0 || batch > 1000 {
		batch = 256
	}
	ctx, cancel := c.deadline(parent)
	defer cancel()
	rows, err := c.pool.Query(ctx, "SELECT lease_id,pool_id,client_id FROM request_leases WHERE expires_at<=clock_timestamp() ORDER BY lease_id LIMIT $1::integer", batch)
	if err != nil {
		return false, err
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (v expiredLease, err error) {
		err = row.Scan(&v.id, &v.pool, &v.client)
		return
	})
	if err != nil {
		return false, err
	}
	if len(items) > 0 {
		if err := c.cleanupExpired(ctx, items); err != nil {
			return false, err
		}
	}
	receiptsFull, err := c.cleanupAdmissionReceipts(ctx, batch)
	return len(items) == batch || receiptsFull, err
}

func (c *AdmissionCoordinator) cleanupAdmissionReceipts(ctx context.Context, batch int) (bool, error) {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return false, err
	}
	// A completion can extend retention while cleanup is selecting candidates.
	// Lock receipts before deciding to delete them, and leave active completions
	// for a later pass instead of waiting on their receipt locks.
	rows, err := tx.Query(ctx, `SELECT o.lease_id FROM admission_operations o
		WHERE o.retain_until<=clock_timestamp()
		AND NOT EXISTS (SELECT 1 FROM request_leases l WHERE l.lease_id=o.lease_id)
		ORDER BY o.lease_id LIMIT $1::integer FOR UPDATE OF o SKIP LOCKED`, batch)
	if err != nil {
		return false, err
	}
	receipts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		var id uuid.UUID
		err := row.Scan(&id)
		return id.String(), err
	})
	if err != nil {
		return false, err
	}
	if len(receipts) > 0 {
		var now time.Time
		if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM admission_operations o
			WHERE o.lease_id=ANY($1::uuid[]) AND o.retain_until<=$2::timestamptz
			AND NOT EXISTS (SELECT 1 FROM request_leases l WHERE l.lease_id=o.lease_id)`, receipts, now); err != nil {
			return false, err
		}
	}
	return len(receipts) == batch, tx.Commit(ctx)
}

func (c *AdmissionCoordinator) cleanupExpired(ctx context.Context, items []expiredLease) error {
	pools, clients := make([]int64, 0, len(items)), make([]int64, 0, len(items))
	leases := make([]string, 0, len(items))
	for _, item := range items {
		pools = append(pools, item.pool)
		clients = append(clients, item.client)
		leases = append(leases, item.id.String())
	}
	pools, clients = sortedUnique(pools), sortedUnique(clients)
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return err
	}
	// Each statement consumes its full ordered result before the next lock
	// category starts. Keep the global pool/client/lease/receipt lock order
	// without a network round trip for every row in the batch.
	if _, err = tx.Exec(ctx, "SELECT pool_id FROM pool_admission_scopes WHERE pool_id=ANY($1::bigint[]) ORDER BY pool_id FOR UPDATE", pools); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "SELECT client_id FROM client_admission_scopes WHERE client_id=ANY($1::bigint[]) ORDER BY client_id FOR UPDATE", clients); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "SELECT lease_id FROM request_leases WHERE lease_id=ANY($1::uuid[]) ORDER BY lease_id FOR UPDATE", leases); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "SELECT lease_id FROM admission_operations WHERE lease_id=ANY($1::uuid[]) ORDER BY lease_id FOR UPDATE", leases); err != nil {
		return err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, "DELETE FROM request_leases WHERE lease_id=ANY($1::uuid[]) AND expires_at<=$2::timestamptz RETURNING lease_id", leases, now)
	if err != nil {
		return err
	}
	expired, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		var id uuid.UUID
		err := row.Scan(&id)
		return id.String(), err
	})
	if err != nil {
		return err
	}
	if len(expired) > 0 {
		_, err = tx.Exec(ctx, "UPDATE admission_operations SET lease_expired_at=COALESCE(lease_expired_at,$2::timestamptz),retain_until=GREATEST(operation_started_at,$2::timestamptz)+interval '24 hours' WHERE lease_id=ANY($1::uuid[]) AND completed_at IS NULL", expired, now)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (c *CircuitCoordinator) Cleanup(parent context.Context, batch int) error {
	_, err := c.CleanupBatch(parent, batch)
	return err
}

// CleanupBatch reports whether any terminal history category filled its batch.
func (c *CircuitCoordinator) CleanupBatch(parent context.Context, batch int) (more bool, resultErr error) {
	defer func() { resultErr = recordCoordinationError(parent, resultErr, c) }()
	if batch <= 0 || batch > 1000 {
		batch = 256
	}
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return false, err
	}
	probes, err := tx.Exec(ctx, `DELETE FROM backend_circuit_probes WHERE permit_id IN (SELECT permit_id FROM backend_circuit_probes WHERE outcome IS NOT NULL AND retain_until<=clock_timestamp() ORDER BY permit_id LIMIT $1::integer)`, batch)
	if err != nil {
		return false, err
	}
	rows, err := tx.Query(ctx, "SELECT attempt_id FROM backend_circuit_failure_receipts WHERE retain_until<=clock_timestamp() ORDER BY attempt_id LIMIT $1::integer FOR UPDATE", batch)
	if err != nil {
		return false, err
	}
	receipts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		var id uuid.UUID
		err := row.Scan(&id)
		return id.String(), err
	})
	if err != nil {
		return false, err
	}
	if len(receipts) > 0 {
		if _, err = tx.Exec(ctx, "DELETE FROM backend_circuit_failures WHERE attempt_id=ANY($1::uuid[])", receipts); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, "DELETE FROM backend_circuit_failure_receipts WHERE attempt_id=ANY($1::uuid[])", receipts); err != nil {
			return false, err
		}
	}
	return probes.RowsAffected() == int64(batch) || len(receipts) == batch, tx.Commit(ctx)
}
