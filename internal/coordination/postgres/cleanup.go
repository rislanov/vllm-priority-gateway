package postgres

import (
	"bytes"
	"context"
	"sort"
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
	if batch <= 0 || batch > 1000 {
		batch = 256
	}
	ctx, cancel := c.deadline(parent)
	defer cancel()
	rows, err := c.pool.Query(ctx, "SELECT lease_id,pool_id,client_id FROM request_leases WHERE expires_at<=clock_timestamp() ORDER BY lease_id LIMIT $1::integer", batch)
	if err != nil {
		return err
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (v expiredLease, err error) {
		err = row.Scan(&v.id, &v.pool, &v.client)
		return
	})
	if err != nil {
		return err
	}
	if len(items) > 0 {
		if err := c.cleanupExpired(ctx, items); err != nil {
			return err
		}
	}
	return c.cleanupAdmissionReceipts(ctx, batch)
}

func (c *AdmissionCoordinator) cleanupAdmissionReceipts(ctx context.Context, batch int) error {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM admission_operations WHERE lease_id IN (
		SELECT o.lease_id FROM admission_operations o LEFT JOIN request_leases l USING(lease_id)
		WHERE l.lease_id IS NULL AND o.retain_until<=clock_timestamp() ORDER BY o.lease_id LIMIT $1::integer)`, batch); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *AdmissionCoordinator) cleanupExpired(ctx context.Context, items []expiredLease) error {
	pools, clients := make([]int64, 0, len(items)), make([]int64, 0, len(items))
	for _, item := range items {
		pools = append(pools, item.pool)
		clients = append(clients, item.client)
	}
	pools, clients = sortedUnique(pools), sortedUnique(clients)
	sort.Slice(items, func(i, j int) bool { return bytes.Compare(items[i].id[:], items[j].id[:]) < 0 })
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return err
	}
	for _, id := range pools {
		if _, err = tx.Exec(ctx, "SELECT pool_id FROM pool_admission_scopes WHERE pool_id=$1::bigint FOR UPDATE", id); err != nil {
			return err
		}
	}
	for _, id := range clients {
		if _, err = tx.Exec(ctx, "SELECT client_id FROM client_admission_scopes WHERE client_id=$1::bigint FOR UPDATE", id); err != nil {
			return err
		}
	}
	for _, item := range items {
		if _, err = tx.Exec(ctx, "SELECT lease_id FROM request_leases WHERE lease_id=$1::uuid FOR UPDATE", item.id); err != nil {
			return err
		}
	}
	for _, item := range items {
		if _, err = tx.Exec(ctx, "SELECT lease_id FROM admission_operations WHERE lease_id=$1::uuid FOR UPDATE", item.id); err != nil {
			return err
		}
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return err
	}
	for _, item := range items {
		tag, deleteErr := tx.Exec(ctx, "DELETE FROM request_leases WHERE lease_id=$1::uuid AND expires_at<=$2::timestamptz", item.id, now)
		if deleteErr != nil {
			return deleteErr
		}
		if tag.RowsAffected() == 1 {
			_, err = tx.Exec(ctx, "UPDATE admission_operations SET lease_expired_at=COALESCE(lease_expired_at,$2::timestamptz),retain_until=GREATEST(operation_started_at,$2::timestamptz)+interval '24 hours' WHERE lease_id=$1::uuid AND completed_at IS NULL", item.id, now)
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

func (c *CircuitCoordinator) Cleanup(parent context.Context, batch int) error {
	if batch <= 0 || batch > 1000 {
		batch = 256
	}
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
	if _, err = tx.Exec(ctx, `DELETE FROM backend_circuit_probes WHERE permit_id IN (SELECT permit_id FROM backend_circuit_probes WHERE outcome IS NOT NULL AND retain_until<=clock_timestamp() ORDER BY permit_id LIMIT $1::integer)`, batch); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, "SELECT attempt_id FROM backend_circuit_failure_receipts WHERE retain_until<=clock_timestamp() ORDER BY attempt_id LIMIT $1::integer FOR UPDATE", batch)
	if err != nil {
		return err
	}
	receipts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (uuid.UUID, error) {
		var id uuid.UUID
		err := row.Scan(&id)
		return id, err
	})
	if err != nil {
		return err
	}
	if len(receipts) > 0 {
		if _, err = tx.Exec(ctx, "DELETE FROM backend_circuit_failures WHERE attempt_id=ANY($1::uuid[])", receipts); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "DELETE FROM backend_circuit_failure_receipts WHERE attempt_id=ANY($1::uuid[])", receipts); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
