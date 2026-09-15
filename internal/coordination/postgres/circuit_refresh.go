package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Refresh reconciles all due states with ordered bulk locks and statements.
// Idle half-open circuits require no transaction until a permit expires. Their
// admission capacity is still read from the final authoritative snapshot.
func (c *CircuitCoordinator) reconcileRefreshStates(ctx context.Context) error {
	ids, err := c.reconcilableBackendIDs(ctx)
	if err != nil || len(ids) == 0 {
		return err
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	locks := &pgx.Batch{}
	locks.Queue("SET LOCAL synchronous_commit=on")
	locks.Queue("SELECT backend_id FROM backend_circuit_state WHERE backend_id=ANY($1::bigint[]) ORDER BY backend_id FOR UPDATE", ids)
	locks.Queue("SELECT permit_id FROM backend_circuit_probes WHERE backend_id=ANY($1::bigint[]) AND outcome IS NULL ORDER BY backend_id,permit_id FOR UPDATE", ids)
	if err := tx.SendBatch(ctx, locks).Close(); err != nil {
		return err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT ON (s.backend_id) s.backend_id,s.backend_revision,s.generation,p.permit_id,p.expires_at
		FROM backend_circuit_state s JOIN backend_circuit_probes p ON p.backend_id=s.backend_id
		AND p.backend_revision=s.backend_revision AND p.circuit_generation=s.generation
		WHERE s.backend_id=ANY($1::bigint[]) AND s.state='half_open' AND p.outcome IS NULL AND p.expires_at<=$2::timestamptz
		ORDER BY s.backend_id,p.expires_at,p.permit_id`, ids, now)
	if err != nil {
		return err
	}
	expiredIDs := make([]int64, 0)
	revisions := make([]int64, 0)
	generations := make([]int64, 0)
	permits := make([]string, 0)
	expiries := make([]time.Time, 0)
	for rows.Next() {
		var id, revision, generation int64
		var permit uuid.UUID
		var expiry time.Time
		if err := rows.Scan(&id, &revision, &generation, &permit, &expiry); err != nil {
			rows.Close()
			return err
		}
		expiredIDs = append(expiredIDs, id)
		revisions = append(revisions, revision)
		generations = append(generations, generation)
		permits = append(permits, permit.String())
		expiries = append(expiries, expiry)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(expiredIDs) > 0 {
		writes := &pgx.Batch{}
		writes.Queue(`UPDATE backend_circuit_probes p SET outcome=CASE WHEN p.permit_id=e.permit_id THEN 'expired' ELSE 'superseded' END,
			processed_at=$5::timestamptz,retain_until=GREATEST($5::timestamptz,p.acquisition_started_at)+interval '24 hours'
			FROM unnest($1::bigint[],$2::bigint[],$3::bigint[],$4::uuid[]) e(backend_id,revision,generation,permit_id)
			WHERE p.backend_id=e.backend_id AND p.backend_revision=e.revision AND p.circuit_generation=e.generation AND p.outcome IS NULL`, expiredIDs, revisions, generations, permits, now)
		writes.Queue(`INSERT INTO backend_circuit_active_failures(attempt_id,backend_id,backend_revision,event_at)
			SELECT * FROM unnest($1::uuid[],$2::bigint[],$3::bigint[],$4::timestamptz[]) ON CONFLICT(attempt_id) DO NOTHING`, permits, expiredIDs, revisions, expiries)
		writes.Queue(`DELETE FROM backend_circuit_active_failures f USING
			(SELECT backend_id,backend_revision,max(event_at) AS latest FROM backend_circuit_active_failures
			WHERE backend_id=ANY($1::bigint[]) GROUP BY backend_id,backend_revision) a
			WHERE f.backend_id=a.backend_id AND f.backend_revision=a.backend_revision AND f.event_at<a.latest-$2::interval`, expiredIDs, c.options.FailureWindow)
		writes.Queue(`UPDATE backend_circuit_state s SET state='open',generation=generation+1,
			opened_at=(SELECT max(event_at) FROM backend_circuit_active_failures f WHERE f.backend_id=s.backend_id AND f.backend_revision=s.backend_revision),
			half_open_succeeded=false,updated_at=$2::timestamptz WHERE s.backend_id=ANY($1::bigint[])`, expiredIDs, now)
		if err := tx.SendBatch(ctx, writes).Close(); err != nil {
			return err
		}
	}
	// A just-expired generation remains open for this refresh, matching the
	// ordinary per-operation expiry path. The next refresh/acquire handles its
	// cooldown. Existing open states can advance together without creating probes.
	tag, err := tx.Exec(ctx, `UPDATE backend_circuit_state SET state='half_open',generation=generation+1,
		opened_at=NULL,half_open_succeeded=false,updated_at=$3::timestamptz
		WHERE backend_id=ANY($1::bigint[]) AND NOT(backend_id=ANY($2::bigint[]))
		AND state='open' AND opened_at+$4::interval<=$3::timestamptz`, ids, expiredIDs, now, c.options.OpenCooldown)
	if err != nil {
		return err
	}
	if len(expiredIDs) > 0 || tag.RowsAffected() > 0 {
		if _, err := tx.Exec(ctx, "SELECT pg_notify('llmgw_circuit_changed','')"); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
