package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

var ErrPoolHasBackends = errors.New("model pool cannot be deleted while backends reference it")

func (s *SQLite) CreatePool(ctx context.Context, params CreatePoolParams) (domain.ModelPool, error) {
	pool := domain.ModelPool{
		PublicModelName: params.PublicModelName, UpstreamModelName: params.UpstreamModelName,
		Enabled: params.Enabled, MaxGatewayInflight: params.MaxGatewayInflight, MaxWaiting: params.MaxWaiting,
	}
	if err := pool.Validate(); err != nil {
		return domain.ModelPool{}, err
	}
	now := s.now().UTC()
	pool.CreatedAt, pool.UpdatedAt = now, now
	tx, err := s.begin(ctx)
	if err != nil {
		return domain.ModelPool{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO model_pools (public_model_name, upstream_model_name, enabled, max_gateway_inflight, max_waiting, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		pool.PublicModelName, pool.UpstreamModelName, boolInt(pool.Enabled), pool.MaxGatewayInflight, pool.MaxWaiting, timestamp(now), timestamp(now),
	)
	if err != nil {
		return domain.ModelPool{}, fmt.Errorf("insert model pool: %w", err)
	}
	pool.ID, err = result.LastInsertId()
	if err != nil {
		return domain.ModelPool{}, fmt.Errorf("read model pool ID: %w", err)
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return domain.ModelPool{}, err
	}
	if err := commit(tx); err != nil {
		return domain.ModelPool{}, err
	}
	return pool, nil
}

func (s *SQLite) UpdatePool(ctx context.Context, id int64, params UpdatePoolParams) (domain.ModelPool, error) {
	pool := domain.ModelPool{
		ID: id, PublicModelName: params.PublicModelName, UpstreamModelName: params.UpstreamModelName,
		Enabled: params.Enabled, MaxGatewayInflight: params.MaxGatewayInflight, MaxWaiting: params.MaxWaiting,
	}
	if err := pool.Validate(); err != nil {
		return domain.ModelPool{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return domain.ModelPool{}, err
	}
	defer tx.Rollback()
	pool.UpdatedAt = s.now().UTC()
	var created string
	err = tx.QueryRowContext(ctx, `
		UPDATE model_pools SET public_model_name = ?, upstream_model_name = ?, enabled = ?, max_gateway_inflight = ?, max_waiting = ?, updated_at = ?
		WHERE id = ? RETURNING created_at`, pool.PublicModelName, pool.UpstreamModelName, boolInt(pool.Enabled), pool.MaxGatewayInflight, pool.MaxWaiting, timestamp(pool.UpdatedAt), id).Scan(&created)
	if err != nil {
		return domain.ModelPool{}, fmt.Errorf("update model pool: %w", err)
	}
	pool.CreatedAt, err = parseTimestamp(created)
	if err != nil {
		return domain.ModelPool{}, err
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return domain.ModelPool{}, err
	}
	if err := commit(tx); err != nil {
		return domain.ModelPool{}, err
	}
	return pool, nil
}

func (s *SQLite) DeletePool(ctx context.Context, id int64) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var backendCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM backends WHERE model_pool_id = ?`, id).Scan(&backendCount); err != nil {
		return fmt.Errorf("count model pool backends: %w", err)
	}
	if backendCount != 0 {
		return ErrPoolHasBackends
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM model_pools WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete model pool: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return err
	}
	return commit(tx)
}

func (s *SQLite) ListPools(ctx context.Context) ([]domain.ModelPool, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, public_model_name, upstream_model_name, enabled, max_gateway_inflight, max_waiting, created_at, updated_at
		FROM model_pools ORDER BY public_model_name`)
	if err != nil {
		return nil, fmt.Errorf("list model pools: %w", err)
	}
	defer rows.Close()
	var pools []domain.ModelPool
	for rows.Next() {
		pool, err := scanPool(rows)
		if err != nil {
			return nil, err
		}
		pools = append(pools, pool)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate model pools: %w", err)
	}
	return pools, nil
}

func scanPool(row scanner) (domain.ModelPool, error) {
	var pool domain.ModelPool
	var enabled int
	var created, updated string
	if err := row.Scan(&pool.ID, &pool.PublicModelName, &pool.UpstreamModelName, &enabled, &pool.MaxGatewayInflight, &pool.MaxWaiting, &created, &updated); err != nil {
		return domain.ModelPool{}, fmt.Errorf("scan model pool: %w", err)
	}
	pool.Enabled = enabled != 0
	var err error
	if pool.CreatedAt, err = parseTimestamp(created); err != nil {
		return domain.ModelPool{}, err
	}
	if pool.UpdatedAt, err = parseTimestamp(updated); err != nil {
		return domain.ModelPool{}, err
	}
	return pool, nil
}
