package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func (s *SQLite) CreatePool(ctx context.Context, params CreatePoolParams) (domain.ModelPool, error) {
	pool := domain.ModelPool{
		PublicModelName: params.PublicModelName, UpstreamModelName: params.UpstreamModelName,
		Enabled: params.Enabled,
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
		INSERT INTO model_pools (public_model_name, upstream_model_name, enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)`,
		pool.PublicModelName, pool.UpstreamModelName, boolInt(pool.Enabled), timestamp(now), timestamp(now),
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
	pool := domain.ModelPool{ID: id, PublicModelName: params.PublicModelName, UpstreamModelName: params.UpstreamModelName, Enabled: params.Enabled}
	if err := pool.Validate(); err != nil {
		return domain.ModelPool{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return domain.ModelPool{}, err
	}
	defer tx.Rollback()
	var created string
	if err := tx.QueryRowContext(ctx, `SELECT created_at FROM model_pools WHERE id = ?`, id).Scan(&created); err != nil {
		return domain.ModelPool{}, fmt.Errorf("find model pool %d: %w", id, err)
	}
	pool.CreatedAt, err = parseTimestamp(created)
	if err != nil {
		return domain.ModelPool{}, err
	}
	pool.UpdatedAt = s.now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE model_pools SET public_model_name = ?, upstream_model_name = ?, enabled = ?, updated_at = ?
		WHERE id = ?`, pool.PublicModelName, pool.UpstreamModelName, boolInt(pool.Enabled), timestamp(pool.UpdatedAt), id)
	if err != nil {
		return domain.ModelPool{}, fmt.Errorf("update model pool: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return domain.ModelPool{}, sql.ErrNoRows
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return domain.ModelPool{}, err
	}
	if err := commit(tx); err != nil {
		return domain.ModelPool{}, err
	}
	return pool, nil
}

func (s *SQLite) ListPools(ctx context.Context) ([]domain.ModelPool, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, public_model_name, upstream_model_name, enabled, created_at, updated_at
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
	if err := row.Scan(&pool.ID, &pool.PublicModelName, &pool.UpstreamModelName, &enabled, &created, &updated); err != nil {
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
