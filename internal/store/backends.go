package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func (s *SQLite) CreateBackend(ctx context.Context, params CreateBackendParams) (domain.Backend, error) {
	backend := backendFromParams(0, params)
	if err := backend.Validate(); err != nil {
		return domain.Backend{}, err
	}
	now := s.now().UTC()
	backend.CreatedAt, backend.UpdatedAt = now, now
	tx, err := s.begin(ctx)
	if err != nil {
		return domain.Backend{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO backends (
			model_pool_id, name, base_url, enabled, draining, capacity_hint,
			running_soft_limit, upstream_api_key_env, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		backend.ModelPoolID, backend.Name, backend.BaseURL, boolInt(backend.Enabled),
		boolInt(backend.Draining), backend.CapacityHint, backend.RunningSoftLimit,
		backend.UpstreamAPIKeyEnv, timestamp(now), timestamp(now),
	)
	if err != nil {
		return domain.Backend{}, fmt.Errorf("insert backend: %w", err)
	}
	backend.ID, err = result.LastInsertId()
	if err != nil {
		return domain.Backend{}, fmt.Errorf("read backend ID: %w", err)
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return domain.Backend{}, err
	}
	if err := commit(tx); err != nil {
		return domain.Backend{}, err
	}
	return backend, nil
}

func (s *SQLite) UpdateBackend(ctx context.Context, id int64, params UpdateBackendParams) (domain.Backend, error) {
	backend := backendFromParams(id, params)
	if err := backend.Validate(); err != nil {
		return domain.Backend{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return domain.Backend{}, err
	}
	defer tx.Rollback()
	var created string
	if err := tx.QueryRowContext(ctx, `SELECT created_at FROM backends WHERE id = ?`, id).Scan(&created); err != nil {
		return domain.Backend{}, fmt.Errorf("find backend %d: %w", id, err)
	}
	backend.CreatedAt, err = parseTimestamp(created)
	if err != nil {
		return domain.Backend{}, err
	}
	backend.UpdatedAt = s.now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE backends SET model_pool_id = ?, name = ?, base_url = ?, enabled = ?, draining = ?,
		capacity_hint = ?, running_soft_limit = ?, upstream_api_key_env = ?, updated_at = ? WHERE id = ?`,
		backend.ModelPoolID, backend.Name, backend.BaseURL, boolInt(backend.Enabled),
		boolInt(backend.Draining), backend.CapacityHint, backend.RunningSoftLimit,
		backend.UpstreamAPIKeyEnv, timestamp(backend.UpdatedAt), id,
	)
	if err != nil {
		return domain.Backend{}, fmt.Errorf("update backend: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return domain.Backend{}, sql.ErrNoRows
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return domain.Backend{}, err
	}
	if err := commit(tx); err != nil {
		return domain.Backend{}, err
	}
	return backend, nil
}

func (s *SQLite) SetBackendDraining(ctx context.Context, id int64, draining bool) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE backends SET draining = ?, updated_at = ? WHERE id = ?`, boolInt(draining), timestamp(s.now()), id)
	if err != nil {
		return fmt.Errorf("set backend draining: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return err
	}
	return commit(tx)
}

func (s *SQLite) ListBackends(ctx context.Context) ([]domain.Backend, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, model_pool_id, name, base_url, enabled, draining, capacity_hint,
		running_soft_limit, upstream_api_key_env, created_at, updated_at
		FROM backends ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list backends: %w", err)
	}
	defer rows.Close()
	var backends []domain.Backend
	for rows.Next() {
		backend, err := scanBackend(rows)
		if err != nil {
			return nil, err
		}
		backends = append(backends, backend)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate backends: %w", err)
	}
	return backends, nil
}

func backendFromParams(id int64, params CreateBackendParams) domain.Backend {
	return domain.Backend{
		ID: id, ModelPoolID: params.ModelPoolID, Name: params.Name, BaseURL: params.BaseURL,
		Enabled: params.Enabled, Draining: params.Draining, CapacityHint: params.CapacityHint,
		RunningSoftLimit: params.RunningSoftLimit, UpstreamAPIKeyEnv: params.UpstreamAPIKeyEnv,
	}
}

func scanBackend(row scanner) (domain.Backend, error) {
	var backend domain.Backend
	var enabled, draining int
	var created, updated string
	if err := row.Scan(
		&backend.ID, &backend.ModelPoolID, &backend.Name, &backend.BaseURL, &enabled,
		&draining, &backend.CapacityHint, &backend.RunningSoftLimit,
		&backend.UpstreamAPIKeyEnv, &created, &updated,
	); err != nil {
		return domain.Backend{}, fmt.Errorf("scan backend: %w", err)
	}
	backend.Enabled = enabled != 0
	backend.Draining = draining != 0
	var err error
	if backend.CreatedAt, err = parseTimestamp(created); err != nil {
		return domain.Backend{}, err
	}
	if backend.UpdatedAt, err = parseTimestamp(updated); err != nil {
		return domain.Backend{}, err
	}
	return backend, nil
}
