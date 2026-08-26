package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
)

func (s *SQLite) LoadSnapshot(ctx context.Context) (registry.Data, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return registry.Data{}, fmt.Errorf("begin configuration snapshot: %w", err)
	}
	defer tx.Rollback()
	var data registry.Data
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM config_meta WHERE singleton = 1`).Scan(&data.Revision); err != nil {
		return registry.Data{}, fmt.Errorf("read configuration revision: %w", err)
	}
	if data.Clients, err = snapshotClients(ctx, tx); err != nil {
		return registry.Data{}, err
	}
	if data.Keys, err = snapshotKeys(ctx, tx); err != nil {
		return registry.Data{}, err
	}
	if data.Pools, err = snapshotPools(ctx, tx); err != nil {
		return registry.Data{}, err
	}
	if data.Access, err = snapshotAccess(ctx, tx); err != nil {
		return registry.Data{}, err
	}
	if data.Backends, err = snapshotBackends(ctx, tx); err != nil {
		return registry.Data{}, err
	}
	if err := tx.Commit(); err != nil {
		return registry.Data{}, fmt.Errorf("commit read snapshot: %w", err)
	}
	return data, nil
}

func snapshotClients(ctx context.Context, tx *sql.Tx) ([]domain.Client, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, name, enabled, priority_class, vllm_priority, max_concurrency, created_at, updated_at
		FROM clients ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("snapshot clients: %w", err)
	}
	defer rows.Close()
	var values []domain.Client
	for rows.Next() {
		value, err := scanClient(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func snapshotKeys(ctx context.Context, tx *sql.Tx) ([]domain.APIKey, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, client_id, prefix, secret_hash, created_at, expires_at, revoked_at, last_used_at
		FROM api_keys ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("snapshot API keys: %w", err)
	}
	defer rows.Close()
	var values []domain.APIKey
	for rows.Next() {
		value, err := scanAPIKey(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func snapshotPools(ctx context.Context, tx *sql.Tx) ([]domain.ModelPool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, public_model_name, upstream_model_name, enabled, created_at, updated_at
		FROM model_pools ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("snapshot model pools: %w", err)
	}
	defer rows.Close()
	var values []domain.ModelPool
	for rows.Next() {
		value, err := scanPool(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func snapshotAccess(ctx context.Context, tx *sql.Tx) ([]domain.ClientModelAccess, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT client_id, model_pool_id, enabled FROM client_model_access
		ORDER BY client_id, model_pool_id`)
	if err != nil {
		return nil, fmt.Errorf("snapshot client model access: %w", err)
	}
	defer rows.Close()
	var values []domain.ClientModelAccess
	for rows.Next() {
		var value domain.ClientModelAccess
		var enabled int
		if err := rows.Scan(&value.ClientID, &value.ModelPoolID, &enabled); err != nil {
			return nil, fmt.Errorf("scan client model access: %w", err)
		}
		value.Enabled = enabled != 0
		values = append(values, value)
	}
	return values, rows.Err()
}

func snapshotBackends(ctx context.Context, tx *sql.Tx) ([]domain.Backend, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, model_pool_id, name, base_url, enabled, draining, capacity_hint,
		running_soft_limit, upstream_api_key_env, created_at, updated_at
		FROM backends ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("snapshot backends: %w", err)
	}
	defer rows.Close()
	var values []domain.Backend
	for rows.Next() {
		value, err := scanBackend(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}
