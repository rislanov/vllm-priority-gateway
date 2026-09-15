package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func (s *SQLite) CreateClient(ctx context.Context, params CreateClientParams) (domain.Client, error) {
	client := domain.Client{
		Name: params.Name, Enabled: params.Enabled, PriorityClass: params.PriorityClass,
		VLLMPriority: params.VLLMPriority, MaxConcurrency: params.MaxConcurrency,
		RequestsPerMinute: params.RequestsPerMinute, TokensPerMinute: params.TokensPerMinute, Revision: 1,
	}
	if err := client.Validate(); err != nil {
		return domain.Client{}, err
	}
	now := s.now().UTC()
	client.CreatedAt = now
	client.UpdatedAt = now

	tx, err := s.begin(ctx)
	if err != nil {
		return domain.Client{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO clients (name, enabled, priority_class, vllm_priority, max_concurrency, requests_per_minute, tokens_per_minute, revision, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		client.Name, boolInt(client.Enabled), client.PriorityClass, client.VLLMPriority,
		client.MaxConcurrency, client.RequestsPerMinute, client.TokensPerMinute, client.Revision, timestamp(now), timestamp(now),
	)
	if err != nil {
		return domain.Client{}, fmt.Errorf("insert client: %w", err)
	}
	client.ID, err = result.LastInsertId()
	if err != nil {
		return domain.Client{}, fmt.Errorf("read client ID: %w", err)
	}
	if err := replaceClientAccess(ctx, tx, client.ID, params.ModelPoolIDs); err != nil {
		return domain.Client{}, err
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return domain.Client{}, err
	}
	if err := commit(tx); err != nil {
		return domain.Client{}, err
	}
	return client, nil
}

func (s *SQLite) UpdateClient(ctx context.Context, id int64, params UpdateClientParams) (domain.Client, error) {
	client := domain.Client{
		ID: id, Name: params.Name, Enabled: params.Enabled, PriorityClass: params.PriorityClass,
		VLLMPriority: params.VLLMPriority, MaxConcurrency: params.MaxConcurrency,
		RequestsPerMinute: params.RequestsPerMinute, TokensPerMinute: params.TokensPerMinute,
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return domain.Client{}, err
	}
	defer tx.Rollback()
	var previous domain.Client
	// Reserve the writer before reading the previous limit so concurrent writes
	// cannot force a deferred transaction to upgrade an obsolete WAL snapshot.
	if err := tx.QueryRowContext(ctx, `UPDATE clients SET id = id WHERE id = ? RETURNING max_concurrency`, id).Scan(&previous.MaxConcurrency); err != nil {
		return domain.Client{}, fmt.Errorf("find client %d: %w", id, err)
	}
	if err := client.ValidateUpdate(previous); err != nil {
		return domain.Client{}, err
	}
	client.UpdatedAt = s.now().UTC()
	var created string
	err = tx.QueryRowContext(ctx, `
		UPDATE clients SET name = ?, enabled = ?, priority_class = ?, vllm_priority = ?,
		max_concurrency = ?, requests_per_minute = ?, tokens_per_minute = ?, revision = revision + 1, updated_at = ?
		WHERE id = ? RETURNING revision, created_at`,
		client.Name, boolInt(client.Enabled), client.PriorityClass, client.VLLMPriority,
		client.MaxConcurrency, client.RequestsPerMinute, client.TokensPerMinute, timestamp(client.UpdatedAt), id,
	).Scan(&client.Revision, &created)
	if err != nil {
		return domain.Client{}, fmt.Errorf("update client: %w", err)
	}
	client.CreatedAt, err = parseTimestamp(created)
	if err != nil {
		return domain.Client{}, err
	}
	if err := replaceClientAccess(ctx, tx, id, params.ModelPoolIDs); err != nil {
		return domain.Client{}, err
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return domain.Client{}, err
	}
	if err := commit(tx); err != nil {
		return domain.Client{}, err
	}
	return client, nil
}

func (s *SQLite) DeleteClient(ctx context.Context, id int64) ([]int64, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var exists int
	if err := tx.QueryRowContext(ctx, `UPDATE clients SET id = id WHERE id = ? RETURNING 1`, id).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("reserve client deletion: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM api_keys WHERE client_id = ? ORDER BY id`, id)
	if err != nil {
		return nil, fmt.Errorf("list client API keys before deletion: %w", err)
	}
	var keyIDs []int64
	for rows.Next() {
		var keyID int64
		if err := rows.Scan(&keyID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan client API key before deletion: %w", err)
		}
		keyIDs = append(keyIDs, keyID)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close client API key rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate client API keys before deletion: %w", err)
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM clients WHERE id = ?`, id)
	if err != nil {
		return nil, fmt.Errorf("delete client: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return nil, sql.ErrNoRows
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return nil, err
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return keyIDs, nil
}

func (s *SQLite) ListClients(ctx context.Context) ([]domain.Client, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, revision, name, enabled, priority_class, vllm_priority, max_concurrency, requests_per_minute, tokens_per_minute, created_at, updated_at
		FROM clients ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list clients: %w", err)
	}
	defer rows.Close()
	var clients []domain.Client
	for rows.Next() {
		client, err := scanClient(rows)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate clients: %w", err)
	}
	return clients, nil
}

func (s *SQLite) SetClientModelAccess(ctx context.Context, clientID int64, modelPoolIDs []int64) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var exists int
	if err := tx.QueryRowContext(ctx, `UPDATE clients SET id = id WHERE id = ? RETURNING 1`, clientID).Scan(&exists); err != nil {
		return fmt.Errorf("find client %d: %w", clientID, err)
	}
	if err := replaceClientAccess(ctx, tx, clientID, modelPoolIDs); err != nil {
		return err
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return err
	}
	return commit(tx)
}

func replaceClientAccess(ctx context.Context, tx *sql.Tx, clientID int64, poolIDs []int64) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM client_model_access WHERE client_id = ?`, clientID); err != nil {
		return fmt.Errorf("clear client model access: %w", err)
	}
	seen := make(map[int64]struct{}, len(poolIDs))
	for _, poolID := range poolIDs {
		if _, exists := seen[poolID]; exists {
			continue
		}
		seen[poolID] = struct{}{}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO client_model_access (client_id, model_pool_id, enabled) VALUES (?, ?, 1)`,
			clientID, poolID,
		); err != nil {
			return fmt.Errorf("grant client %d access to pool %d: %w", clientID, poolID, err)
		}
	}
	return nil
}

type scanner interface {
	Scan(...any) error
}

func scanClient(row scanner) (domain.Client, error) {
	var client domain.Client
	var enabled int
	var created, updated string
	if err := row.Scan(
		&client.ID, &client.Revision, &client.Name, &enabled, &client.PriorityClass, &client.VLLMPriority,
		&client.MaxConcurrency, &client.RequestsPerMinute, &client.TokensPerMinute, &created, &updated,
	); err != nil {
		return domain.Client{}, fmt.Errorf("scan client: %w", err)
	}
	client.Enabled = enabled != 0
	var err error
	if client.CreatedAt, err = parseTimestamp(created); err != nil {
		return domain.Client{}, err
	}
	if client.UpdatedAt, err = parseTimestamp(updated); err != nil {
		return domain.Client{}, err
	}
	return client, nil
}
