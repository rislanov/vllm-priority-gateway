package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func (s *SQLite) CreateAPIKey(ctx context.Context, params CreateAPIKeyParams) (domain.APIKey, error) {
	if len(params.Prefix) != 12 || !strings.HasPrefix(params.Prefix, "llmgw_") {
		return domain.APIKey{}, errors.New("API key prefix must be the first 12 characters of an llmgw_ key")
	}
	key := domain.APIKey{
		ClientID: params.ClientID, Prefix: params.Prefix, SecretHash: params.SecretHash,
		CreatedAt: s.now().UTC(), ExpiresAt: copyTime(params.ExpiresAt),
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return domain.APIKey{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO api_keys (client_id, prefix, secret_hash, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?)`,
		key.ClientID, key.Prefix, key.SecretHash[:], timestamp(key.CreatedAt), optionalTimestamp(key.ExpiresAt),
	)
	if err != nil {
		return domain.APIKey{}, fmt.Errorf("insert API key: %w", err)
	}
	key.ID, err = result.LastInsertId()
	if err != nil {
		return domain.APIKey{}, fmt.Errorf("read API key ID: %w", err)
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return domain.APIKey{}, err
	}
	if err := commit(tx); err != nil {
		return domain.APIKey{}, err
	}
	return key, nil
}

func (s *SQLite) RevokeAPIKey(ctx context.Context, id int64) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE api_keys SET revoked_at = COALESCE(revoked_at, ?) WHERE id = ?`, timestamp(s.now()), id)
	if err != nil {
		return fmt.Errorf("revoke API key: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	if err := bumpRevision(ctx, tx); err != nil {
		return err
	}
	return commit(tx)
}

func (s *SQLite) TouchKeyLastUsed(ctx context.Context, id int64, usedAt time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE api_keys SET last_used_at = ? WHERE id = ?`, timestamp(usedAt), id)
	if err != nil {
		return fmt.Errorf("update API key last-used timestamp: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *SQLite) ListAPIKeys(ctx context.Context) ([]domain.APIKey, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, client_id, prefix, secret_hash, created_at, expires_at, revoked_at, last_used_at
		FROM api_keys ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list API keys: %w", err)
	}
	defer rows.Close()
	var keys []domain.APIKey
	for rows.Next() {
		key, err := scanAPIKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate API keys: %w", err)
	}
	return keys, nil
}

func scanAPIKey(row scanner) (domain.APIKey, error) {
	var key domain.APIKey
	var digest []byte
	var created string
	var expires, revoked, lastUsed sql.NullString
	if err := row.Scan(
		&key.ID, &key.ClientID, &key.Prefix, &digest, &created,
		&expires, &revoked, &lastUsed,
	); err != nil {
		return domain.APIKey{}, fmt.Errorf("scan API key: %w", err)
	}
	if len(digest) != len(key.SecretHash) {
		return domain.APIKey{}, fmt.Errorf("API key %d has invalid digest length %d", key.ID, len(digest))
	}
	copy(key.SecretHash[:], digest)
	var err error
	if key.CreatedAt, err = parseTimestamp(created); err != nil {
		return domain.APIKey{}, err
	}
	if key.ExpiresAt, err = parseOptionalTimestamp(expires); err != nil {
		return domain.APIKey{}, err
	}
	if key.RevokedAt, err = parseOptionalTimestamp(revoked); err != nil {
		return domain.APIKey{}, err
	}
	if key.LastUsedAt, err = parseOptionalTimestamp(lastUsed); err != nil {
		return domain.APIKey{}, err
	}
	return key, nil
}

func copyTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := value.UTC()
	return &copied
}
