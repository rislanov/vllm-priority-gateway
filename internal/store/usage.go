package store

import (
	"context"
	"fmt"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
)

const insertUsageRequest = `
	INSERT INTO usage_requests (
		occurred_at_ms, request_id, parent_request_id,
		client_id, client_name, model_pool_id, model_name, backend_name,
		http_status, duration_ms, ttft_ms, retry_count, disconnected,
		usage_available, input_tokens, output_tokens, cache_read_tokens
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(request_id) DO NOTHING`

func (s *SQLite) InsertUsageBatch(ctx context.Context, records []analytics.RequestRecord) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	statement, err := tx.PrepareContext(ctx, insertUsageRequest)
	if err != nil {
		return fmt.Errorf("prepare usage request insert: %w", err)
	}
	defer statement.Close()

	for _, record := range records {
		if _, err := statement.ExecContext(ctx,
			record.OccurredAt.UTC().UnixMilli(),
			record.RequestID,
			nullableString(record.ParentRequestID),
			record.ClientID,
			record.ClientName,
			record.ModelPoolID,
			record.ModelName,
			record.BackendName,
			record.HTTPStatus,
			record.DurationMS,
			nullableInt64(record.TTFTMS),
			record.RetryCount,
			boolInt(record.Disconnected),
			boolInt(record.UsageAvailable),
			nullableInt64(record.InputTokens),
			nullableInt64(record.OutputTokens),
			nullableInt64(record.CacheReadTokens),
		); err != nil {
			return fmt.Errorf("insert usage request %q: %w", record.RequestID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit usage batch: %w", err)
	}
	return nil
}

func (s *SQLite) DeleteUsageBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	result, err := s.db.ExecContext(ctx,
		`DELETE FROM usage_requests WHERE occurred_at_ms < ?`, cutoff.UTC().UnixMilli(),
	)
	if err != nil {
		return 0, fmt.Errorf("delete expired usage requests: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count deleted usage requests: %w", err)
	}
	return deleted, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}
