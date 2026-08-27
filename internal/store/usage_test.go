package store_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
)

func TestUsageBatchPreservesMetadataNullabilityAndIndexes(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	zero := int64(0)
	seven := int64(7)
	ttft := int64(125)
	records := []analytics.RequestRecord{
		{
			OccurredAt:      time.UnixMilli(1_725_000_000_456).UTC(),
			RequestID:       "req-metered",
			ParentRequestID: "req-parent",
			ClientID:        17,
			ClientName:      "client-before-rename",
			ModelPoolID:     23,
			ModelName:       "model-before-rename",
			BackendName:     "gpu-a",
			HTTPStatus:      200,
			DurationMS:      340,
			TTFTMS:          &ttft,
			RetryCount:      2,
			Disconnected:    true,
			UsageAvailable:  true,
			InputTokens:     &zero,
			OutputTokens:    &seven,
			CacheReadTokens: &zero,
		},
		{
			OccurredAt:     time.UnixMilli(1_725_000_001_789).UTC(),
			RequestID:      "req-unavailable",
			ClientID:       29,
			ClientName:     "historical-client",
			ModelPoolID:    31,
			ModelName:      "historical-model",
			HTTPStatus:     503,
			DurationMS:     12,
			UsageAvailable: false,
		},
	}
	if err := db.InsertUsageBatch(ctx, records); err != nil {
		t.Fatalf("InsertUsageBatch() error = %v", err)
	}
	if err := db.InsertUsageBatch(ctx, records); err != nil {
		t.Fatalf("duplicate InsertUsageBatch() error = %v", err)
	}

	raw, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer raw.Close()

	rows, err := raw.QueryContext(ctx, `
		SELECT occurred_at_ms, request_id, parent_request_id,
		       client_id, client_name, model_pool_id, model_name, backend_name,
		       http_status, duration_ms, ttft_ms, retry_count, disconnected,
		       usage_available, input_tokens, output_tokens, cache_read_tokens
		FROM usage_requests
		ORDER BY occurred_at_ms`)
	if err != nil {
		t.Fatalf("query usage_requests error = %v", err)
	}
	defer rows.Close()

	type storedRecord struct {
		occurredAtMS    int64
		requestID       string
		parentRequestID sql.NullString
		clientID        int64
		clientName      string
		modelPoolID     int64
		modelName       string
		backendName     string
		httpStatus      int
		durationMS      int64
		ttftMS          sql.NullInt64
		retryCount      int
		disconnected    int
		usageAvailable  int
		inputTokens     sql.NullInt64
		outputTokens    sql.NullInt64
		cacheReadTokens sql.NullInt64
	}
	var stored []storedRecord
	for rows.Next() {
		var record storedRecord
		if err := rows.Scan(
			&record.occurredAtMS, &record.requestID, &record.parentRequestID,
			&record.clientID, &record.clientName, &record.modelPoolID, &record.modelName, &record.backendName,
			&record.httpStatus, &record.durationMS, &record.ttftMS, &record.retryCount, &record.disconnected,
			&record.usageAvailable, &record.inputTokens, &record.outputTokens, &record.cacheReadTokens,
		); err != nil {
			t.Fatalf("scan usage request error = %v", err)
		}
		stored = append(stored, record)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate usage requests error = %v", err)
	}
	if len(stored) != 2 {
		t.Fatalf("stored request count = %d, want 2", len(stored))
	}
	metered := stored[0]
	if metered.occurredAtMS != 1_725_000_000_456 || metered.requestID != "req-metered" ||
		!metered.parentRequestID.Valid || metered.parentRequestID.String != "req-parent" ||
		metered.clientID != 17 || metered.clientName != "client-before-rename" ||
		metered.modelPoolID != 23 || metered.modelName != "model-before-rename" || metered.backendName != "gpu-a" ||
		metered.httpStatus != 200 || metered.durationMS != 340 || !metered.ttftMS.Valid || metered.ttftMS.Int64 != 125 ||
		metered.retryCount != 2 || metered.disconnected != 1 || metered.usageAvailable != 1 ||
		!metered.inputTokens.Valid || metered.inputTokens.Int64 != 0 ||
		!metered.outputTokens.Valid || metered.outputTokens.Int64 != 7 ||
		!metered.cacheReadTokens.Valid || metered.cacheReadTokens.Int64 != 0 {
		t.Fatalf("metered row = %+v", metered)
	}
	unavailable := stored[1]
	if unavailable.requestID != "req-unavailable" || unavailable.parentRequestID.Valid || unavailable.ttftMS.Valid ||
		unavailable.usageAvailable != 0 || unavailable.inputTokens.Valid || unavailable.outputTokens.Valid || unavailable.cacheReadTokens.Valid {
		t.Fatalf("unavailable row = %+v", unavailable)
	}

	assertUsageColumns(t, raw)
	assertUsageIndexes(t, raw)
}

func TestUsageBatchRollsBackOnConstraintFailure(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	validInput := int64(4)
	validOutput := int64(2)
	err := db.InsertUsageBatch(ctx, []analytics.RequestRecord{
		{
			OccurredAt: time.UnixMilli(1_725_000_002_000).UTC(), RequestID: "req-valid",
			ClientID: 1, ClientName: "client", ModelPoolID: 2, ModelName: "model",
			HTTPStatus: 200, DurationMS: 1, UsageAvailable: true,
			InputTokens: &validInput, OutputTokens: &validOutput,
		},
		{
			OccurredAt: time.UnixMilli(1_725_000_003_000).UTC(), RequestID: "req-invalid",
			ClientID: 1, ClientName: "client", ModelPoolID: 2, ModelName: "model",
			HTTPStatus: 200, DurationMS: 1, UsageAvailable: true,
		},
	})
	if err == nil {
		t.Fatal("InsertUsageBatch() accepted available usage without token counts")
	}

	raw, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer raw.Close()
	var count int
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_requests`).Scan(&count); err != nil {
		t.Fatalf("count usage requests error = %v", err)
	}
	if count != 0 {
		t.Fatalf("stored request count after failed batch = %d, want 0", count)
	}
}

func assertUsageColumns(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(usage_requests)`)
	if err != nil {
		t.Fatalf("PRAGMA table_info error = %v", err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan table_info error = %v", err)
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table_info error = %v", err)
	}
	want := []string{
		"id", "occurred_at_ms", "request_id", "parent_request_id", "client_id", "client_name",
		"model_pool_id", "model_name", "backend_name", "http_status", "duration_ms", "ttft_ms",
		"retry_count", "disconnected", "usage_available", "input_tokens", "output_tokens", "cache_read_tokens",
	}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("usage_requests columns = %v, want exact metadata-only columns %v", columns, want)
	}
}

func assertUsageIndexes(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`PRAGMA index_list(usage_requests)`)
	if err != nil {
		t.Fatalf("PRAGMA index_list error = %v", err)
	}
	defer rows.Close()
	definitions := make(map[string]bool)
	for rows.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err := rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan index_list error = %v", err)
		}
		definitions[indexDefinition(t, db, name)] = unique == 1
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index_list error = %v", err)
	}
	for _, definition := range []string{
		"occurred_at_ms DESC",
		"client_id ASC,occurred_at_ms DESC",
		"model_pool_id ASC,occurred_at_ms DESC",
	} {
		if _, ok := definitions[definition]; !ok {
			t.Errorf("missing usage index %q; got %v", definition, definitions)
		}
	}
	if unique, ok := definitions["request_id ASC"]; !ok || !unique {
		t.Errorf("missing unique request_id index; got %v", definitions)
	}
}

func indexDefinition(t *testing.T, db *sql.DB, indexName string) string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(`PRAGMA index_xinfo(%q)`, strings.ReplaceAll(indexName, "'", "''")))
	if err != nil {
		t.Fatalf("PRAGMA index_xinfo(%q) error = %v", indexName, err)
	}
	defer rows.Close()
	type column struct {
		sequence int
		text     string
	}
	var columns []column
	for rows.Next() {
		var sequence, cid, descending, key int
		var name sql.NullString
		var collation sql.NullString
		if err := rows.Scan(&sequence, &cid, &name, &descending, &collation, &key); err != nil {
			t.Fatalf("scan index_xinfo(%q) error = %v", indexName, err)
		}
		if key == 0 || !name.Valid {
			continue
		}
		direction := "ASC"
		if descending == 1 {
			direction = "DESC"
		}
		columns = append(columns, column{sequence: sequence, text: name.String + " " + direction})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index_xinfo(%q) error = %v", indexName, err)
	}
	sort.Slice(columns, func(i, j int) bool { return columns[i].sequence < columns[j].sequence })
	parts := make([]string, len(columns))
	for index := range columns {
		parts[index] = columns[index].text
	}
	return strings.Join(parts, ",")
}
