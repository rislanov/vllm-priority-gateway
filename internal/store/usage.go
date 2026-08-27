package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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

const usageRequestColumns = `
	id, occurred_at_ms, request_id, parent_request_id,
	client_id, client_name, model_pool_id, model_name, backend_name,
	http_status, duration_ms, ttft_ms, retry_count, disconnected,
	usage_available, input_tokens, output_tokens, cache_read_tokens`

const (
	defaultPageLimit = 100
	maximumPageLimit = 500
)

type usageQueryPhase uint8

const (
	usageQueryAfterSummary usageQueryPhase = iota + 1
	usageQueryAfterRequestCount
	usageQueryAfterDashboardDataset
)

type usageQueryHookKey struct{}

func withUsageQueryHook(ctx context.Context, hook func(usageQueryPhase)) context.Context {
	return context.WithValue(ctx, usageQueryHookKey{}, hook)
}

func notifyUsageQueryHook(ctx context.Context, phase usageQueryPhase) {
	if hook, ok := ctx.Value(usageQueryHookKey{}).(func(usageQueryPhase)); ok && hook != nil {
		hook(phase)
	}
}

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

func (s *SQLite) Analytics(ctx context.Context, filter analytics.Filter) (analytics.Dataset, error) {
	if !filter.From.Before(filter.To) {
		return emptyAnalyticsDataset(), nil
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return analytics.Dataset{}, err
	}
	defer tx.Rollback()
	dataset, err := s.queryAnalytics(ctx, tx, filter)
	if err != nil {
		return analytics.Dataset{}, err
	}
	if err := commit(tx); err != nil {
		if ctx.Err() != nil {
			return analytics.Dataset{}, ctx.Err()
		}
		return analytics.Dataset{}, err
	}
	return dataset, nil
}

type usageQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *SQLite) queryAnalytics(
	ctx context.Context,
	queryer usageQueryer,
	filter analytics.Filter,
) (analytics.Dataset, error) {
	dataset := emptyAnalyticsDataset()
	if !filter.From.Before(filter.To) {
		return dataset, nil
	}
	predicate, arguments := usageFilterPredicate(filter)

	if err := s.queryAnalyticsSummary(ctx, queryer, predicate, arguments, &dataset.Summary); err != nil {
		return analytics.Dataset{}, err
	}
	notifyUsageQueryHook(ctx, usageQueryAfterSummary)
	series, err := s.queryAnalyticsSeries(ctx, queryer, filter, predicate, arguments)
	if err != nil {
		return analytics.Dataset{}, err
	}
	dataset.Series = series
	breakdown, err := s.queryAnalyticsBreakdown(ctx, queryer, predicate, arguments)
	if err != nil {
		return analytics.Dataset{}, err
	}
	dataset.Breakdown = breakdown
	clients, err := s.queryDimensions(ctx, queryer, "client")
	if err != nil {
		return analytics.Dataset{}, err
	}
	dataset.Clients = clients
	models, err := s.queryDimensions(ctx, queryer, "model")
	if err != nil {
		return analytics.Dataset{}, err
	}
	dataset.Models = models
	return dataset, nil
}

func (s *SQLite) queryAnalyticsSummary(
	ctx context.Context,
	queryer usageQueryer,
	predicate string,
	arguments []any,
	summary *analytics.Summary,
) error {
	var cacheRead, uncachedInput, cacheKnownInput sql.NullInt64
	err := queryer.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COALESCE(SUM(usage_available), 0),
		       COALESCE(SUM(input_tokens), 0),
		       COALESCE(SUM(output_tokens), 0),
		       SUM(cache_read_tokens),
		       SUM(CASE WHEN cache_read_tokens IS NOT NULL THEN input_tokens - cache_read_tokens END),
		       SUM(CASE WHEN cache_read_tokens IS NOT NULL THEN input_tokens END)
		FROM usage_requests
		WHERE `+predicate, arguments...).Scan(
		&summary.RequestCount,
		&summary.MeteredRequestCount,
		&summary.InputTokens,
		&summary.OutputTokens,
		&cacheRead,
		&uncachedInput,
		&cacheKnownInput,
	)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	summary.UsageCoverage = ratio(summary.MeteredRequestCount, summary.RequestCount)
	setCacheAggregates(cacheRead, uncachedInput, cacheKnownInput,
		&summary.CacheReadTokens, &summary.UncachedInputTokens, &summary.CacheHitRatio)
	return nil
}

func (s *SQLite) queryAnalyticsSeries(
	ctx context.Context,
	queryer usageQueryer,
	filter analytics.Filter,
	predicate string,
	arguments []any,
) ([]analytics.SeriesPoint, error) {
	fromMS := storedMillisecondCeiling(filter.From)
	toMS := storedMillisecondCeiling(filter.To)
	bucketWidthMS := analyticsBucketWidthMilliseconds(fromMS, toMS)
	bucketExpression := analyticsBucketExpression(bucketWidthMS)
	query := `
		SELECT ` + bucketExpression + ` AS bucket_start_ms,
		       COUNT(*),
		       COALESCE(SUM(input_tokens), 0),
		       COALESCE(SUM(output_tokens), 0),
		       SUM(cache_read_tokens),
		       SUM(CASE WHEN cache_read_tokens IS NOT NULL THEN input_tokens END)
		FROM usage_requests
		WHERE ` + predicate + `
		GROUP BY bucket_start_ms
		ORDER BY bucket_start_ms ASC`
	queryArguments := make([]any, 0, len(arguments)+2)
	queryArguments = append(queryArguments, fromMS, fromMS)
	queryArguments = append(queryArguments, arguments...)
	rows, err := queryer.QueryContext(ctx, query, queryArguments...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	defer rows.Close()
	points := make([]analytics.SeriesPoint, 0)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var bucketStartMS int64
		var cacheRead, cacheKnownInput sql.NullInt64
		var point analytics.SeriesPoint
		if err := rows.Scan(
			&bucketStartMS,
			&point.RequestCount,
			&point.InputTokens,
			&point.OutputTokens,
			&cacheRead,
			&cacheKnownInput,
		); err != nil {
			return nil, err
		}
		point.BucketStart = time.UnixMilli(bucketStartMS).UTC()
		if cacheRead.Valid {
			point.CacheReadTokens = pointer(cacheRead.Int64)
			if cacheKnownInput.Valid && cacheKnownInput.Int64 > 0 {
				point.CacheHitRatio = pointer(float64(cacheRead.Int64) / float64(cacheKnownInput.Int64))
			}
		}
		points = append(points, point)
	}
	if err := rows.Err(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return densifyAnalyticsSeries(ctx, points, fromMS, toMS, bucketWidthMS)
}

func (s *SQLite) queryAnalyticsBreakdown(
	ctx context.Context,
	queryer usageQueryer,
	predicate string,
	arguments []any,
) ([]analytics.BreakdownRow, error) {
	query := `
		SELECT grouped.client_id,
		       (SELECT snapshot.client_name
		        FROM usage_requests AS snapshot
		        WHERE snapshot.client_id = grouped.client_id
		          AND snapshot.model_pool_id = grouped.model_pool_id
		        ORDER BY snapshot.occurred_at_ms DESC, snapshot.id DESC
		        LIMIT 1),
		       grouped.model_pool_id,
		       (SELECT snapshot.model_name
		        FROM usage_requests AS snapshot
		        WHERE snapshot.client_id = grouped.client_id
		          AND snapshot.model_pool_id = grouped.model_pool_id
		        ORDER BY snapshot.occurred_at_ms DESC, snapshot.id DESC
		        LIMIT 1),
		       grouped.request_count,
		       grouped.metered_request_count,
		       grouped.input_tokens,
		       grouped.output_tokens,
		       grouped.cache_read_tokens,
		       grouped.uncached_input_tokens,
		       grouped.cache_known_input_tokens
		FROM (
			SELECT client_id,
			       model_pool_id,
			       COUNT(*) AS request_count,
			       COALESCE(SUM(usage_available), 0) AS metered_request_count,
			       COALESCE(SUM(input_tokens), 0) AS input_tokens,
			       COALESCE(SUM(output_tokens), 0) AS output_tokens,
			       SUM(cache_read_tokens) AS cache_read_tokens,
			       SUM(CASE WHEN cache_read_tokens IS NOT NULL THEN input_tokens - cache_read_tokens END) AS uncached_input_tokens,
			       SUM(CASE WHEN cache_read_tokens IS NOT NULL THEN input_tokens END) AS cache_known_input_tokens
			FROM usage_requests
			WHERE ` + predicate + `
			GROUP BY client_id, model_pool_id
		) AS grouped
		ORDER BY grouped.client_id ASC, grouped.model_pool_id ASC`
	rows, err := queryer.QueryContext(ctx, query, arguments...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	defer rows.Close()
	breakdown := make([]analytics.BreakdownRow, 0)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var cacheRead, uncachedInput, cacheKnownInput sql.NullInt64
		var row analytics.BreakdownRow
		if err := rows.Scan(
			&row.ClientID,
			&row.ClientName,
			&row.ModelPoolID,
			&row.ModelName,
			&row.RequestCount,
			&row.MeteredRequestCount,
			&row.InputTokens,
			&row.OutputTokens,
			&cacheRead,
			&uncachedInput,
			&cacheKnownInput,
		); err != nil {
			return nil, err
		}
		setCacheAggregates(cacheRead, uncachedInput, cacheKnownInput,
			&row.CacheReadTokens, &row.UncachedInputTokens, &row.CacheHitRatio)
		breakdown = append(breakdown, row)
	}
	if err := rows.Err(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return breakdown, nil
}

func (s *SQLite) queryDimensions(ctx context.Context, queryer usageQueryer, kind string) ([]analytics.Dimension, error) {
	query := clientDimensionsQuery
	if kind == "model" {
		query = modelDimensionsQuery
	}
	rows, err := queryer.QueryContext(ctx, query)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	defer rows.Close()
	dimensions := make([]analytics.Dimension, 0)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var dimension analytics.Dimension
		if err := rows.Scan(&dimension.ID, &dimension.Name); err != nil {
			return nil, err
		}
		dimensions = append(dimensions, dimension)
	}
	if err := rows.Err(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return dimensions, nil
}

const clientDimensionsQuery = `
	SELECT identities.client_id,
	       (SELECT snapshot.client_name
	        FROM usage_requests AS snapshot
	        WHERE snapshot.client_id = identities.client_id
	        ORDER BY snapshot.occurred_at_ms DESC, snapshot.id DESC
	        LIMIT 1)
	FROM (SELECT DISTINCT client_id FROM usage_requests) AS identities
	ORDER BY identities.client_id ASC`

const modelDimensionsQuery = `
	SELECT identities.model_pool_id,
	       (SELECT snapshot.model_name
	        FROM usage_requests AS snapshot
	        WHERE snapshot.model_pool_id = identities.model_pool_id
	        ORDER BY snapshot.occurred_at_ms DESC, snapshot.id DESC
	        LIMIT 1)
	FROM (SELECT DISTINCT model_pool_id FROM usage_requests) AS identities
	ORDER BY identities.model_pool_id ASC`

func (s *SQLite) UsageRequests(
	ctx context.Context,
	filter analytics.Filter,
	limit int,
	offset int,
) (analytics.RequestPage, error) {
	limit, offset = boundedPagination(limit, offset)
	if !filter.From.Before(filter.To) {
		return analytics.RequestPage{Requests: make([]analytics.RequestRecord, 0), Limit: limit, Offset: offset}, nil
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return analytics.RequestPage{}, err
	}
	defer tx.Rollback()
	page, err := s.queryUsageRequests(ctx, tx, filter, limit, offset)
	if err != nil {
		return analytics.RequestPage{}, err
	}
	if err := commit(tx); err != nil {
		if ctx.Err() != nil {
			return analytics.RequestPage{}, ctx.Err()
		}
		return analytics.RequestPage{}, err
	}
	return page, nil
}

// AnalyticsDashboard returns the aggregate dataset and request page from one
// read transaction so server-rendered cards, charts, totals, and rows reconcile.
func (s *SQLite) AnalyticsDashboard(
	ctx context.Context,
	filter analytics.Filter,
	limit int,
	offset int,
) (analytics.Dataset, analytics.RequestPage, error) {
	limit, offset = boundedPagination(limit, offset)
	if !filter.From.Before(filter.To) {
		return emptyAnalyticsDataset(), analytics.RequestPage{
			Requests: make([]analytics.RequestRecord, 0), Limit: limit, Offset: offset,
		}, nil
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return analytics.Dataset{}, analytics.RequestPage{}, err
	}
	defer tx.Rollback()
	dataset, err := s.queryAnalytics(ctx, tx, filter)
	if err != nil {
		return analytics.Dataset{}, analytics.RequestPage{}, err
	}
	notifyUsageQueryHook(ctx, usageQueryAfterDashboardDataset)
	page, err := s.queryUsageRequests(ctx, tx, filter, limit, offset)
	if err != nil {
		return analytics.Dataset{}, analytics.RequestPage{}, err
	}
	if err := commit(tx); err != nil {
		if ctx.Err() != nil {
			return analytics.Dataset{}, analytics.RequestPage{}, ctx.Err()
		}
		return analytics.Dataset{}, analytics.RequestPage{}, err
	}
	return dataset, page, nil
}

func (s *SQLite) queryUsageRequests(
	ctx context.Context,
	queryer usageQueryer,
	filter analytics.Filter,
	limit int,
	offset int,
) (analytics.RequestPage, error) {
	limit, offset = boundedPagination(limit, offset)
	page := analytics.RequestPage{
		Requests: make([]analytics.RequestRecord, 0),
		Limit:    limit,
		Offset:   offset,
	}
	if !filter.From.Before(filter.To) {
		return page, nil
	}
	predicate, arguments := usageFilterPredicate(filter)
	if err := queryer.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM usage_requests WHERE `+predicate,
		arguments...,
	).Scan(&page.Total); err != nil {
		if ctx.Err() != nil {
			return analytics.RequestPage{}, ctx.Err()
		}
		return analytics.RequestPage{}, err
	}
	notifyUsageQueryHook(ctx, usageQueryAfterRequestCount)

	queryArguments := append(append([]any{}, arguments...), limit, offset)
	rows, err := queryer.QueryContext(ctx, `
		SELECT `+usageRequestColumns+`
		FROM usage_requests
		WHERE `+predicate+`
		ORDER BY occurred_at_ms DESC, id DESC
		LIMIT ? OFFSET ?`, queryArguments...)
	if err != nil {
		if ctx.Err() != nil {
			return analytics.RequestPage{}, ctx.Err()
		}
		return analytics.RequestPage{}, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return analytics.RequestPage{}, err
		}
		record, err := scanUsageRequest(rows)
		if err != nil {
			return analytics.RequestPage{}, err
		}
		page.Requests = append(page.Requests, record)
	}
	if err := rows.Err(); err != nil {
		if ctx.Err() != nil {
			return analytics.RequestPage{}, ctx.Err()
		}
		return analytics.RequestPage{}, err
	}
	return page, nil
}

func (s *SQLite) StreamUsageRequests(
	ctx context.Context,
	filter analytics.Filter,
	yield func(analytics.RequestRecord) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filter.From.Before(filter.To) {
		return nil
	}
	predicate, arguments := usageFilterPredicate(filter)
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+usageRequestColumns+`
		FROM usage_requests
		WHERE `+predicate+`
		ORDER BY occurred_at_ms ASC, id ASC`, arguments...)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, err := scanUsageRequest(rows)
		if err != nil {
			return err
		}
		if err := yield(record); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return nil
}

type usageRowScanner interface {
	Scan(...any) error
}

func scanUsageRequest(scanner usageRowScanner) (analytics.RequestRecord, error) {
	var record analytics.RequestRecord
	var occurredAtMS int64
	var parentRequestID sql.NullString
	var ttftMS, inputTokens, outputTokens, cacheReadTokens sql.NullInt64
	var disconnected, usageAvailable int
	if err := scanner.Scan(
		&record.ID,
		&occurredAtMS,
		&record.RequestID,
		&parentRequestID,
		&record.ClientID,
		&record.ClientName,
		&record.ModelPoolID,
		&record.ModelName,
		&record.BackendName,
		&record.HTTPStatus,
		&record.DurationMS,
		&ttftMS,
		&record.RetryCount,
		&disconnected,
		&usageAvailable,
		&inputTokens,
		&outputTokens,
		&cacheReadTokens,
	); err != nil {
		return analytics.RequestRecord{}, err
	}
	record.OccurredAt = time.UnixMilli(occurredAtMS).UTC()
	record.ParentRequestID = parentRequestID.String
	record.Disconnected = disconnected != 0
	record.UsageAvailable = usageAvailable != 0
	record.TTFTMS = nullableInt64Pointer(ttftMS)
	record.InputTokens = nullableInt64Pointer(inputTokens)
	record.OutputTokens = nullableInt64Pointer(outputTokens)
	record.CacheReadTokens = nullableInt64Pointer(cacheReadTokens)
	return record, nil
}

func usageFilterPredicate(filter analytics.Filter) (string, []any) {
	clauses := []string{"occurred_at_ms >= ?", "occurred_at_ms < ?"}
	arguments := []any{storedMillisecondCeiling(filter.From), storedMillisecondCeiling(filter.To)}
	if filter.ClientID != nil {
		clauses = append(clauses, "client_id = ?")
		arguments = append(arguments, *filter.ClientID)
	}
	if filter.ModelPoolID != nil {
		clauses = append(clauses, "model_pool_id = ?")
		arguments = append(arguments, *filter.ModelPoolID)
	}
	if filter.UsageAvailable != nil {
		clauses = append(clauses, "usage_available = ?")
		arguments = append(arguments, boolInt(*filter.UsageAvailable))
	}
	return strings.Join(clauses, " AND "), arguments
}

func storedMillisecondCeiling(value time.Time) int64 {
	value = value.UTC()
	milliseconds := value.UnixMilli()
	if value.Nanosecond()%int(time.Millisecond) != 0 {
		milliseconds++
	}
	return milliseconds
}

func analyticsBucketWidthMilliseconds(fromMS int64, toMS int64) int64 {
	const (
		fiveMinutesMS = int64((5 * time.Minute) / time.Millisecond)
		hourMS        = int64(time.Hour / time.Millisecond)
		dayMS         = int64((24 * time.Hour) / time.Millisecond)
		maxPoints     = int64(366)
	)
	rangeWidthMS := toMS - fromMS
	switch {
	case rangeWidthMS <= 24*hourMS:
		return fiveMinutesMS
	case rangeWidthMS <= 7*24*hourMS:
		return hourMS
	default:
		wholeDays := ceilDividePositive(rangeWidthMS, dayMS)
		return ceilDividePositive(wholeDays, maxPoints) * dayMS
	}
}

func ceilDividePositive(dividend int64, divisor int64) int64 {
	quotient := dividend / divisor
	if dividend%divisor != 0 {
		quotient++
	}
	return quotient
}

func analyticsBucketExpression(bucketWidthMS int64) string {
	return fmt.Sprintf("((occurred_at_ms - ?) / %d) * %d + ?", bucketWidthMS, bucketWidthMS)
}

func densifyAnalyticsSeries(
	ctx context.Context,
	points []analytics.SeriesPoint,
	fromMS int64,
	toMS int64,
	bucketWidthMS int64,
) ([]analytics.SeriesPoint, error) {
	if len(points) == 0 || fromMS >= toMS {
		return points, nil
	}
	bucketCount := ceilDividePositive(toMS-fromMS, bucketWidthMS)
	dense := make([]analytics.SeriesPoint, 0, int(bucketCount))
	pointIndex := 0
	for bucketStartMS := fromMS; bucketStartMS < toMS; bucketStartMS += bucketWidthMS {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if pointIndex < len(points) && points[pointIndex].BucketStart.UnixMilli() == bucketStartMS {
			dense = append(dense, points[pointIndex])
			pointIndex++
			continue
		}
		dense = append(dense, analytics.SeriesPoint{BucketStart: time.UnixMilli(bucketStartMS).UTC()})
	}
	return dense, nil
}

func boundedPagination(limit int, offset int) (int, int) {
	if limit <= 0 {
		limit = defaultPageLimit
	}
	if limit > maximumPageLimit {
		limit = maximumPageLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func emptyAnalyticsDataset() analytics.Dataset {
	return analytics.Dataset{
		Series:    make([]analytics.SeriesPoint, 0),
		Breakdown: make([]analytics.BreakdownRow, 0),
		Clients:   make([]analytics.Dimension, 0),
		Models:    make([]analytics.Dimension, 0),
	}
}

func setCacheAggregates(
	cacheRead sql.NullInt64,
	uncachedInput sql.NullInt64,
	cacheKnownInput sql.NullInt64,
	cacheReadTarget **int64,
	uncachedInputTarget **int64,
	cacheHitRatioTarget **float64,
) {
	if !cacheRead.Valid {
		return
	}
	*cacheReadTarget = pointer(cacheRead.Int64)
	if uncachedInput.Valid {
		*uncachedInputTarget = pointer(uncachedInput.Int64)
	}
	if cacheKnownInput.Valid && cacheKnownInput.Int64 > 0 {
		*cacheHitRatioTarget = pointer(float64(cacheRead.Int64) / float64(cacheKnownInput.Int64))
	}
}

func ratio(numerator int64, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func pointer[T any](value T) *T {
	return &value
}

func nullableInt64Pointer(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	return pointer(value.Int64)
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
