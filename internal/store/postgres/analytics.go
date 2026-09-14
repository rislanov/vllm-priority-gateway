package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
)

const pgUsageColumns = "id,occurred_at,request_id,parent_request_id,client_id,client_name,model_pool_id,model_name,backend_name,http_status,duration_ms,ttft_ms,retry_count,disconnected,usage_available,input_tokens,output_tokens,cache_read_tokens"

func (s *Store) InsertUsageBatch(ctx context.Context, records []analytics.RequestRecord) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := s.analytics.Begin(ctx)
	if err != nil {
		return errors.New("begin PostgreSQL analytics batch")
	}
	defer tx.Rollback(ctx)
	for _, r := range records {
		_, err = tx.Exec(ctx, `INSERT INTO usage_requests(occurred_at,request_id,parent_request_id,client_id,client_name,model_pool_id,model_name,backend_name,http_status,duration_ms,ttft_ms,retry_count,disconnected,usage_available,input_tokens,output_tokens,cache_read_tokens) VALUES($1::timestamptz,$2::text,$3::text,$4::bigint,$5::text,$6::bigint,$7::text,$8::text,$9::integer,$10::bigint,$11::bigint,$12::integer,$13::boolean,$14::boolean,$15::bigint,$16::bigint,$17::bigint) ON CONFLICT(request_id) DO NOTHING`, r.OccurredAt.UTC(), r.RequestID, r.ParentRequestID, r.ClientID, r.ClientName, r.ModelPoolID, r.ModelName, r.BackendName, r.HTTPStatus, r.DurationMS, r.TTFTMS, r.RetryCount, r.Disconnected, r.UsageAvailable, r.InputTokens, r.OutputTokens, r.CacheReadTokens)
		if err != nil {
			return errors.New("insert PostgreSQL usage request")
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return errors.New("commit PostgreSQL analytics batch")
	}
	return nil
}
func (s *Store) DeleteUsageBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := s.analytics.Exec(ctx, `DELETE FROM usage_requests WHERE id IN (SELECT id FROM usage_requests WHERE occurred_at<$1::timestamptz ORDER BY occurred_at,id LIMIT 1000)`, cutoff.UTC())
	if err != nil {
		return 0, errors.New("delete PostgreSQL usage retention batch")
	}
	return tag.RowsAffected(), nil
}

type pgQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func usagePredicate(f analytics.Filter) (string, []any) {
	parts := []string{"occurred_at >= $1::timestamptz", "occurred_at < $2::timestamptz"}
	args := []any{f.From.UTC(), f.To.UTC()}
	if f.ClientID != nil {
		args = append(args, *f.ClientID)
		parts = append(parts, fmt.Sprintf("client_id=$%d::bigint", len(args)))
	}
	if f.ModelPoolID != nil {
		args = append(args, *f.ModelPoolID)
		parts = append(parts, fmt.Sprintf("model_pool_id=$%d::bigint", len(args)))
	}
	if f.UsageAvailable != nil {
		args = append(args, *f.UsageAvailable)
		parts = append(parts, fmt.Sprintf("usage_available=$%d::boolean", len(args)))
	}
	return strings.Join(parts, " AND "), args
}
func queryRecords(ctx context.Context, q pgQuerier, f analytics.Filter, order string, limit, offset int) ([]analytics.RequestRecord, error) {
	if !f.From.Before(f.To) {
		return []analytics.RequestRecord{}, nil
	}
	pred, args := usagePredicate(f)
	sqlText := "SELECT " + pgUsageColumns + " FROM usage_requests WHERE " + pred + " ORDER BY occurred_at " + order + ",id " + order
	if limit > 0 {
		args = append(args, limit, offset)
		sqlText += fmt.Sprintf(" LIMIT $%d::integer OFFSET $%d::integer", len(args)-1, len(args))
	}
	rows, err := q.Query(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, scanPGUsage)
}
func scanPGUsage(r pgx.CollectableRow) (v analytics.RequestRecord, err error) {
	err = r.Scan(&v.ID, &v.OccurredAt, &v.RequestID, &v.ParentRequestID, &v.ClientID, &v.ClientName, &v.ModelPoolID, &v.ModelName, &v.BackendName, &v.HTTPStatus, &v.DurationMS, &v.TTFTMS, &v.RetryCount, &v.Disconnected, &v.UsageAvailable, &v.InputTokens, &v.OutputTokens, &v.CacheReadTokens)
	v.OccurredAt = v.OccurredAt.UTC()
	return
}

func (s *Store) Analytics(ctx context.Context, f analytics.Filter) (analytics.Dataset, error) {
	f = canonicalPostgresAnalyticsFilter(f)
	tx, err := s.analytics.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return analytics.Dataset{}, err
	}
	defer tx.Rollback(ctx)
	out, err := analyticsDataset(ctx, tx, f)
	if err == nil {
		err = tx.Commit(ctx)
	}
	return out, err
}
func (s *Store) AnalyticsDashboard(ctx context.Context, f analytics.Filter, limit, offset int) (analytics.Dataset, analytics.RequestPage, error) {
	f = canonicalPostgresAnalyticsFilter(f)
	tx, err := s.analytics.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return analytics.Dataset{}, analytics.RequestPage{}, err
	}
	defer tx.Rollback(ctx)
	d, err := analyticsDataset(ctx, tx, f)
	if err != nil {
		return d, analytics.RequestPage{}, err
	}
	p, err := usagePage(ctx, tx, f, limit, offset)
	if err == nil {
		err = tx.Commit(ctx)
	}
	return d, p, err
}
func (s *Store) UsageRequests(ctx context.Context, f analytics.Filter, limit, offset int) (analytics.RequestPage, error) {
	return usagePage(ctx, s.analytics, canonicalPostgresAnalyticsFilter(f), limit, offset)
}
func usagePage(ctx context.Context, q pgQuerier, f analytics.Filter, limit, offset int) (analytics.RequestPage, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	out := analytics.RequestPage{Limit: limit, Offset: offset, Requests: []analytics.RequestRecord{}}
	if !f.From.Before(f.To) {
		return out, nil
	}
	pred, args := usagePredicate(f)
	if err := q.QueryRow(ctx, "SELECT count(*) FROM usage_requests WHERE "+pred, args...).Scan(&out.Total); err != nil {
		return out, err
	}
	rows, err := queryRecords(ctx, q, f, "DESC", limit, offset)
	out.Requests = rows
	return out, err
}
func (s *Store) StreamUsageRequests(ctx context.Context, f analytics.Filter, yield func(analytics.RequestRecord) error) error {
	f = canonicalPostgresAnalyticsFilter(f)
	if !f.From.Before(f.To) {
		return nil
	}
	pred, args := usagePredicate(f)
	rows, err := s.analytics.Query(ctx, "SELECT "+pgUsageColumns+" FROM usage_requests WHERE "+pred+" ORDER BY occurred_at ASC,id ASC", args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := scanPGUsage(rows)
		if err != nil {
			return err
		}
		if err := yield(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

func analyticsDataset(ctx context.Context, q pgQuerier, f analytics.Filter) (analytics.Dataset, error) {
	out := analytics.Dataset{Series: []analytics.SeriesPoint{}, Breakdown: []analytics.BreakdownRow{}, Clients: []analytics.Dimension{}, Models: []analytics.Dimension{}}
	f = canonicalPostgresAnalyticsFilter(f)
	if !f.From.Before(f.To) {
		return out, nil
	}
	predicate, arguments := usagePredicate(f)
	if err := queryAnalyticsSummary(ctx, q, predicate, arguments, &out.Summary); err != nil {
		return out, err
	}
	var err error
	out.Series, err = queryAnalyticsSeries(ctx, q, f, predicate, arguments)
	if err != nil {
		return out, err
	}
	out.Breakdown, err = queryAnalyticsBreakdown(ctx, q, predicate, arguments)
	if err != nil {
		return out, err
	}
	out.Clients, err = queryDimensions(ctx, q, "client_id", "client_name")
	if err == nil {
		out.Models, err = queryDimensions(ctx, q, "model_pool_id", "model_name")
	}
	return out, err
}

func queryAnalyticsSummary(ctx context.Context, q pgQuerier, predicate string, arguments []any, summary *analytics.Summary) error {
	var cacheRead, uncachedInput, cacheKnownInput *int64
	err := q.QueryRow(ctx, `SELECT COUNT(*),
		COALESCE(SUM(usage_available::integer),0),
		COALESCE(SUM(input_tokens),0),
		COALESCE(SUM(output_tokens),0),
		SUM(cache_read_tokens),
		SUM(CASE WHEN cache_read_tokens IS NOT NULL THEN input_tokens-cache_read_tokens END),
		SUM(CASE WHEN cache_read_tokens IS NOT NULL THEN input_tokens END)
		FROM usage_requests WHERE `+predicate, arguments...).Scan(
		&summary.RequestCount, &summary.MeteredRequestCount, &summary.InputTokens, &summary.OutputTokens,
		&cacheRead, &uncachedInput, &cacheKnownInput,
	)
	if err != nil {
		return err
	}
	summary.UsageCoverage = ratio(summary.MeteredRequestCount, summary.RequestCount)
	setCacheAggregates(cacheRead, uncachedInput, cacheKnownInput, &summary.CacheReadTokens, &summary.UncachedInputTokens, &summary.CacheHitRatio)
	return nil
}

func queryAnalyticsSeries(ctx context.Context, q pgQuerier, f analytics.Filter, predicate string, arguments []any) ([]analytics.SeriesPoint, error) {
	widthSeconds := analyticsBucketWidthSeconds(f.From, f.To)
	arguments = append(append([]any(nil), arguments...), widthSeconds)
	widthArg := len(arguments)
	bucket := fmt.Sprintf(`$1::timestamptz + floor(extract(epoch FROM (occurred_at-$1::timestamptz))/$%d::numeric)::bigint*$%d::bigint*interval '1 second'`, widthArg, widthArg)
	rows, err := q.Query(ctx, `SELECT `+bucket+` AS bucket_start,
		COUNT(*),COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),
		SUM(cache_read_tokens),SUM(CASE WHEN cache_read_tokens IS NOT NULL THEN input_tokens END)
		FROM usage_requests WHERE `+predicate+` GROUP BY bucket_start ORDER BY bucket_start ASC`, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	points := make([]analytics.SeriesPoint, 0)
	for rows.Next() {
		var point analytics.SeriesPoint
		var cacheRead, cacheKnownInput *int64
		if err := rows.Scan(&point.BucketStart, &point.RequestCount, &point.InputTokens, &point.OutputTokens, &cacheRead, &cacheKnownInput); err != nil {
			return nil, err
		}
		point.BucketStart = point.BucketStart.UTC()
		point.CacheReadTokens = cacheRead
		if cacheRead != nil && cacheKnownInput != nil && *cacheKnownInput > 0 {
			point.CacheHitRatio = ptr(float64(*cacheRead) / float64(*cacheKnownInput))
		}
		points = append(points, point)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(points) == 0 {
		return points, nil
	}
	byBucket := make(map[int64]analytics.SeriesPoint, len(points))
	for _, point := range points {
		byBucket[point.BucketStart.UnixMicro()] = point
	}
	fromMicros := f.From.UnixMicro()
	toMicros := f.To.UnixMicro()
	widthMicros := widthSeconds * int64(time.Second/time.Microsecond)
	bucketCount := ceilDividePositive(toMicros-fromMicros, widthMicros)
	dense := make([]analytics.SeriesPoint, 0, int(bucketCount))
	for atMicros := fromMicros; atMicros < toMicros; atMicros += widthMicros {
		at := time.UnixMicro(atMicros).UTC()
		if point, exists := byBucket[atMicros]; exists {
			dense = append(dense, point)
		} else {
			dense = append(dense, analytics.SeriesPoint{BucketStart: at})
		}
	}
	return dense, nil
}

func queryAnalyticsBreakdown(ctx context.Context, q pgQuerier, predicate string, arguments []any) ([]analytics.BreakdownRow, error) {
	rows, err := q.Query(ctx, `WITH grouped AS (
		SELECT client_id,model_pool_id,COUNT(*) AS request_count,
			COALESCE(SUM(usage_available::integer),0) AS metered_request_count,
			COALESCE(SUM(input_tokens),0) AS input_tokens,COALESCE(SUM(output_tokens),0) AS output_tokens,
			SUM(cache_read_tokens) AS cache_read_tokens,
			SUM(CASE WHEN cache_read_tokens IS NOT NULL THEN input_tokens-cache_read_tokens END) AS uncached_input_tokens,
			SUM(CASE WHEN cache_read_tokens IS NOT NULL THEN input_tokens END) AS cache_known_input_tokens
		FROM usage_requests WHERE `+predicate+` GROUP BY client_id,model_pool_id
	), latest AS (
		SELECT DISTINCT ON (client_id,model_pool_id) client_id,model_pool_id,client_name,model_name
		FROM usage_requests WHERE `+predicate+`
		ORDER BY client_id,model_pool_id,occurred_at DESC,id DESC
	)
	SELECT grouped.client_id,latest.client_name,grouped.model_pool_id,latest.model_name,
		grouped.request_count,grouped.metered_request_count,grouped.input_tokens,grouped.output_tokens,
		grouped.cache_read_tokens,grouped.uncached_input_tokens,grouped.cache_known_input_tokens
	FROM grouped JOIN latest USING (client_id,model_pool_id)
	ORDER BY grouped.client_id,grouped.model_pool_id`, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]analytics.BreakdownRow, 0)
	for rows.Next() {
		var row analytics.BreakdownRow
		var cacheRead, uncachedInput, cacheKnownInput *int64
		if err := rows.Scan(&row.ClientID, &row.ClientName, &row.ModelPoolID, &row.ModelName,
			&row.RequestCount, &row.MeteredRequestCount, &row.InputTokens, &row.OutputTokens,
			&cacheRead, &uncachedInput, &cacheKnownInput); err != nil {
			return nil, err
		}
		setCacheAggregates(cacheRead, uncachedInput, cacheKnownInput, &row.CacheReadTokens, &row.UncachedInputTokens, &row.CacheHitRatio)
		out = append(out, row)
	}
	return out, rows.Err()
}

func setCacheAggregates(cacheRead, uncachedInput, cacheKnownInput *int64, cacheReadTarget, uncachedInputTarget **int64, cacheHitRatioTarget **float64) {
	if cacheRead == nil {
		return
	}
	*cacheReadTarget = cacheRead
	*uncachedInputTarget = uncachedInput
	if cacheKnownInput != nil && *cacheKnownInput > 0 {
		*cacheHitRatioTarget = ptr(float64(*cacheRead) / float64(*cacheKnownInput))
	}
}

func ratio(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}
func queryDimensions(ctx context.Context, q pgQuerier, id, name string) ([]analytics.Dimension, error) {
	rows, err := q.Query(ctx, fmt.Sprintf(`SELECT DISTINCT ON (%s) %s,%s FROM usage_requests ORDER BY %s,occurred_at DESC,id DESC`, id, id, name, id))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (v analytics.Dimension, err error) { err = r.Scan(&v.ID, &v.Name); return })
}
func canonicalPostgresAnalyticsFilter(f analytics.Filter) analytics.Filter {
	f.From = postgresTimestampCeiling(f.From)
	f.To = postgresTimestampCeiling(f.To)
	return f
}

func postgresTimestampCeiling(value time.Time) time.Time {
	value = value.UTC()
	result := value.Truncate(time.Microsecond)
	if result.Before(value) {
		result = result.Add(time.Microsecond)
	}
	return result
}

func analyticsBucketWidthSeconds(from, to time.Time) int64 {
	const (
		fiveMinutesSeconds = int64((5 * time.Minute) / time.Second)
		hourSeconds        = int64(time.Hour / time.Second)
		daySeconds         = int64((24 * time.Hour) / time.Second)
		microsPerSecond    = int64(time.Second / time.Microsecond)
		maxPoints          = int64(366)
	)
	rangeWidthMicros := to.UnixMicro() - from.UnixMicro()
	switch {
	case rangeWidthMicros <= 24*hourSeconds*microsPerSecond:
		return fiveMinutesSeconds
	case rangeWidthMicros <= 7*24*hourSeconds*microsPerSecond:
		return hourSeconds
	default:
		wholeDays := ceilDividePositive(rangeWidthMicros, daySeconds*microsPerSecond)
		return ceilDividePositive(wholeDays, maxPoints) * daySeconds
	}
}

func ceilDividePositive(dividend, divisor int64) int64 {
	quotient := dividend / divisor
	if dividend%divisor != 0 {
		quotient++
	}
	return quotient
}
func ptr[T any](v T) *T { return &v }
