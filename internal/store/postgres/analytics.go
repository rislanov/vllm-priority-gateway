package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
	return usagePage(ctx, s.analytics, f, limit, offset)
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

type breakdownKey struct{ client, pool int64 }

func analyticsDataset(ctx context.Context, q pgQuerier, f analytics.Filter) (analytics.Dataset, error) {
	out := analytics.Dataset{Series: []analytics.SeriesPoint{}, Breakdown: []analytics.BreakdownRow{}, Clients: []analytics.Dimension{}, Models: []analytics.Dimension{}}
	if !f.From.Before(f.To) {
		return out, nil
	}
	records, err := queryRecords(ctx, q, f, "ASC", 0, 0)
	if err != nil {
		return out, err
	}
	width := bucketWidth(f.To.Sub(f.From))
	series := map[int64]*analytics.SeriesPoint{}
	seriesKnownInput := map[int64]int64{}
	breakdowns := map[breakdownKey]*analytics.BreakdownRow{}
	breakdownKnownInput := map[breakdownKey]int64{}
	var summaryCache, summaryKnown int64
	var summaryHasCache bool
	for _, r := range records {
		out.Summary.RequestCount++
		if r.UsageAvailable {
			out.Summary.MeteredRequestCount++
		}
		if r.InputTokens != nil {
			out.Summary.InputTokens += *r.InputTokens
		}
		if r.OutputTokens != nil {
			out.Summary.OutputTokens += *r.OutputTokens
		}
		if r.CacheReadTokens != nil && r.InputTokens != nil {
			summaryHasCache = true
			summaryCache += *r.CacheReadTokens
			summaryKnown += *r.InputTokens
		}
		bucket := f.From.UTC().Add(r.OccurredAt.Sub(f.From.UTC()) / width * width).UnixNano()
		p := series[bucket]
		if p == nil {
			p = &analytics.SeriesPoint{BucketStart: time.Unix(0, bucket).UTC()}
			series[bucket] = p
		}
		p.RequestCount++
		if r.InputTokens != nil {
			p.InputTokens += *r.InputTokens
		}
		if r.OutputTokens != nil {
			p.OutputTokens += *r.OutputTokens
		}
		if r.CacheReadTokens != nil && r.InputTokens != nil {
			if p.CacheReadTokens == nil {
				p.CacheReadTokens = ptr(int64(0))
			}
			*p.CacheReadTokens += *r.CacheReadTokens
			seriesKnownInput[bucket] += *r.InputTokens
			known := seriesKnownInput[bucket]
			if known > 0 {
				ratio := float64(*p.CacheReadTokens) / float64(known)
				p.CacheHitRatio = &ratio
			}
		}
		key := breakdownKey{r.ClientID, r.ModelPoolID}
		b := breakdowns[key]
		if b == nil {
			b = &analytics.BreakdownRow{ClientID: r.ClientID, ModelPoolID: r.ModelPoolID}
			breakdowns[key] = b
		}
		b.ClientName = r.ClientName
		b.ModelName = r.ModelName
		b.RequestCount++
		if r.UsageAvailable {
			b.MeteredRequestCount++
		}
		if r.InputTokens != nil {
			b.InputTokens += *r.InputTokens
		}
		if r.OutputTokens != nil {
			b.OutputTokens += *r.OutputTokens
		}
		if r.CacheReadTokens != nil && r.InputTokens != nil {
			if b.CacheReadTokens == nil {
				b.CacheReadTokens = ptr(int64(0))
				b.UncachedInputTokens = ptr(int64(0))
			}
			*b.CacheReadTokens += *r.CacheReadTokens
			*b.UncachedInputTokens += *r.InputTokens - *r.CacheReadTokens
			breakdownKnownInput[key] += *r.InputTokens
			if breakdownKnownInput[key] > 0 {
				ratio := float64(*b.CacheReadTokens) / float64(breakdownKnownInput[key])
				b.CacheHitRatio = &ratio
			}
		}
	}
	if out.Summary.RequestCount > 0 {
		out.Summary.UsageCoverage = float64(out.Summary.MeteredRequestCount) / float64(out.Summary.RequestCount)
	}
	if summaryHasCache {
		out.Summary.CacheReadTokens = ptr(summaryCache)
		out.Summary.UncachedInputTokens = ptr(summaryKnown - summaryCache)
		if summaryKnown > 0 {
			v := float64(summaryCache) / float64(summaryKnown)
			out.Summary.CacheHitRatio = &v
		}
	}
	keys := make([]int64, 0, len(series))
	for k := range series {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	if len(keys) > 0 {
		for at := f.From.UTC(); at.Before(f.To); at = at.Add(width) {
			key := at.UnixNano()
			if p := series[key]; p != nil {
				out.Series = append(out.Series, *p)
			} else {
				out.Series = append(out.Series, analytics.SeriesPoint{BucketStart: at})
			}
		}
	}
	bkeys := make([]breakdownKey, 0, len(breakdowns))
	for k := range breakdowns {
		bkeys = append(bkeys, k)
	}
	sort.Slice(bkeys, func(i, j int) bool {
		if bkeys[i].client == bkeys[j].client {
			return bkeys[i].pool < bkeys[j].pool
		}
		return bkeys[i].client < bkeys[j].client
	})
	for _, k := range bkeys {
		out.Breakdown = append(out.Breakdown, *breakdowns[k])
	}
	out.Clients, err = queryDimensions(ctx, q, "client_id", "client_name")
	if err == nil {
		out.Models, err = queryDimensions(ctx, q, "model_pool_id", "model_name")
	}
	return out, err
}
func queryDimensions(ctx context.Context, q pgQuerier, id, name string) ([]analytics.Dimension, error) {
	rows, err := q.Query(ctx, fmt.Sprintf(`SELECT DISTINCT ON (%s) %s,%s FROM usage_requests ORDER BY %s,occurred_at DESC,id DESC`, id, id, name, id))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (v analytics.Dimension, err error) { err = r.Scan(&v.ID, &v.Name); return })
}
func bucketWidth(span time.Duration) time.Duration {
	if span <= 24*time.Hour {
		return 5 * time.Minute
	}
	if span <= 7*24*time.Hour {
		return time.Hour
	}
	days := (span + 24*time.Hour - 1) / (24 * time.Hour)
	step := (days + 365) / 366
	return step * 24 * time.Hour
}
func ptr[T any](v T) *T { return &v }
