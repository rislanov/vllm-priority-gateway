package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
)

func TestAnalyticsRangeFiltersAggregatesAndPartialCache(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	from := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	to := from.Add(15 * time.Minute)
	input10, input20, input7 := int64(10), int64(20), int64(7)
	output2, output3, output1 := int64(2), int64(3), int64(1)
	cache4 := int64(4)
	records := []analytics.RequestRecord{
		analyticsUsageRecord("before", from.Add(-time.Millisecond), 9, "excluded", 90, "excluded-model", &input7, &output1, nil),
		analyticsUsageRecord("included-from", from, 1, "client-old", 10, "model-old", &input10, &output2, &cache4),
		analyticsUsageRecord("cache-unknown", from.Add(5*time.Minute), 1, "client-current", 10, "model-current", &input20, &output3, nil),
		analyticsUsageRecord("unmetered", from.Add(10*time.Minute), 2, "client-two", 20, "model-two", nil, nil, nil),
		analyticsUsageRecord("excluded-to", to, 2, "client-two", 20, "model-two", &input7, &output1, nil),
		analyticsUsageRecord("latest-retained-name", to.Add(time.Minute), 1, "client-renamed", 10, "model-renamed", nil, nil, nil),
	}
	if err := db.InsertUsageBatch(ctx, records); err != nil {
		t.Fatalf("InsertUsageBatch() error = %v", err)
	}

	dataset, err := db.Analytics(ctx, analytics.Filter{From: from, To: to})
	if err != nil {
		t.Fatalf("Analytics() error = %v", err)
	}
	wantSummary := analytics.Summary{
		RequestCount: 3, MeteredRequestCount: 2, UsageCoverage: 2.0 / 3.0,
		InputTokens: 30, OutputTokens: 5,
		CacheReadTokens: int64Pointer(4), UncachedInputTokens: int64Pointer(6), CacheHitRatio: float64Pointer(0.4),
	}
	if !reflect.DeepEqual(dataset.Summary, wantSummary) {
		t.Fatalf("Analytics().Summary = %+v, want %+v", dataset.Summary, wantSummary)
	}
	if len(dataset.Series) != 3 {
		t.Fatalf("Analytics().Series length = %d, want 3: %+v", len(dataset.Series), dataset.Series)
	}
	if got := dataset.Series[0]; !got.BucketStart.Equal(from) || got.RequestCount != 1 || got.InputTokens != 10 ||
		got.OutputTokens != 2 || !reflect.DeepEqual(got.CacheReadTokens, int64Pointer(4)) ||
		!reflect.DeepEqual(got.CacheHitRatio, float64Pointer(0.4)) {
		t.Fatalf("first series point = %+v", got)
	}
	if got := dataset.Series[1]; !got.BucketStart.Equal(from.Add(5*time.Minute)) || got.RequestCount != 1 ||
		got.InputTokens != 20 || got.OutputTokens != 3 || got.CacheReadTokens != nil || got.CacheHitRatio != nil {
		t.Fatalf("cache-unknown series point = %+v", got)
	}
	if got := dataset.Series[2]; !got.BucketStart.Equal(from.Add(10*time.Minute)) || got.RequestCount != 1 ||
		got.InputTokens != 0 || got.OutputTokens != 0 || got.CacheReadTokens != nil || got.CacheHitRatio != nil {
		t.Fatalf("unmetered series point = %+v", got)
	}
	if len(dataset.Breakdown) != 2 {
		t.Fatalf("Analytics().Breakdown length = %d, want 2: %+v", len(dataset.Breakdown), dataset.Breakdown)
	}
	wantBreakdown := []analytics.BreakdownRow{
		{ClientID: 1, ClientName: "client-renamed", ModelPoolID: 10, ModelName: "model-renamed",
			RequestCount: 2, MeteredRequestCount: 2, InputTokens: 30, OutputTokens: 5,
			CacheReadTokens: int64Pointer(4), UncachedInputTokens: int64Pointer(6), CacheHitRatio: float64Pointer(0.4)},
		{ClientID: 2, ClientName: "client-two", ModelPoolID: 20, ModelName: "model-two", RequestCount: 1},
	}
	if !reflect.DeepEqual(dataset.Breakdown, wantBreakdown) {
		t.Fatalf("Analytics().Breakdown = %+v, want %+v", dataset.Breakdown, wantBreakdown)
	}
	wantClients := []analytics.Dimension{{ID: 1, Name: "client-renamed"}, {ID: 2, Name: "client-two"}, {ID: 9, Name: "excluded"}}
	wantModels := []analytics.Dimension{{ID: 10, Name: "model-renamed"}, {ID: 20, Name: "model-two"}, {ID: 90, Name: "excluded-model"}}
	if !reflect.DeepEqual(dataset.Clients, wantClients) || !reflect.DeepEqual(dataset.Models, wantModels) {
		t.Fatalf("dimensions clients/models = %+v/%+v, want %+v/%+v", dataset.Clients, dataset.Models, wantClients, wantModels)
	}

	filterCases := []struct {
		name   string
		filter analytics.Filter
		want   int64
	}{
		{name: "client", filter: analytics.Filter{From: from, To: to, ClientID: int64Pointer(1)}, want: 2},
		{name: "model", filter: analytics.Filter{From: from, To: to, ModelPoolID: int64Pointer(20)}, want: 1},
		{name: "available", filter: analytics.Filter{From: from, To: to, UsageAvailable: boolPointer(true)}, want: 2},
		{name: "unavailable", filter: analytics.Filter{From: from, To: to, UsageAvailable: boolPointer(false)}, want: 1},
		{name: "combined", filter: analytics.Filter{From: from, To: to, ClientID: int64Pointer(2), ModelPoolID: int64Pointer(20), UsageAvailable: boolPointer(false)}, want: 1},
	}
	for _, test := range filterCases {
		t.Run("filter_"+test.name, func(t *testing.T) {
			got, err := db.Analytics(ctx, test.filter)
			if err != nil {
				t.Fatalf("Analytics() error = %v", err)
			}
			if got.Summary.RequestCount != test.want {
				t.Fatalf("Analytics().Summary.RequestCount = %d, want %d", got.Summary.RequestCount, test.want)
			}
		})
	}
}

func TestUsageRangeBoundsCeilFractionalMillisecondsForEveryQueryPath(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	base := time.Date(2026, time.August, 27, 12, 0, 0, 123_000_000, time.UTC)
	if err := db.InsertUsageBatch(ctx, []analytics.RequestRecord{
		analyticsUsageRecord("at-base", base, 1, "client", 10, "model", nil, nil, nil),
		analyticsUsageRecord("at-next-millisecond", base.Add(time.Millisecond), 1, "client", 10, "model", nil, nil, nil),
	}); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		filter analytics.Filter
		wantID string
	}{
		{
			name:   "fractional inclusive from excludes the containing stored millisecond",
			filter: analytics.Filter{From: base.Add(time.Nanosecond), To: base.Add(2 * time.Millisecond)},
			wantID: "at-next-millisecond",
		},
		{
			name:   "fractional exclusive to includes the containing stored millisecond",
			filter: analytics.Filter{From: base, To: base.Add(time.Nanosecond)},
			wantID: "at-base",
		},
		{
			name:   "exact millisecond remains unchanged",
			filter: analytics.Filter{From: base, To: base.Add(time.Millisecond)},
			wantID: "at-base",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dataset, err := db.Analytics(ctx, test.filter)
			if err != nil {
				t.Fatal(err)
			}
			page, err := db.UsageRequests(ctx, test.filter, 100, 0)
			if err != nil {
				t.Fatal(err)
			}
			dashboard, dashboardPage, err := db.AnalyticsDashboard(ctx, test.filter, 100, 0)
			if err != nil {
				t.Fatal(err)
			}
			var streamed []string
			if err := db.StreamUsageRequests(ctx, test.filter, func(record analytics.RequestRecord) error {
				streamed = append(streamed, record.RequestID)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if dataset.Summary.RequestCount != 1 || dashboard.Summary.RequestCount != 1 || page.Total != 1 ||
				!reflect.DeepEqual(requestIDs(page.Requests), []string{test.wantID}) || dashboardPage.Total != 1 ||
				!reflect.DeepEqual(requestIDs(dashboardPage.Requests), []string{test.wantID}) ||
				!reflect.DeepEqual(streamed, []string{test.wantID}) {
				t.Fatalf("range query mismatch: analytics=%d dashboard=%d page=%+v dashboardPage=%+v stream=%v",
					dataset.Summary.RequestCount, dashboard.Summary.RequestCount, page, dashboardPage, streamed)
			}
		})
	}
}

func TestAnalyticsChoosesAutomaticBucketWidth(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	occurredAt := time.Date(2026, time.August, 27, 12, 34, 56, 789_000_000, time.UTC)
	if err := db.InsertUsageBatch(ctx, []analytics.RequestRecord{analyticsUsageRecord(
		"bucketed", occurredAt, 1, "client", 2, "model", nil, nil, nil,
	)}); err != nil {
		t.Fatalf("InsertUsageBatch() error = %v", err)
	}
	tests := []struct {
		name        string
		rangeWidth  time.Duration
		wantWidth   time.Duration
		wantBuckets int
	}{
		{name: "twenty_four_hours", rangeWidth: 24 * time.Hour, wantWidth: 5 * time.Minute, wantBuckets: 288},
		{name: "over_twenty_four_hours", rangeWidth: 24*time.Hour + time.Millisecond, wantWidth: time.Hour, wantBuckets: 25},
		{name: "seven_days", rangeWidth: 7 * 24 * time.Hour, wantWidth: time.Hour, wantBuckets: 168},
		{name: "over_seven_days", rangeWidth: 7*24*time.Hour + time.Millisecond, wantWidth: 24 * time.Hour, wantBuckets: 8},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			from := occurredAt.Add(-time.Millisecond)
			filter := analytics.Filter{From: from, To: from.Add(test.rangeWidth)}
			got, err := db.Analytics(ctx, filter)
			if err != nil {
				t.Fatalf("Analytics() error = %v", err)
			}
			if len(got.Series) != test.wantBuckets {
				t.Fatalf("Analytics().Series length = %d, want %d", len(got.Series), test.wantBuckets)
			}
			if !got.Series[0].BucketStart.Equal(from) || got.Series[0].RequestCount != 1 {
				t.Fatalf("first bucket = %+v, want occupied bucket at %s", got.Series[0], from)
			}
			if width := got.Series[1].BucketStart.Sub(got.Series[0].BucketStart); width != test.wantWidth {
				t.Fatalf("bucket width = %s, want %s", width, test.wantWidth)
			}
		})
	}
}

func TestAnalyticsDensifiesSilentBucketsWithoutInventingAnEmptyRange(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	from := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	if err := db.InsertUsageBatch(ctx, []analytics.RequestRecord{
		analyticsUsageRecord("first", from.Add(time.Minute), 1, "client", 10, "model", nil, nil, nil),
		analyticsUsageRecord("third", from.Add(11*time.Minute), 1, "client", 10, "model", nil, nil, nil),
	}); err != nil {
		t.Fatal(err)
	}
	dataset, err := db.Analytics(ctx, analytics.Filter{From: from, To: from.Add(15 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if len(dataset.Series) != 3 {
		t.Fatalf("dense series = %+v, want three five-minute buckets", dataset.Series)
	}
	for index, wantStart := range []time.Time{from, from.Add(5 * time.Minute), from.Add(10 * time.Minute)} {
		if !dataset.Series[index].BucketStart.Equal(wantStart) {
			t.Fatalf("series bucket %d start = %s, want %s", index, dataset.Series[index].BucketStart, wantStart)
		}
	}
	middle := dataset.Series[1]
	if middle.RequestCount != 0 || middle.InputTokens != 0 || middle.OutputTokens != 0 ||
		middle.CacheReadTokens != nil || middle.CacheHitRatio != nil {
		t.Fatalf("silent bucket = %+v, want zero counts/tokens and unknown cache fields", middle)
	}
	if dataset.Series[0].RequestCount != 1 || dataset.Series[2].RequestCount != 1 {
		t.Fatalf("occupied buckets = %+v, want one request each", dataset.Series)
	}
	empty, err := db.Analytics(ctx, analytics.Filter{From: from.Add(time.Hour), To: from.Add(2 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Series) != 0 {
		t.Fatalf("truly empty range gained synthetic buckets: %+v", empty.Series)
	}
}

func TestAnalyticsDenseSeriesKeepsStandardPresetPointBounds(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	from := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	if err := db.InsertUsageBatch(ctx, []analytics.RequestRecord{
		analyticsUsageRecord("one", from.Add(time.Minute), 1, "client", 10, "model", nil, nil, nil),
	}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		width time.Duration
		want  int
	}{
		{name: "one hour", width: time.Hour, want: 12},
		{name: "twenty four hours", width: 24 * time.Hour, want: 288},
		{name: "seven days", width: 7 * 24 * time.Hour, want: 168},
		{name: "ninety days", width: 90 * 24 * time.Hour, want: 90},
	} {
		t.Run(test.name, func(t *testing.T) {
			dataset, err := db.Analytics(ctx, analytics.Filter{From: from, To: from.Add(test.width)})
			if err != nil {
				t.Fatal(err)
			}
			if len(dataset.Series) != test.want || !dataset.Series[0].BucketStart.Equal(from) {
				t.Fatalf("dense series length/first = %d/%v, want %d/%v", len(dataset.Series), dataset.Series, test.want, from)
			}
		})
	}
}

func TestAnalyticsBoundsExtremeCustomRangeSeries(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	from := time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(9999, time.December, 31, 23, 59, 59, 999_999_999, time.UTC)
	occurredAt := time.Date(9999, time.December, 31, 23, 59, 59, 998_000_000, time.UTC)
	if err := db.InsertUsageBatch(ctx, []analytics.RequestRecord{
		analyticsUsageRecord("extreme-range", occurredAt, 1, "client", 10, "model", nil, nil, nil),
	}); err != nil {
		t.Fatal(err)
	}

	dataset, err := db.Analytics(ctx, analytics.Filter{From: from, To: to})
	if err != nil {
		t.Fatal(err)
	}
	if dataset.Summary.RequestCount != 1 {
		t.Fatalf("Analytics().Summary.RequestCount = %d, want 1 for the complete RFC 3339 year range", dataset.Summary.RequestCount)
	}
	if len(dataset.Series) == 0 || len(dataset.Series) > 366 {
		t.Fatalf("Analytics().Series length = %d, want 1 through 366 points", len(dataset.Series))
	}
	if dataset.Series[len(dataset.Series)-1].RequestCount != 1 {
		t.Fatalf("last series point = %+v, want the matching request", dataset.Series[len(dataset.Series)-1])
	}
}

func TestAnalyticsUsesAdaptiveWholeDayBucketsForLongRanges(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	from := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC)
	if err := db.InsertUsageBatch(ctx, []analytics.RequestRecord{
		analyticsUsageRecord("two-year-range", time.Date(2021, time.June, 1, 0, 0, 0, 0, time.UTC), 1, "client", 10, "model", nil, nil, nil),
	}); err != nil {
		t.Fatal(err)
	}

	dataset, err := db.Analytics(ctx, analytics.Filter{From: from, To: to})
	if err != nil {
		t.Fatal(err)
	}
	if len(dataset.Series) != 366 {
		t.Fatalf("Analytics().Series length = %d, want 366 (731 days rounded up into two-day buckets)", len(dataset.Series))
	}
	if got := dataset.Series[1].BucketStart.Sub(dataset.Series[0].BucketStart); got != 48*time.Hour {
		t.Fatalf("analytics bucket width = %s, want 48h (the smallest whole-day multiple for 731 days)", got)
	}
}

func TestAnalyticsEmptyReturnsInitializedSlices(t *testing.T) {
	db := openTestDB(t)
	at := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	got, err := db.Analytics(context.Background(), analytics.Filter{From: at, To: at})
	if err != nil {
		t.Fatalf("Analytics() error = %v", err)
	}
	if got.Summary != (analytics.Summary{}) {
		t.Fatalf("Analytics().Summary = %+v, want zero", got.Summary)
	}
	if got.Series == nil || got.Breakdown == nil || got.Clients == nil || got.Models == nil {
		t.Fatalf("Analytics() returned nil slices: %+v", got)
	}
	if len(got.Series)+len(got.Breakdown)+len(got.Clients)+len(got.Models) != 0 {
		t.Fatalf("Analytics() returned non-empty slices: %+v", got)
	}
}

func TestAnalyticsUsageRequestsPaginationAndDeterministicOrdering(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	from := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	input, output, cache, ttft := int64(5), int64(2), int64(3), int64(17)
	records := []analytics.RequestRecord{
		analyticsUsageRecord("old", from.Add(time.Minute), 1, "client", 10, "model", nil, nil, nil),
		analyticsUsageRecord("tie-low-id", from.Add(2*time.Minute), 1, "client", 10, "model", &input, &output, &cache),
		analyticsUsageRecord("tie-high-id", from.Add(2*time.Minute), 1, "client", 10, "model", nil, nil, nil),
		analyticsUsageRecord("new", from.Add(3*time.Minute), 2, "other", 20, "other-model", nil, nil, nil),
	}
	records[1].ParentRequestID = "parent"
	records[1].BackendName = "gpu-a"
	records[1].TTFTMS = &ttft
	records[1].RetryCount = 2
	records[1].Disconnected = true
	if err := db.InsertUsageBatch(ctx, records); err != nil {
		t.Fatalf("InsertUsageBatch() error = %v", err)
	}
	filter := analytics.Filter{From: from, To: from.Add(time.Hour), ClientID: int64Pointer(1)}

	first, err := db.UsageRequests(ctx, filter, 2, 0)
	if err != nil {
		t.Fatalf("UsageRequests() first page error = %v", err)
	}
	if first.Total != 3 || first.Limit != 2 || first.Offset != 0 || requestIDs(first.Requests) == nil ||
		!reflect.DeepEqual(requestIDs(first.Requests), []string{"tie-high-id", "tie-low-id"}) {
		t.Fatalf("UsageRequests() first page = %+v", first)
	}
	second, err := db.UsageRequests(ctx, filter, 2, 2)
	if err != nil {
		t.Fatalf("UsageRequests() second page error = %v", err)
	}
	if second.Total != 3 || !reflect.DeepEqual(requestIDs(second.Requests), []string{"old"}) {
		t.Fatalf("UsageRequests() second page = %+v", second)
	}
	detail := first.Requests[1]
	if detail.ParentRequestID != "parent" || detail.BackendName != "gpu-a" || detail.TTFTMS == nil || *detail.TTFTMS != 17 ||
		detail.RetryCount != 2 || !detail.Disconnected || !detail.UsageAvailable || detail.InputTokens == nil ||
		*detail.InputTokens != 5 || detail.OutputTokens == nil || *detail.OutputTokens != 2 ||
		detail.CacheReadTokens == nil || *detail.CacheReadTokens != 3 {
		t.Fatalf("UsageRequests() detailed row = %+v", detail)
	}
}

func TestAnalyticsStreamUsageRequestsChronologicalAndCancellation(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	from := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	if err := db.InsertUsageBatch(ctx, []analytics.RequestRecord{
		analyticsUsageRecord("tie-low-id", from, 1, "client", 10, "model", nil, nil, nil),
		analyticsUsageRecord("tie-high-id", from, 1, "client", 10, "model", nil, nil, nil),
		analyticsUsageRecord("later", from.Add(time.Millisecond), 1, "client", 10, "model", nil, nil, nil),
	}); err != nil {
		t.Fatalf("InsertUsageBatch() error = %v", err)
	}
	filter := analytics.Filter{From: from, To: from.Add(time.Hour)}
	var ids []string
	if err := db.StreamUsageRequests(ctx, filter, func(record analytics.RequestRecord) error {
		ids = append(ids, record.RequestID)
		return nil
	}); err != nil {
		t.Fatalf("StreamUsageRequests() error = %v", err)
	}
	if !reflect.DeepEqual(ids, []string{"tie-low-id", "tie-high-id", "later"}) {
		t.Fatalf("StreamUsageRequests() order = %v", ids)
	}

	callbackErr := errors.New("stop export")
	err := db.StreamUsageRequests(ctx, filter, func(analytics.RequestRecord) error { return callbackErr })
	if !errors.Is(err, callbackErr) {
		t.Fatalf("StreamUsageRequests() callback error = %v, want %v", err, callbackErr)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	seen := 0
	err = db.StreamUsageRequests(cancelCtx, filter, func(analytics.RequestRecord) error {
		seen++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || seen != 1 {
		t.Fatalf("StreamUsageRequests() cancellation error/seen = %v/%d, want context.Canceled/1", err, seen)
	}
}

func TestAnalyticsHistoricalDimensionsUseLatestRetainedSnapshotDeterministically(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	at := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	if err := db.InsertUsageBatch(ctx, []analytics.RequestRecord{
		analyticsUsageRecord("first", at, 7, "old-client", 8, "old-model", nil, nil, nil),
		analyticsUsageRecord("second", at, 7, "latest-client", 8, "latest-model", nil, nil, nil),
	}); err != nil {
		t.Fatalf("InsertUsageBatch() error = %v", err)
	}
	got, err := db.Analytics(ctx, analytics.Filter{From: at.Add(time.Hour), To: at.Add(2 * time.Hour)})
	if err != nil {
		t.Fatalf("Analytics() error = %v", err)
	}
	if !reflect.DeepEqual(got.Clients, []analytics.Dimension{{ID: 7, Name: "latest-client"}}) ||
		!reflect.DeepEqual(got.Models, []analytics.Dimension{{ID: 8, Name: "latest-model"}}) {
		t.Fatalf("Analytics() historical dimensions = %+v/%+v", got.Clients, got.Models)
	}
}

func TestAnalyticsPropagatesSQLiteAggregateOverflow(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	from := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	max, zero := int64(math.MaxInt64), int64(0)
	if err := db.InsertUsageBatch(ctx, []analytics.RequestRecord{
		analyticsUsageRecord("one", from, 1, "client", 2, "model", &max, &zero, nil),
		analyticsUsageRecord("two", from.Add(time.Millisecond), 1, "client", 2, "model", &max, &zero, nil),
	}); err != nil {
		t.Fatalf("InsertUsageBatch() error = %v", err)
	}
	_, err := db.Analytics(ctx, analytics.Filter{From: from, To: from.Add(time.Hour)})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "integer overflow") {
		t.Fatalf("Analytics() overflow error = %v, want SQLite integer overflow", err)
	}
}

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

func TestDeleteUsageBeforeUsesExclusiveUTCMillisecondCutoff(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	cutoff := time.Date(2026, time.August, 27, 18, 0, 0, 123_000_000, time.FixedZone("test", 2*60*60))
	records := []analytics.RequestRecord{
		usageRecord("old", cutoff.UTC().Add(-time.Millisecond)),
		usageRecord("equal", cutoff.UTC()),
		usageRecord("new", cutoff.UTC().Add(time.Millisecond)),
	}
	if err := db.InsertUsageBatch(ctx, records); err != nil {
		t.Fatalf("InsertUsageBatch() error = %v", err)
	}

	deleted, err := db.DeleteUsageBefore(ctx, cutoff)
	if err != nil {
		t.Fatalf("DeleteUsageBefore() error = %v", err)
	}
	if deleted != 1 {
		t.Fatalf("DeleteUsageBefore() deleted = %d, want 1", deleted)
	}

	raw, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer raw.Close()
	rows, err := raw.QueryContext(ctx, `SELECT request_id FROM usage_requests ORDER BY occurred_at_ms`)
	if err != nil {
		t.Fatalf("query remaining usage requests: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan request ID: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate remaining usage requests: %v", err)
	}
	if !reflect.DeepEqual(ids, []string{"equal", "new"}) {
		t.Fatalf("remaining request IDs = %v", ids)
	}
}

func usageRecord(requestID string, occurredAt time.Time) analytics.RequestRecord {
	return analytics.RequestRecord{
		OccurredAt: occurredAt, RequestID: requestID,
		ClientID: 1, ClientName: "client", ModelPoolID: 2, ModelName: "model",
		HTTPStatus: 200, DurationMS: 1,
	}
}

func analyticsUsageRecord(
	requestID string,
	occurredAt time.Time,
	clientID int64,
	clientName string,
	modelPoolID int64,
	modelName string,
	inputTokens *int64,
	outputTokens *int64,
	cacheReadTokens *int64,
) analytics.RequestRecord {
	return analytics.RequestRecord{
		OccurredAt: occurredAt, RequestID: requestID,
		ClientID: clientID, ClientName: clientName, ModelPoolID: modelPoolID, ModelName: modelName,
		HTTPStatus: 200, DurationMS: 1,
		UsageAvailable: inputTokens != nil && outputTokens != nil,
		InputTokens:    inputTokens, OutputTokens: outputTokens, CacheReadTokens: cacheReadTokens,
	}
}

func int64Pointer(value int64) *int64       { return &value }
func float64Pointer(value float64) *float64 { return &value }
func boolPointer(value bool) *bool          { return &value }

func requestIDs(records []analytics.RequestRecord) []string {
	ids := make([]string, len(records))
	for index := range records {
		ids[index] = records[index].RequestID
	}
	return ids
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
