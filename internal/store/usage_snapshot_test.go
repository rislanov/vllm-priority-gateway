package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
)

func TestAnalyticsUsesOneReadSnapshotAcrossLogicalPhases(t *testing.T) {
	database := openUsageSnapshotTestDB(t)
	from := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	if err := database.InsertUsageBatch(context.Background(), []analytics.RequestRecord{
		usageSnapshotRecord("initial", from.Add(time.Minute), 1, 10),
	}); err != nil {
		t.Fatal(err)
	}

	var hookErr error
	hookCalls := 0
	ctx := withUsageQueryHook(context.Background(), func(phase usageQueryPhase) {
		if phase != usageQueryAfterSummary || hookCalls != 0 {
			return
		}
		hookCalls++
		hookErr = database.InsertUsageBatch(context.Background(), []analytics.RequestRecord{
			usageSnapshotRecord("concurrent", from.Add(2*time.Minute), 2, 20),
		})
	})
	dataset, err := database.Analytics(ctx, analytics.Filter{From: from, To: from.Add(time.Hour)})
	if err != nil {
		t.Fatalf("Analytics() error = %v", err)
	}
	if hookErr != nil || hookCalls != 1 {
		t.Fatalf("inter-phase insert error/calls = %v/%d", hookErr, hookCalls)
	}

	var seriesRequests, breakdownRequests int64
	for _, point := range dataset.Series {
		seriesRequests += point.RequestCount
	}
	for _, row := range dataset.Breakdown {
		breakdownRequests += row.RequestCount
	}
	if dataset.Summary.RequestCount != 1 || seriesRequests != 1 || breakdownRequests != 1 ||
		len(dataset.Clients) != 1 || dataset.Clients[0].ID != 1 ||
		len(dataset.Models) != 1 || dataset.Models[0].ID != 10 {
		t.Fatalf("Analytics() mixed snapshots: summary=%+v series=%+v breakdown=%+v clients=%+v models=%+v",
			dataset.Summary, dataset.Series, dataset.Breakdown, dataset.Clients, dataset.Models)
	}
}

func TestUsageRequestsUsesOneReadSnapshotForCountAndPage(t *testing.T) {
	database := openUsageSnapshotTestDB(t)
	from := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	if err := database.InsertUsageBatch(context.Background(), []analytics.RequestRecord{
		usageSnapshotRecord("deleted-between-phases", from.Add(time.Minute), 1, 10),
		usageSnapshotRecord("retained", from.Add(2*time.Minute), 1, 10),
	}); err != nil {
		t.Fatal(err)
	}

	var hookErr error
	hookCalls := 0
	ctx := withUsageQueryHook(context.Background(), func(phase usageQueryPhase) {
		if phase != usageQueryAfterRequestCount || hookCalls != 0 {
			return
		}
		hookCalls++
		_, hookErr = database.DeleteUsageBefore(context.Background(), from.Add(90*time.Second))
	})
	page, err := database.UsageRequests(ctx, analytics.Filter{From: from, To: from.Add(time.Hour)}, 100, 0)
	if err != nil {
		t.Fatalf("UsageRequests() error = %v", err)
	}
	if hookErr != nil || hookCalls != 1 {
		t.Fatalf("inter-phase delete error/calls = %v/%d", hookErr, hookCalls)
	}
	if page.Total != 2 || len(page.Requests) != 2 || page.Requests[0].RequestID != "retained" ||
		page.Requests[1].RequestID != "deleted-between-phases" {
		t.Fatalf("UsageRequests() mixed count/page snapshot = %+v", page)
	}
}

func TestAnalyticsDashboardUsesOneReadSnapshotForDatasetAndPage(t *testing.T) {
	database := openUsageSnapshotTestDB(t)
	from := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	if err := database.InsertUsageBatch(context.Background(), []analytics.RequestRecord{
		usageSnapshotRecord("initial", from.Add(time.Minute), 1, 10),
	}); err != nil {
		t.Fatal(err)
	}

	var hookErr error
	hookCalls := 0
	ctx := withUsageQueryHook(context.Background(), func(phase usageQueryPhase) {
		if phase != usageQueryAfterDashboardDataset || hookCalls != 0 {
			return
		}
		hookCalls++
		hookErr = database.InsertUsageBatch(context.Background(), []analytics.RequestRecord{
			usageSnapshotRecord("concurrent", from.Add(2*time.Minute), 2, 20),
		})
	})
	dataset, page, err := database.AnalyticsDashboard(ctx, analytics.Filter{From: from, To: from.Add(time.Hour)}, 100, 0)
	if err != nil {
		t.Fatalf("AnalyticsDashboard() error = %v", err)
	}
	if hookErr != nil || hookCalls != 1 {
		t.Fatalf("inter-phase insert error/calls = %v/%d", hookErr, hookCalls)
	}
	if dataset.Summary.RequestCount != 1 || page.Total != 1 || len(page.Requests) != 1 ||
		page.Requests[0].RequestID != "initial" {
		t.Fatalf("AnalyticsDashboard() mixed dataset/page snapshots = dataset %+v page %+v", dataset, page)
	}
}

func openUsageSnapshotTestDB(t *testing.T) *SQLite {
	t.Helper()
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func usageSnapshotRecord(requestID string, occurredAt time.Time, clientID, modelPoolID int64) analytics.RequestRecord {
	return analytics.RequestRecord{
		OccurredAt: occurredAt, RequestID: requestID,
		ClientID: clientID, ClientName: "client", ModelPoolID: modelPoolID, ModelName: "model",
		HTTPStatus: 200, DurationMS: 1,
	}
}
