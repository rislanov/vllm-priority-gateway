package web_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
)

// TestWriteAnalyticsBrowserFixtures exports real SSR only when explicitly requested
// by the browser suite. It never registers a production fixture endpoint.
func TestWriteAnalyticsBrowserFixtures(t *testing.T) {
	directory := os.Getenv("LLMGW_BROWSER_ANALYTICS_FIXTURE_DIR")
	if directory == "" {
		t.Skip("browser fixture export not requested")
	}
	for _, tc := range []struct {
		name, from, to, occurred string
		empty                    bool
	}{
		{"normal", "2026-01-15T23:30:00.123456789Z", "2026-01-16T02:30:00.987654321Z", "2026-01-15T23:45:00.123456789Z", false},
		{"empty", "2026-01-15T23:30:00.123456789Z", "2026-01-16T02:30:00.987654321Z", "", true},
		{"precise", "2026-01-15T12:00:00.123456789Z", "2026-01-15T12:00:00.12345679Z", "2026-01-15T12:00:00.123456789Z", false},
		{"fold", "2026-11-01T06:30:00.123456789Z", "2026-11-01T07:30:00.987654321Z", "2026-11-01T06:30:00.123456789Z", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			from, err := time.Parse(time.RFC3339Nano, tc.from)
			if err != nil {
				t.Fatal(err)
			}
			queryStore := &dashboardQueryStoreStub{dataset: analytics.Dataset{
				Clients: []analytics.Dimension{{ID: 1, Name: "Test client"}}, Models: []analytics.Dimension{{ID: 1, Name: "Test model"}},
			}}
			if !tc.empty {
				occurred, err := time.Parse(time.RFC3339Nano, tc.occurred)
				if err != nil {
					t.Fatal(err)
				}
				cache := int64(20)
				ratio := .2
				queryStore.dataset.Series = []analytics.SeriesPoint{{BucketStart: from, RequestCount: 2, InputTokens: 100, OutputTokens: 30, CacheReadTokens: &cache, CacheHitRatio: &ratio}}
				if tc.name == "normal" {
					queryStore.dataset.Series = append(queryStore.dataset.Series, analytics.SeriesPoint{BucketStart: from.Add(time.Hour)})
				}
				queryStore.dataset.Summary = analytics.Summary{RequestCount: 2, MeteredRequestCount: 2, InputTokens: 100, OutputTokens: 30, UsageCoverage: 1}
				queryStore.page = analytics.RequestPage{Requests: []analytics.RequestRecord{{OccurredAt: occurred, RequestID: "synthetic-request", ClientName: "Test client", ModelName: "Test model", HTTPStatus: 200}}, Total: 4}
			}
			handler := newWebFixtureWithQueryStore(t, nil, queryStore)
			values := url.Values{"from": {tc.from}, "to": {tc.to}, "client_id": {"1"}, "model_pool_id": {"1"}, "usage_available": {"true"}, "limit": {"1"}, "offset": {"1"}}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/analytics?"+values.Encode(), nil))
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
			}
			body := response.Body.String()
			for _, marker := range []string{`data-range-from="` + tc.from + `"`, `data-range-to="` + tc.to + `"`, `data-utc-input=`} {
				if !strings.Contains(body, marker) {
					t.Fatalf("missing canonical marker %s", marker)
				}
			}
			if err := os.WriteFile(filepath.Join(directory, tc.name+".html"), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
		})
	}
}
