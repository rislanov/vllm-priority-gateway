package httpapi_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

func TestAnalyticsDefaultsRangeAndPaginationAndReturnsJSON(t *testing.T) {
	now := time.Date(2026, 8, 27, 15, 45, 30, 123_456_789, time.UTC)
	queryStore := &analyticsQueryStoreStub{
		dataset: analytics.Dataset{
			Summary:   analytics.Summary{RequestCount: 3, MeteredRequestCount: 2, UsageCoverage: 2.0 / 3.0},
			Series:    []analytics.SeriesPoint{},
			Breakdown: []analytics.BreakdownRow{},
			Clients:   []analytics.Dimension{{ID: 7, Name: "payments"}},
			Models:    []analytics.Dimension{{ID: 9, Name: "qwen"}},
		},
		page: analytics.RequestPage{Requests: []analytics.RequestRecord{}, Total: 3, Limit: 100, Offset: 0},
	}
	handler := newAnalyticsHandler(t, queryStore, func() time.Time { return now })

	response := analyticsRequest(t, handler, "/admin/api/analytics")
	if response.Code != http.StatusOK {
		t.Fatalf("analytics status = %d body=%s", response.Code, response.Body.String())
	}
	var dataset map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &dataset); err != nil {
		t.Fatal(err)
	}
	if summary, ok := dataset["summary"].(map[string]any); !ok || summary["requestCount"] != float64(3) {
		t.Fatalf("analytics JSON = %#v", dataset)
	}
	if _, ok := dataset["series"]; !ok {
		t.Fatalf("analytics JSON missing series: %#v", dataset)
	}

	response = analyticsRequest(t, handler, "/admin/api/analytics/requests")
	if response.Code != http.StatusOK {
		t.Fatalf("requests status = %d body=%s", response.Code, response.Body.String())
	}
	var page map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page["total"] != float64(3) || page["limit"] != float64(100) || page["offset"] != float64(0) {
		t.Fatalf("request page JSON = %#v", page)
	}

	wantFilter := analytics.Filter{From: now.Add(-24 * time.Hour), To: now}
	if got := queryStore.analyticsFilters(); !reflect.DeepEqual(got, []analytics.Filter{wantFilter}) {
		t.Fatalf("Analytics filters = %#v, want %#v", got, []analytics.Filter{wantFilter})
	}
	requestCalls := queryStore.requestCallsSnapshot()
	if len(requestCalls) != 1 || !reflect.DeepEqual(requestCalls[0].filter, wantFilter) || requestCalls[0].limit != 100 || requestCalls[0].offset != 0 {
		t.Fatalf("UsageRequests calls = %#v", requestCalls)
	}
}

func TestAnalyticsParsesActiveFiltersForJSONAndCSV(t *testing.T) {
	queryStore := &analyticsQueryStoreStub{
		dataset: analytics.Dataset{Series: []analytics.SeriesPoint{}, Breakdown: []analytics.BreakdownRow{}, Clients: []analytics.Dimension{}, Models: []analytics.Dimension{}},
		page:    analytics.RequestPage{Requests: []analytics.RequestRecord{}, Limit: 500, Offset: 17},
	}
	handler := newAnalyticsHandler(t, queryStore, time.Now)
	from := time.Date(2026, 8, 20, 9, 15, 0, 0, time.UTC)
	to := time.Date(2026, 8, 27, 9, 15, 0, 0, time.UTC)

	for _, usageAvailable := range []string{"true", "false"} {
		t.Run(usageAvailable, func(t *testing.T) {
			values := url.Values{
				"from":            {from.Format(time.RFC3339)},
				"to":              {to.Format(time.RFC3339)},
				"client_id":       {"23"},
				"model_pool_id":   {"41"},
				"usage_available": {usageAvailable},
				"limit":           {"500"},
				"offset":          {"17"},
			}
			base := "/admin/api/analytics?" + values.Encode()
			for _, path := range []string{base, "/admin/api/analytics/requests?" + values.Encode(), "/admin/api/analytics/export.csv?" + values.Encode()} {
				response := analyticsRequest(t, handler, path)
				if response.Code != http.StatusOK {
					t.Fatalf("GET %s = %d body=%s", path, response.Code, response.Body.String())
				}
			}
		})
	}

	wantFilters := []analytics.Filter{
		{From: from, To: to, ClientID: int64Pointer(23), ModelPoolID: int64Pointer(41), UsageAvailable: boolPointer(true)},
		{From: from, To: to, ClientID: int64Pointer(23), ModelPoolID: int64Pointer(41), UsageAvailable: boolPointer(true)},
		{From: from, To: to, ClientID: int64Pointer(23), ModelPoolID: int64Pointer(41), UsageAvailable: boolPointer(true)},
		{From: from, To: to, ClientID: int64Pointer(23), ModelPoolID: int64Pointer(41), UsageAvailable: boolPointer(false)},
		{From: from, To: to, ClientID: int64Pointer(23), ModelPoolID: int64Pointer(41), UsageAvailable: boolPointer(false)},
		{From: from, To: to, ClientID: int64Pointer(23), ModelPoolID: int64Pointer(41), UsageAvailable: boolPointer(false)},
	}
	if got := queryStore.allFilters(); !reflect.DeepEqual(got, wantFilters) {
		t.Fatalf("filters = %#v, want %#v", got, wantFilters)
	}
	for _, call := range queryStore.requestCallsSnapshot() {
		if call.limit != 500 || call.offset != 17 {
			t.Fatalf("pagination = %d/%d, want 500/17", call.limit, call.offset)
		}
	}
}

func TestAnalyticsRejectsInvalidQueries(t *testing.T) {
	handler := newAnalyticsHandler(t, &analyticsQueryStoreStub{}, time.Now)
	validFrom := "2026-08-26T12:00:00Z"
	validTo := "2026-08-27T12:00:00Z"
	tests := map[string]string{
		"only from":             "from=" + url.QueryEscape(validFrom),
		"only to":               "to=" + url.QueryEscape(validTo),
		"empty range":           "from=&to=",
		"invalid from":          "from=yesterday&to=" + url.QueryEscape(validTo),
		"equal range":           "from=" + url.QueryEscape(validFrom) + "&to=" + url.QueryEscape(validFrom),
		"reversed range":        "from=" + url.QueryEscape(validTo) + "&to=" + url.QueryEscape(validFrom),
		"zero client":           "client_id=0",
		"negative client":       "client_id=-1",
		"zero model":            "model_pool_id=0",
		"negative model":        "model_pool_id=-2",
		"invalid boolean":       "usage_available=yes",
		"empty boolean":         "usage_available=",
		"numeric true boolean":  "usage_available=1",
		"numeric false boolean": "usage_available=0",
		"short true boolean":    "usage_available=t",
		"short false boolean":   "usage_available=F",
		"uppercase boolean":     "usage_available=TRUE",
		"mixed-case boolean":    "usage_available=False",
		"zero limit":            "limit=0",
		"excessive limit":       "limit=501",
		"negative offset":       "offset=-1",
		"duplicate parameter":   "client_id=1&client_id=2",
		"unsupported parameter": "client=1",
	}
	for name, query := range tests {
		t.Run(name, func(t *testing.T) {
			response := analyticsRequest(t, handler, "/admin/api/analytics/requests?"+query)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 body=%s", response.Code, response.Body.String())
			}
			var envelope struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("decode error envelope: %v body=%s", err, response.Body.String())
			}
			if envelope.Error.Code != "invalid_analytics_query" || envelope.Error.Message == "" {
				t.Fatalf("error envelope = %+v", envelope)
			}
		})
	}
}

func TestAnalyticsStoreErrorsUseControlledJSONEnvelope(t *testing.T) {
	queryStore := &analyticsQueryStoreStub{queryErr: errors.New("database unavailable")}
	handler := newAnalyticsHandler(t, queryStore, time.Now)
	for _, path := range []string{"/admin/api/analytics", "/admin/api/analytics/requests", "/admin/api/analytics/export.csv"} {
		t.Run(path, func(t *testing.T) {
			response := analyticsRequest(t, handler, path)
			if response.Code != http.StatusInternalServerError || response.Header().Get("Content-Type") != "application/json; charset=utf-8" {
				t.Fatalf("response = %d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
			}
			var envelope map[string]map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("decode response: %v body=%s", err, response.Body.String())
			}
			if envelope["error"]["code"] != "analytics_query_failed" || strings.Contains(response.Body.String(), "database unavailable") {
				t.Fatalf("error envelope = %#v", envelope)
			}
		})
	}
}

func TestAnalyticsCSVStoreErrorAfterRowsReturnsControlledErrorBeforeCommit(t *testing.T) {
	queryStore := &analyticsQueryStoreStub{}
	queryStore.stream = func(_ context.Context, _ analytics.Filter, yield func(analytics.RequestRecord) error) error {
		if err := yield(analytics.RequestRecord{
			ID: 1, OccurredAt: time.Unix(1, 0).UTC(), RequestID: "committed-row",
		}); err != nil {
			return err
		}
		return errors.New("late database failure")
	}
	handler := newAnalyticsHandler(t, queryStore, time.Now)
	response := analyticsRequest(t, handler, "/admin/api/analytics/export.csv")
	if response.Code != http.StatusInternalServerError || response.Header().Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("response = %d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
	}
	if strings.Contains(response.Body.String(), "committed-row") || !strings.Contains(response.Body.String(), "analytics_query_failed") {
		t.Fatalf("pre-commit store error response = %q", response.Body.String())
	}
}

func TestAnalyticsCSVWriterFailureAfterCommitStopsWithoutJSON(t *testing.T) {
	tests := []struct {
		name    string
		records []analytics.RequestRecord
		failAt  int
	}{
		{name: "empty export header flush", failAt: 1},
		{
			name:    "large export second delivery write",
			records: manyAnalyticsCSVRecords(2_000),
			failAt:  2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := newAnalyticsHandler(t, &analyticsQueryStoreStub{streamRecords: test.records}, time.Now)
			request := httptest.NewRequest(http.MethodGet, "/admin/api/analytics/export.csv", nil)
			request.SetBasicAuth(adminUser, adminPassword)
			writer := &failingAnalyticsResponseWriter{header: make(http.Header), failAt: test.failAt}

			handler.ServeHTTP(writer, request)

			if writer.status != http.StatusOK || writer.writeCalls != test.failAt {
				t.Fatalf("writer status/calls = %d/%d, want 200/%d", writer.status, writer.writeCalls, test.failAt)
			}
			if strings.Contains(writer.body.String(), "analytics_query_failed") || strings.Contains(writer.body.String(), `{"error"`) {
				t.Fatalf("writer failure corrupted CSV: %q", writer.body.String())
			}
		})
	}
}

func TestAnalyticsCSVCompletesStoreStreamBeforeBlockedClientDelivery(t *testing.T) {
	var cursor sync.RWMutex
	streamDone := make(chan struct{})
	queryStore := &analyticsQueryStoreStub{}
	queryStore.stream = func(_ context.Context, _ analytics.Filter, yield func(analytics.RequestRecord) error) error {
		cursor.RLock()
		defer cursor.RUnlock()
		defer close(streamDone)
		return yield(analytics.RequestRecord{ID: 1, OccurredAt: time.Unix(1, 0).UTC(), RequestID: "metadata-row"})
	}
	handler := newAnalyticsHandler(t, queryStore, time.Now)
	writer := newBlockingAnalyticsResponseWriter()
	request := httptest.NewRequest(http.MethodGet, "/admin/api/analytics/export.csv", nil)
	request.SetBasicAuth(adminUser, adminPassword)
	handlerDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, request)
		close(handlerDone)
	}()

	awaitAnalyticsSignal(t, writer.writeStarted, "client delivery start")
	storeWriterProgress := make(chan struct{})
	go func() {
		cursor.Lock()
		close(storeWriterProgress)
		cursor.Unlock()
	}()
	streamFinished := analyticsSignalReady(streamDone)
	writerProgressed := analyticsSignalReady(storeWriterProgress)
	if !streamFinished || !writerProgressed {
		close(writer.release)
		awaitAnalyticsSignal(t, handlerDone, "blocked CSV handler cleanup")
		awaitAnalyticsSignal(t, storeWriterProgress, "store writer cleanup")
		t.Fatalf("client delivery began with store stream/writer progress = %t/%t, want true/true", streamFinished, writerProgressed)
	}
	close(writer.release)
	awaitAnalyticsSignal(t, handlerDone, "CSV handler completion")
	if writer.status != http.StatusOK {
		t.Fatalf("CSV response status = %d", writer.status)
	}
}

func TestAnalyticsCSVLimitsConcurrentSpoolsWithoutBlocking(t *testing.T) {
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	queryStore := &analyticsQueryStoreStub{}
	queryStore.stream = func(ctx context.Context, _ analytics.Filter, _ func(analytics.RequestRecord) error) error {
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	handler := newAnalyticsHandler(t, queryStore, time.Now)
	serve := func() <-chan *httptest.ResponseRecorder {
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() { done <- analyticsRequest(t, handler, "/admin/api/analytics/export.csv") }()
		return done
	}
	first, second := serve(), serve()
	awaitAnalyticsSignal(t, started, "first export spool")
	awaitAnalyticsSignal(t, started, "second export spool")
	third := serve()
	select {
	case response := <-third:
		if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") == "" ||
			!strings.Contains(response.Body.String(), "analytics_export_busy") {
			releaseOnce.Do(func() { close(release) })
			t.Fatalf("saturated export response = %d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
		}
	case <-time.After(200 * time.Millisecond):
		releaseOnce.Do(func() { close(release) })
		<-first
		<-second
		<-third
		t.Fatal("third export blocked instead of returning a controlled saturation response")
	}
	releaseOnce.Do(func() { close(release) })
	if response := <-first; response.Code != http.StatusOK {
		t.Fatalf("first export status = %d body=%s", response.Code, response.Body.String())
	}
	if response := <-second; response.Code != http.StatusOK {
		t.Fatalf("second export status = %d body=%s", response.Code, response.Body.String())
	}
}

func TestAnalyticsCSVTemporaryFileIsSecureAndRemovedOnEveryPath(t *testing.T) {
	tests := []struct {
		name       string
		storeError bool
		failWrite  bool
	}{
		{name: "success"},
		{name: "store error", storeError: true},
		{name: "client write error", failWrite: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempDirectory := t.TempDir()
			t.Setenv("TMPDIR", tempDirectory)
			observedSecureFile := false
			queryStore := &analyticsQueryStoreStub{}
			queryStore.stream = func(_ context.Context, _ analytics.Filter, yield func(analytics.RequestRecord) error) error {
				matches, err := filepath.Glob(filepath.Join(tempDirectory, "llmgw-usage-analytics-*.csv"))
				if err != nil || len(matches) != 1 {
					return errors.New("secure analytics spool was not present during store scan")
				}
				info, err := os.Stat(matches[0])
				if err != nil || info.Mode().Perm() != 0o600 {
					return errors.New("analytics spool permissions were not 0600")
				}
				observedSecureFile = true
				if err := yield(analytics.RequestRecord{ID: 1, OccurredAt: time.Unix(1, 0).UTC(), RequestID: "metadata-only"}); err != nil {
					return err
				}
				if test.storeError {
					return errors.New("forced store error")
				}
				return nil
			}
			handler := newAnalyticsHandler(t, queryStore, time.Now)
			request := httptest.NewRequest(http.MethodGet, "/admin/api/analytics/export.csv", nil)
			request.SetBasicAuth(adminUser, adminPassword)
			if test.failWrite {
				handler.ServeHTTP(&failingAnalyticsResponseWriter{header: make(http.Header), failAt: 1}, request)
			} else {
				handler.ServeHTTP(httptest.NewRecorder(), request)
			}
			if !observedSecureFile {
				t.Fatal("store scan did not observe a secure local spool")
			}
			entries, err := os.ReadDir(tempDirectory)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("temporary analytics spools remain after handler return: %v", entries)
			}
		})
	}
}

func TestAnalyticsCSVContainsOnlyChronologicalLedgerMetadataAndNeutralizesFormulas(t *testing.T) {
	ttft := int64(12)
	input := int64(40)
	output := int64(5)
	cacheRead := int64(10)
	records := []analytics.RequestRecord{
		{
			ID: 2, OccurredAt: time.Date(2026, 8, 26, 10, 0, 0, 123_456_789, time.FixedZone("plus-two", 2*60*60)),
			RequestID: "=SUM(A1:A2)", ParentRequestID: "+parent", ClientID: 7, ClientName: "-payments",
			ModelPoolID: 9, ModelName: "@qwen", BackendName: "=gpu-a", HTTPStatus: 200, DurationMS: 25,
			TTFTMS: &ttft, RetryCount: 1, Disconnected: false, UsageAvailable: true,
			InputTokens: &input, OutputTokens: &output, CacheReadTokens: &cacheRead,
		},
		{
			ID: 3, OccurredAt: time.Date(2026, 8, 26, 10, 0, 1, 0, time.UTC), RequestID: "req-2",
			ClientID: 7, ClientName: "payments, europe", ModelPoolID: 9, ModelName: "qwen\nchat",
			BackendName: "gpu-b", HTTPStatus: 499, DurationMS: 31, RetryCount: 0, Disconnected: true,
			UsageAvailable: false,
		},
	}
	queryStore := &analyticsQueryStoreStub{streamRecords: records}
	handler := newAnalyticsHandler(t, queryStore, time.Now)
	response := analyticsRequest(t, handler, "/admin/api/analytics/export.csv")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "text/csv; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := response.Header().Get("Content-Disposition"); got != `attachment; filename="usage-analytics.csv"` {
		t.Fatalf("Content-Disposition = %q", got)
	}
	if strings.Contains(response.Body.String(), "\n") && !strings.Contains(response.Body.String(), "\r\n") {
		t.Fatalf("CSV does not use RFC 4180 CRLF records: %q", response.Body.String())
	}
	rows, err := csv.NewReader(strings.NewReader(response.Body.String())).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v body=%q", err, response.Body.String())
	}
	wantHeader := []string{
		"id", "occurred_at", "request_id", "parent_request_id", "client_id", "client_name", "model_pool_id", "model_name",
		"backend_name", "http_status", "duration_ms", "ttft_ms", "retry_count", "disconnected", "usage_available",
		"input_tokens", "output_tokens", "cache_read_tokens",
	}
	if len(rows) != 3 || !reflect.DeepEqual(rows[0], wantHeader) {
		t.Fatalf("CSV rows = %#v", rows)
	}
	wantFirst := []string{
		"2", "2026-08-26T08:00:00.123456789Z", "'=SUM(A1:A2)", "'+parent", "7", "'-payments", "9", "'@qwen",
		"'=gpu-a", "200", "25", "12", "1", "false", "true", "40", "5", "10",
	}
	if !reflect.DeepEqual(rows[1], wantFirst) {
		t.Fatalf("first CSV row = %#v, want %#v", rows[1], wantFirst)
	}
	if rows[2][0] != "3" || rows[2][2] != "req-2" || rows[2][3] != "" || rows[2][5] != "payments, europe" || rows[2][7] != "qwen\nchat" {
		t.Fatalf("second CSV row = %#v", rows[2])
	}
	for _, index := range []int{11, 15, 16, 17} {
		if rows[2][index] != "" {
			t.Fatalf("nullable column %d = %q, want empty", index, rows[2][index])
		}
	}
	for _, forbidden := range []string{"prompt", "request_body", "response_body", "authorization", "api_key"} {
		if strings.Contains(strings.Join(rows[0], ","), forbidden) {
			t.Fatalf("CSV header contains forbidden field %q: %v", forbidden, rows[0])
		}
	}
}

func TestAnalyticsCSVCancellationDuringSpoolingCommitsNoCSV(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var callbackErr error
	queryStore := &analyticsQueryStoreStub{}
	queryStore.stream = func(_ context.Context, _ analytics.Filter, yield func(analytics.RequestRecord) error) error {
		if err := yield(analytics.RequestRecord{ID: 1, OccurredAt: time.Unix(1, 0).UTC(), RequestID: "first"}); err != nil {
			return err
		}
		cancel()
		callbackErr = yield(analytics.RequestRecord{ID: 2, OccurredAt: time.Unix(2, 0).UTC(), RequestID: "second"})
		return callbackErr
	}
	handler := newAnalyticsHandler(t, queryStore, time.Now)
	request := httptest.NewRequest(http.MethodGet, "/admin/api/analytics/export.csv", nil).WithContext(ctx)
	request.SetBasicAuth(adminUser, adminPassword)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if !errors.Is(callbackErr, context.Canceled) {
		t.Fatalf("stream callback error = %v, want context cancellation", callbackErr)
	}
	if response.Header().Get("Content-Type") == "text/csv; charset=utf-8" || response.Body.Len() != 0 {
		t.Fatalf("cancelled pre-commit export wrote response headers/body: headers=%v body=%q", response.Header(), response.Body.String())
	}
}

func TestAnalyticsCSVCancellationDuringDeliveryStopsCopying(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	queryStore := &analyticsQueryStoreStub{streamRecords: manyAnalyticsCSVRecords(2_000)}
	handler := newAnalyticsHandler(t, queryStore, time.Now)
	request := httptest.NewRequest(http.MethodGet, "/admin/api/analytics/export.csv", nil).WithContext(ctx)
	request.SetBasicAuth(adminUser, adminPassword)
	writer := &cancelingAnalyticsResponseWriter{header: make(http.Header), cancel: cancel}

	handler.ServeHTTP(writer, request)

	if writer.status != http.StatusOK || writer.writeCalls != 1 {
		t.Fatalf("cancelled delivery status/write calls = %d/%d, want 200/1", writer.status, writer.writeCalls)
	}
}

func TestAdminAnalyticsSecurity(t *testing.T) {
	handler := newAnalyticsHandler(t, &analyticsQueryStoreStub{}, time.Now)
	for _, path := range []string{"/admin/api/analytics", "/admin/api/analytics/requests", "/admin/api/analytics/export.csv"} {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized || response.Header().Get("WWW-Authenticate") == "" {
				t.Fatalf("unauthenticated response = %d headers=%v", response.Code, response.Header())
			}
			if got := response.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("unauthenticated Cache-Control = %q", got)
			}

			response = analyticsRequest(t, handler, path)
			if response.Code != http.StatusOK {
				t.Fatalf("authenticated response = %d body=%s", response.Code, response.Body.String())
			}
			if got := response.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("authenticated Cache-Control = %q", got)
			}
		})
	}
}

func TestAnalyticsDependencyIsRequired(t *testing.T) {
	database, registryValue := newAnalyticsDatabase(t)
	_, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Registry: registryValue, Runtime: &adminRuntimeStub{values: map[int64]domain.BackendRuntime{}},
		HMACSecret: []byte(strings.Repeat("h", 32)),
	})
	if err == nil || !strings.Contains(err.Error(), "analytics") {
		t.Fatalf("NewAdminService() error = %v, want missing analytics dependency", err)
	}
}

func newAnalyticsHandler(t *testing.T, queryStore analytics.QueryStore, now func() time.Time) http.Handler {
	t.Helper()
	database, registryValue := newAnalyticsDatabase(t)
	service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Analytics: queryStore, Registry: registryValue,
		Runtime:    &adminRuntimeStub{values: make(map[int64]domain.BackendRuntime)},
		HMACSecret: []byte(strings.Repeat("h", 32)), Random: bytes.NewReader(bytes.Repeat([]byte{9}, 4096)), Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	security, err := httpapi.NewAdminSecurity(httpapi.AdminSecurityConfig{
		Username: adminUser, Password: adminPassword, Random: bytes.NewReader(bytes.Repeat([]byte{8}, 4096)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return security.Wrap(httpapi.NewAdminAPI(service))
}

func newAnalyticsDatabase(t *testing.T) (*store.SQLite, *registry.Registry) {
	t.Helper()
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	registryValue := registry.New(database)
	if err := registryValue.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	return database, registryValue
}

func analyticsRequest(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.SetBasicAuth(adminUser, adminPassword)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

type analyticsRequestCall struct {
	filter analytics.Filter
	limit  int
	offset int
}

type analyticsQueryStoreStub struct {
	mu            sync.Mutex
	dataset       analytics.Dataset
	page          analytics.RequestPage
	queryErr      error
	streamRecords []analytics.RequestRecord
	stream        func(context.Context, analytics.Filter, func(analytics.RequestRecord) error) error
	analyticsSeen []analytics.Filter
	requestsSeen  []analyticsRequestCall
	streamsSeen   []analytics.Filter
}

func (s *analyticsQueryStoreStub) Analytics(_ context.Context, filter analytics.Filter) (analytics.Dataset, error) {
	s.mu.Lock()
	s.analyticsSeen = append(s.analyticsSeen, filter)
	s.mu.Unlock()
	if s.queryErr != nil {
		return analytics.Dataset{}, s.queryErr
	}
	return s.dataset, nil
}

func (s *analyticsQueryStoreStub) UsageRequests(_ context.Context, filter analytics.Filter, limit, offset int) (analytics.RequestPage, error) {
	s.mu.Lock()
	s.requestsSeen = append(s.requestsSeen, analyticsRequestCall{filter: filter, limit: limit, offset: offset})
	s.mu.Unlock()
	if s.queryErr != nil {
		return analytics.RequestPage{}, s.queryErr
	}
	return s.page, nil
}

func (s *analyticsQueryStoreStub) AnalyticsDashboard(_ context.Context, filter analytics.Filter, limit, offset int) (analytics.Dataset, analytics.RequestPage, error) {
	s.mu.Lock()
	s.analyticsSeen = append(s.analyticsSeen, filter)
	s.requestsSeen = append(s.requestsSeen, analyticsRequestCall{filter: filter, limit: limit, offset: offset})
	s.mu.Unlock()
	if s.queryErr != nil {
		return analytics.Dataset{}, analytics.RequestPage{}, s.queryErr
	}
	return s.dataset, s.page, nil
}

func (s *analyticsQueryStoreStub) StreamUsageRequests(ctx context.Context, filter analytics.Filter, yield func(analytics.RequestRecord) error) error {
	s.mu.Lock()
	s.streamsSeen = append(s.streamsSeen, filter)
	s.mu.Unlock()
	if s.queryErr != nil {
		return s.queryErr
	}
	if s.stream != nil {
		return s.stream(ctx, filter, yield)
	}
	for _, record := range s.streamRecords {
		if err := yield(record); err != nil {
			return err
		}
	}
	return nil
}

func (s *analyticsQueryStoreStub) analyticsFilters() []analytics.Filter {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]analytics.Filter(nil), s.analyticsSeen...)
}

func (s *analyticsQueryStoreStub) requestCallsSnapshot() []analyticsRequestCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]analyticsRequestCall(nil), s.requestsSeen...)
}

func (s *analyticsQueryStoreStub) allFilters() []analytics.Filter {
	s.mu.Lock()
	defer s.mu.Unlock()
	filters := make([]analytics.Filter, 0, len(s.analyticsSeen)+len(s.requestsSeen)+len(s.streamsSeen))
	requestIndex, streamIndex := 0, 0
	for _, filter := range s.analyticsSeen {
		filters = append(filters, filter)
		if requestIndex < len(s.requestsSeen) {
			filters = append(filters, s.requestsSeen[requestIndex].filter)
			requestIndex++
		}
		if streamIndex < len(s.streamsSeen) {
			filters = append(filters, s.streamsSeen[streamIndex])
			streamIndex++
		}
	}
	return filters
}

func int64Pointer(value int64) *int64 { return &value }
func boolPointer(value bool) *bool    { return &value }

func manyAnalyticsCSVRecords(count int) []analytics.RequestRecord {
	records := make([]analytics.RequestRecord, 0, count)
	for index := 0; index < count; index++ {
		records = append(records, analytics.RequestRecord{
			ID: int64(index + 1), OccurredAt: time.Unix(int64(index+1), 0).UTC(),
			RequestID: strings.Repeat("metadata-", 8), ClientID: 1, ClientName: "client",
			ModelPoolID: 2, ModelName: "model", HTTPStatus: http.StatusOK, DurationMS: 1,
		})
	}
	return records
}

func awaitAnalyticsSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func analyticsSignalReady(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return true
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

type blockingAnalyticsResponseWriter struct {
	header       http.Header
	status       int
	writeStarted chan struct{}
	release      chan struct{}
	once         sync.Once
}

func newBlockingAnalyticsResponseWriter() *blockingAnalyticsResponseWriter {
	return &blockingAnalyticsResponseWriter{
		header: make(http.Header), writeStarted: make(chan struct{}), release: make(chan struct{}),
	}
}

func (w *blockingAnalyticsResponseWriter) Header() http.Header { return w.header }
func (w *blockingAnalyticsResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *blockingAnalyticsResponseWriter) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.once.Do(func() { close(w.writeStarted) })
	<-w.release
	return len(value), nil
}

type cancelingAnalyticsResponseWriter struct {
	header     http.Header
	status     int
	writeCalls int
	cancel     context.CancelFunc
}

func (w *cancelingAnalyticsResponseWriter) Header() http.Header { return w.header }
func (w *cancelingAnalyticsResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *cancelingAnalyticsResponseWriter) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.writeCalls++
	if w.writeCalls == 1 {
		w.cancel()
	}
	return len(value), nil
}

type failingAnalyticsResponseWriter struct {
	header     http.Header
	status     int
	writeCalls int
	failAt     int
	body       bytes.Buffer
}

func (w *failingAnalyticsResponseWriter) Header() http.Header { return w.header }

func (w *failingAnalyticsResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *failingAnalyticsResponseWriter) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.writeCalls++
	if w.writeCalls == w.failAt {
		return 0, errors.New("forced response write failure")
	}
	return w.body.Write(value)
}

func (w *failingAnalyticsResponseWriter) Flush() {}
