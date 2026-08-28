package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/html"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

func TestProjectedKeyUsageUpdatesRegistryAfterDurableWrite(t *testing.T) {
	destination := &keyUsageStoreStub{}
	projection := &keyUsageRegistryStub{}
	usedAt := time.Unix(1_700_000_000, 0).UTC()
	writer := projectedKeyUsageStore{destination: destination, projection: projection}
	if err := writer.TouchKeyLastUsed(context.Background(), 7, usedAt); err != nil {
		t.Fatal(err)
	}
	if destination.keyID != 7 || !destination.usedAt.Equal(usedAt) || projection.keyID != 7 || !projection.usedAt.Equal(usedAt) {
		t.Fatalf("destination=%+v projection=%+v", destination, projection)
	}
}

func TestCloseRecorderStoreDefersStoreCloseUntilRecorderDone(t *testing.T) {
	recorder := &lifecycleRecorderStub{done: make(chan struct{}), closeErr: context.DeadlineExceeded}
	store := &closeTrackingStore{closed: make(chan struct{})}
	if err := closeRecorderStore(context.Background(), recorder, store); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("closeRecorderStore() error = %v", err)
	}
	select {
	case <-store.closed:
		t.Fatal("store closed while recorder worker could still access it")
	default:
	}
	close(recorder.done)
	select {
	case <-store.closed:
	case <-time.After(time.Second):
		t.Fatal("store did not close after recorder worker completed")
	}
}

func TestRunServesHealthAndShutsDownGracefully(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	environment := validEnvironment(filepath.Join(t.TempDir(), "gateway.db"))
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	client := &http.Client{Timeout: 200 * time.Millisecond}
	url := "http://" + listener.Addr().String() + "/healthz"
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, requestErr := client.Get(url)
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("gateway did not become healthy")
		}
		time.Sleep(10 * time.Millisecond)
	}
	readyResponse, err := client.Get("http://" + listener.Addr().String() + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	var readiness struct {
		Status              string `json:"status"`
		Revision            int64  `json:"revision"`
		BackendAvailability int    `json:"backendAvailability"`
	}
	if err := json.NewDecoder(readyResponse.Body).Decode(&readiness); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, readyResponse.Body)
	readyResponse.Body.Close()
	if readyResponse.StatusCode != http.StatusOK || readiness.Status != "ready" || readiness.Revision != 0 || readiness.BackendAvailability != 0 {
		t.Fatalf("readiness = %d %+v", readyResponse.StatusCode, readiness)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("gateway did not shut down within deadline")
	}
}

func TestRunRecordsTokenAnalyticsEndToEndWithoutPersistingBodies(t *testing.T) {
	const (
		ordinaryPrompt = "e2e-ordinary-prompt-4f5eb9a7"
		streamPrompt   = "e2e-stream-prompt-739adb16"
		ordinaryOutput = "e2e-ordinary-generated-d84c3f02"
		streamOutput   = "e2e-stream-generated-c9247b51"
	)
	sensitive := []string{ordinaryPrompt, streamPrompt, ordinaryOutput, streamOutput}
	cacheRead := int64(7)
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{
		Tokens: []string{ordinaryOutput},
		Usage: &fakevllm.Usage{
			InputTokens: 11, OutputTokens: 4, CacheReadTokens: &cacheRead,
		},
	})
	upstream := httptest.NewServer(fake.Handler())
	defer upstream.Close()

	databasePath := filepath.Join(t.TempDir(), "gateway.db")
	seed := seedGatewayDatabaseDetails(t, databasePath, upstream.URL, []byte(strings.Repeat("h", 32)))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	environment := validEnvironment(databasePath)
	environment["LLMGW_HEALTH_INTERVAL"] = "10ms"
	environment["LLMGW_METRICS_INTERVAL"] = "10ms"
	environment["LLMGW_UNHEALTHY_AFTER"] = "1"
	environment["LLMGW_RECOVERY_AFTER"] = "1"
	environment["LLMGW_SHUTDOWN_GRACE_PERIOD"] = "2s"
	var stdout, stderr bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- run(ctx, mapLookup(environment), listener, &stdout, &stderr) }()

	baseURL := "http://" + listener.Addr().String()
	waitForGateway(t, 3*time.Second, func() bool {
		body, status, requestErr := gatewayGET(baseURL+"/admin/api/status", "operator", "correct horse battery staple")
		if requestErr != nil || status != http.StatusOK {
			return false
		}
		var view struct {
			Pools []struct {
				ID      int64 `json:"id"`
				Runtime struct {
					State             domain.PoolState `json:"state"`
					AvailableBackends int              `json:"availableBackends"`
				} `json:"runtime"`
			} `json:"pools"`
		}
		if json.Unmarshal(body, &view) != nil {
			return false
		}
		for _, pool := range view.Pools {
			if pool.ID == seed.modelPoolID {
				return pool.Runtime.State != domain.PoolUnavailable && pool.Runtime.AvailableBackends == 1
			}
		}
		return false
	})

	ordinaryBody := `{"model":"qwen","messages":[{"role":"user","content":"` + ordinaryPrompt + `"}]}`
	ordinaryResponse := gatewayPOST(t, baseURL+"/v1/chat/completions", seed.clientKey, "e2e-parent-ordinary", ordinaryBody)
	ordinaryRequestID := ordinaryResponse.Header.Get("X-Request-Id")
	assertGatewayRequestID(t, ordinaryRequestID)
	ordinaryBytes, err := io.ReadAll(ordinaryResponse.Body)
	ordinaryResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	wantOrdinary := `{"choices":[{"finish_reason":"stop","index":0,"message":{"content":"` + ordinaryOutput + `","role":"assistant"}}],"id":"fake-response","model":"fake-model","object":"chat.completion","usage":{"completion_tokens":4,"prompt_tokens":11,"prompt_tokens_details":{"cached_tokens":7},"total_tokens":15}}`
	if ordinaryResponse.StatusCode != http.StatusOK || string(ordinaryBytes) != wantOrdinary {
		t.Fatalf("ordinary client response = %d %s, want %s", ordinaryResponse.StatusCode, ordinaryBytes, wantOrdinary)
	}

	fake.SetState(fakevllm.State{
		Tokens: []string{streamOutput},
		Usage:  &fakevllm.Usage{InputTokens: 13, OutputTokens: 5},
	})
	streamBody := `{"model":"qwen","messages":[{"role":"user","content":"` + streamPrompt + `"}],"stream":true}`
	streamResponse := gatewayPOST(t, baseURL+"/v1/chat/completions", seed.clientKey, "e2e-parent-stream", streamBody)
	streamRequestID := streamResponse.Header.Get("X-Request-Id")
	assertGatewayRequestID(t, streamRequestID)
	if streamRequestID == ordinaryRequestID {
		t.Fatalf("gateway request IDs are not distinct: %q", streamRequestID)
	}
	streamBytes, err := io.ReadAll(streamResponse.Body)
	streamResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	wantStream := "data: {\"choices\":[{\"delta\":{\"content\":\"" + streamOutput + "\"}}],\"usage\":null}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"completion_tokens\":5,\"prompt_tokens\":13,\"total_tokens\":18}}\n\n" +
		"data: [DONE]\n\n"
	if streamResponse.StatusCode != http.StatusOK || string(streamBytes) != wantStream {
		t.Fatalf("stream client response = %d %q, want %q", streamResponse.StatusCode, streamBytes, wantStream)
	}

	upstreamRequests := fake.Snapshot().Requests
	if len(upstreamRequests) != 2 || upstreamRequests[0].RequestID != ordinaryRequestID || upstreamRequests[0].IncludeUsage ||
		upstreamRequests[1].RequestID != streamRequestID || !upstreamRequests[1].IncludeUsage {
		t.Fatalf("upstream requests = %+v", upstreamRequests)
	}

	filter := url.Values{
		"from":            {time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)},
		"to":              {time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)},
		"client_id":       {strconv.FormatInt(seed.clientID, 10)},
		"model_pool_id":   {strconv.FormatInt(seed.modelPoolID, 10)},
		"usage_available": {"true"},
	}
	var pageBody []byte
	waitForGateway(t, 3*time.Second, func() bool {
		values := filter.Clone()
		values.Set("limit", "10")
		body, status, requestErr := gatewayGET(baseURL+"/admin/api/analytics/requests?"+values.Encode(), "operator", "correct horse battery staple")
		if requestErr != nil || status != http.StatusOK {
			return false
		}
		var page analytics.RequestPage
		if json.Unmarshal(body, &page) != nil || page.Total != 2 || len(page.Requests) != 2 {
			return false
		}
		pageBody = append([]byte(nil), body...)
		return true
	})

	metricsBody, status, err := gatewayGET(baseURL+"/metrics", "", "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("metrics response = %d err=%v body=%s", status, err, metricsBody)
	}
	for _, line := range []string{
		`llmgw_input_tokens_total{client="stream-client",model="qwen"} 24`,
		`llmgw_output_tokens_total{client="stream-client",model="qwen"} 9`,
		`llmgw_cache_read_tokens_total{client="stream-client",model="qwen"} 7`,
	} {
		if !strings.Contains(string(metricsBody), line+"\n") {
			t.Fatalf("metrics missing %q:\n%s", line, metricsBody)
		}
	}
	if strings.Contains(string(metricsBody), `llmgw_usage_parse_failures_total{format="sse"}`) {
		t.Fatalf("interim null usage registered an SSE parse failure:\n%s", metricsBody)
	}

	summaryBody, status, err := gatewayGET(baseURL+"/admin/api/analytics?"+filter.Encode(), "operator", "correct horse battery staple")
	if err != nil || status != http.StatusOK {
		t.Fatalf("analytics response = %d err=%v body=%s", status, err, summaryBody)
	}
	var dataset analytics.Dataset
	if err := json.Unmarshal(summaryBody, &dataset); err != nil {
		t.Fatal(err)
	}
	if dataset.Summary.RequestCount != 2 || dataset.Summary.MeteredRequestCount != 2 || dataset.Summary.UsageCoverage != 1 ||
		dataset.Summary.InputTokens != 24 || dataset.Summary.OutputTokens != 9 ||
		dataset.Summary.CacheReadTokens == nil || *dataset.Summary.CacheReadTokens != 7 ||
		dataset.Summary.UncachedInputTokens == nil || *dataset.Summary.UncachedInputTokens != 4 ||
		dataset.Summary.CacheHitRatio == nil || *dataset.Summary.CacheHitRatio != 7.0/11.0 {
		t.Fatalf("filtered analytics summary = %+v", dataset.Summary)
	}
	negativeArtifacts := make(map[string][]byte)
	for _, test := range []struct {
		name string
		edit func(url.Values)
	}{
		{name: "wrong client", edit: func(values url.Values) { values.Set("client_id", strconv.FormatInt(seed.clientID+1000, 10)) }},
		{name: "wrong model", edit: func(values url.Values) { values.Set("model_pool_id", strconv.FormatInt(seed.modelPoolID+1000, 10)) }},
		{name: "usage unavailable", edit: func(values url.Values) { values.Set("usage_available", "false") }},
	} {
		values := filter.Clone()
		test.edit(values)
		body, negativeStatus, requestErr := gatewayGET(baseURL+"/admin/api/analytics?"+values.Encode(), "operator", "correct horse battery staple")
		if requestErr != nil || negativeStatus != http.StatusOK {
			t.Fatalf("%s analytics = %d err=%v body=%s", test.name, negativeStatus, requestErr, body)
		}
		var emptyDataset analytics.Dataset
		if err := json.Unmarshal(body, &emptyDataset); err != nil {
			t.Fatal(err)
		}
		if emptyDataset.Summary != (analytics.Summary{}) || len(emptyDataset.Series) != 0 || len(emptyDataset.Breakdown) != 0 {
			t.Fatalf("%s analytics was not empty: %+v", test.name, emptyDataset)
		}
		negativeArtifacts[test.name+" analytics JSON"] = body
	}
	usageUnavailable := filter.Clone()
	usageUnavailable.Set("usage_available", "false")
	emptyRequestsBody, status, err := gatewayGET(baseURL+"/admin/api/analytics/requests?"+usageUnavailable.Encode(), "operator", "correct horse battery staple")
	if err != nil || status != http.StatusOK {
		t.Fatalf("usage-unavailable requests = %d err=%v body=%s", status, err, emptyRequestsBody)
	}
	var emptyPage analytics.RequestPage
	if err := json.Unmarshal(emptyRequestsBody, &emptyPage); err != nil {
		t.Fatal(err)
	}
	if emptyPage.Total != 0 || len(emptyPage.Requests) != 0 {
		t.Fatalf("usage-unavailable request page = %+v", emptyPage)
	}
	negativeArtifacts["usage-unavailable request JSON"] = emptyRequestsBody
	emptyCSVBody, status, err := gatewayGET(baseURL+"/admin/api/analytics/export.csv?"+usageUnavailable.Encode(), "operator", "correct horse battery staple")
	if err != nil || status != http.StatusOK {
		t.Fatalf("usage-unavailable CSV = %d err=%v body=%s", status, err, emptyCSVBody)
	}
	emptyCSVRows, err := csv.NewReader(bytes.NewReader(emptyCSVBody)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(emptyCSVRows) != 1 || len(emptyCSVRows[0]) != 18 {
		t.Fatalf("usage-unavailable CSV rows = %#v", emptyCSVRows)
	}
	negativeArtifacts["usage-unavailable CSV"] = emptyCSVBody

	var requestPage analytics.RequestPage
	if err := json.Unmarshal(pageBody, &requestPage); err != nil {
		t.Fatal(err)
	}
	if requestPage.Total != 2 || len(requestPage.Requests) != 2 ||
		requestPage.Requests[0].RequestID != streamRequestID || requestPage.Requests[1].RequestID != ordinaryRequestID {
		t.Fatalf("filtered request page = %+v", requestPage)
	}
	assertUsageRow(t, requestPage.Requests[0], 13, 5, nil)
	assertUsageRow(t, requestPage.Requests[1], 11, 4, &cacheRead)

	pageValues := filter.Clone()
	pageValues.Set("limit", "10")
	htmlBody, status, err := gatewayGET(baseURL+"/admin/analytics?"+pageValues.Encode(), "operator", "correct horse battery staple")
	if err != nil || status != http.StatusOK {
		t.Fatalf("analytics HTML = %d err=%v body=%s", status, err, htmlBody)
	}
	htmlText := string(htmlBody)
	if !strings.Contains(htmlText, "Usage analytics") ||
		!strings.Contains(htmlText, ">24</strong>") || !strings.Contains(htmlText, ">9</strong>") || !strings.Contains(htmlText, ">7</strong>") {
		t.Fatalf("analytics HTML does not agree with filtered data: %s", htmlText)
	}
	htmlRows, htmlOrder, err := analyticsHTMLRequestRows(htmlBody)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(htmlOrder, []string{streamRequestID, ordinaryRequestID}) {
		t.Fatalf("analytics HTML request order = %v", htmlOrder)
	}
	if got := htmlRows[ordinaryRequestID]; !reflect.DeepEqual(got, []string{"11", "4", "7"}) {
		t.Fatalf("ordinary analytics HTML token cells = %v, want [11 4 7]", got)
	}
	if got := htmlRows[streamRequestID]; !reflect.DeepEqual(got, []string{"13", "5", "—"}) {
		t.Fatalf("stream analytics HTML token cells = %v, want [13 5 —]", got)
	}

	csvBody, status, err := gatewayGET(baseURL+"/admin/api/analytics/export.csv?"+filter.Encode(), "operator", "correct horse battery staple")
	if err != nil || status != http.StatusOK {
		t.Fatalf("analytics CSV = %d err=%v body=%s", status, err, csvBody)
	}
	csvRows, err := csv.NewReader(bytes.NewReader(csvBody)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(csvRows) != 3 || len(csvRows[0]) != 18 || csvRows[1][2] != ordinaryRequestID || csvRows[2][2] != streamRequestID ||
		csvRows[1][14] != "true" || csvRows[1][15] != "11" || csvRows[1][16] != "4" || csvRows[1][17] != "7" ||
		csvRows[2][14] != "true" || csvRows[2][15] != "13" || csvRows[2][16] != "5" || csvRows[2][17] != "" {
		t.Fatalf("filtered CSV rows = %#v", csvRows)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("gateway did not shut down")
	}
	artifacts := map[string][]byte{
		"stderr logs": stderr.Bytes(), "metrics": metricsBody, "analytics JSON": summaryBody,
		"request JSON": pageBody, "analytics HTML": htmlBody, "analytics CSV": csvBody,
	}
	for name, contents := range negativeArtifacts {
		artifacts[name] = contents
	}
	assertSensitiveArtifactsAbsent(t, artifacts, sensitive)
	assertSensitiveSQLiteFilesAbsent(t, "before raw database open/checkpoint", databasePath, sensitive)

	raw, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := raw.Query(`
		SELECT request_id, http_status, input_tokens, output_tokens, cache_read_tokens
		FROM usage_requests ORDER BY id ASC`)
	if err != nil {
		t.Fatal(err)
	}
	type databaseUsageRow struct {
		requestID                string
		status                   int
		input, output, cacheRead sql.NullInt64
	}
	var databaseRows []databaseUsageRow
	for rows.Next() {
		var row databaseUsageRow
		if err := rows.Scan(&row.requestID, &row.status, &row.input, &row.output, &row.cacheRead); err != nil {
			t.Fatal(err)
		}
		databaseRows = append(databaseRows, row)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(databaseRows) != 2 || databaseRows[0].requestID != ordinaryRequestID || databaseRows[0].status != http.StatusOK ||
		databaseRows[0].input.Int64 != 11 || !databaseRows[0].input.Valid || databaseRows[0].output.Int64 != 4 || !databaseRows[0].output.Valid ||
		databaseRows[0].cacheRead.Int64 != 7 || !databaseRows[0].cacheRead.Valid ||
		databaseRows[1].requestID != streamRequestID || databaseRows[1].status != http.StatusOK ||
		databaseRows[1].input.Int64 != 13 || !databaseRows[1].input.Valid || databaseRows[1].output.Int64 != 5 || !databaseRows[1].output.Valid || databaseRows[1].cacheRead.Valid {
		t.Fatalf("usage ledger rows = %+v", databaseRows)
	}
	if _, err := raw.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	assertSensitiveSQLiteFilesAbsent(t, "after checkpoint and close", databasePath, sensitive)
}

func TestGatewayRequestIDValidationRequiresLowercaseHex(t *testing.T) {
	if !validGatewayRequestID("0123456789abcdef0123456789abcdef") {
		t.Fatal("valid lowercase request ID was rejected")
	}
	for _, invalid := range []string{
		"0123456789ABCDEF0123456789ABCDEF",
		"0123456789abcdef0123456789abcdeg",
		"0123456789abcdef",
	} {
		if validGatewayRequestID(invalid) {
			t.Fatalf("invalid request ID %q was accepted", invalid)
		}
	}
}

func TestRunLetsActiveStreamFinishInsideGracePeriod(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{Tokens: []string{"one", "two"}, TokenDelay: 100 * time.Millisecond})
	upstream := httptest.NewServer(fake.Handler())
	defer upstream.Close()
	databasePath := filepath.Join(t.TempDir(), "gateway.db")
	secret := strings.Repeat("h", 32)
	clientKey := seedGatewayDatabase(t, databasePath, upstream.URL, []byte(secret))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	environment := validEnvironment(databasePath)
	environment["LLMGW_HEALTH_INTERVAL"] = "10ms"
	environment["LLMGW_METRICS_INTERVAL"] = "10ms"
	environment["LLMGW_UNHEALTHY_AFTER"] = "1"
	environment["LLMGW_RECOVERY_AFTER"] = "1"
	environment["LLMGW_SHUTDOWN_GRACE_PERIOD"] = "1s"
	done := make(chan error, 1)
	go func() { done <- run(ctx, mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{}) }()

	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}
	var response *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for {
		request, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/completions", strings.NewReader(`{"model":"qwen","stream":true}`))
		request.Header.Set("Authorization", "Bearer "+clientKey)
		response, err = client.Do(request)
		if err == nil && response.StatusCode == http.StatusOK {
			break
		}
		if response != nil {
			response.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(first, "one") {
		t.Fatalf("first stream frame = %q err=%v", first, err)
	}
	cancel()
	rest, err := io.ReadAll(reader)
	response.Body.Close()
	if err != nil || !strings.Contains(string(rest), "two") || !strings.Contains(string(rest), "[DONE]") {
		t.Fatalf("stream remainder = %q err=%v", rest, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gateway did not finish after active stream")
	}
	if snapshot := fake.Snapshot(); snapshot.ActiveRequests != 0 {
		t.Fatalf("upstream requests still active: %+v", snapshot)
	}
	raw, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open usage database: %v", err)
	}
	var usageRows int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM usage_requests WHERE http_status = 200`).Scan(&usageRows); err != nil {
		_ = raw.Close()
		t.Fatalf("count usage requests after shutdown: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close usage database: %v", err)
	}
	if usageRows != 1 {
		t.Fatalf("persisted successful usage rows after graceful shutdown = %d, want 1", usageRows)
	}
	database, err := store.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("database remained locked after shutdown: %v", err)
	}
	_ = database.Close()
}

func TestRunForceClosesActiveStreamAfterGracePeriod(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{Tokens: []string{"one", "two", "three"}, TokenDelay: 500 * time.Millisecond})
	upstream := httptest.NewServer(fake.Handler())
	defer upstream.Close()
	databasePath := filepath.Join(t.TempDir(), "gateway.db")
	secret := strings.Repeat("h", 32)
	clientKey := seedGatewayDatabase(t, databasePath, upstream.URL, []byte(secret))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	environment := validEnvironment(databasePath)
	environment["LLMGW_HEALTH_INTERVAL"] = "10ms"
	environment["LLMGW_METRICS_INTERVAL"] = "10ms"
	environment["LLMGW_UNHEALTHY_AFTER"] = "1"
	environment["LLMGW_RECOVERY_AFTER"] = "1"
	environment["LLMGW_SHUTDOWN_GRACE_PERIOD"] = "30ms"
	done := make(chan error, 1)
	go func() { done <- run(ctx, mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{}) }()

	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}
	var response *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for {
		request, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/completions", strings.NewReader(`{"model":"qwen","stream":true}`))
		request.Header.Set("Authorization", "Bearer "+clientKey)
		response, err = client.Do(request)
		if err == nil && response.StatusCode == http.StatusOK {
			break
		}
		if response != nil {
			response.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(first, "one") {
		t.Fatalf("first stream frame = %q err=%v", first, err)
	}
	cancel()
	_, _ = io.ReadAll(reader)
	response.Body.Close()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "graceful HTTP shutdown") {
			t.Fatalf("run error = %v, want forced shutdown error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gateway did not force-close after grace period")
	}
	deadline = time.Now().Add(time.Second)
	for fake.Snapshot().ActiveRequests != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if snapshot := fake.Snapshot(); snapshot.ActiveRequests != 0 {
		t.Fatalf("forced shutdown left upstream request active: %+v", snapshot)
	}
	database, err := store.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("database remained locked after forced shutdown: %v", err)
	}
	_ = database.Close()
}

func TestRunRejectsMissingHMACSecretBeforeServing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	environment := validEnvironment(filepath.Join(t.TempDir(), "gateway.db"))
	delete(environment, "LLMGW_API_KEY_HMAC_SECRET")
	err = run(context.Background(), mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "LLMGW_API_KEY_HMAC_SECRET") {
		t.Fatalf("run error = %v", err)
	}
}

func validEnvironment(databasePath string) map[string]string {
	return map[string]string{
		"LLMGW_LISTEN_ADDRESS":        "127.0.0.1:0",
		"LLMGW_DATABASE_PATH":         databasePath,
		"LLMGW_ADMIN_USERNAME":        "operator",
		"LLMGW_ADMIN_PASSWORD":        "correct horse battery staple",
		"LLMGW_API_KEY_HMAC_SECRET":   strings.Repeat("h", 32),
		"LLMGW_SHUTDOWN_GRACE_PERIOD": "500ms",
	}
}

type gatewayDatabaseSeed struct {
	clientKey   string
	clientID    int64
	modelPoolID int64
}

func seedGatewayDatabase(t *testing.T, path, upstreamURL string, hmacSecret []byte) string {
	t.Helper()
	return seedGatewayDatabaseDetails(t, path, upstreamURL, hmacSecret).clientKey
}

func seedGatewayDatabaseDetails(t *testing.T, path, upstreamURL string, hmacSecret []byte) gatewayDatabaseSeed {
	t.Helper()
	database, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := database.CreatePool(context.Background(), store.CreatePoolParams{PublicModelName: "qwen", UpstreamModelName: "fake-model", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateBackend(context.Background(), store.CreateBackendParams{ModelPoolID: pool.ID, Name: "gpu-a", BaseURL: upstreamURL, Enabled: true, CapacityHint: 1, RunningSoftLimit: 16}); err != nil {
		t.Fatal(err)
	}
	client, err := database.CreateClient(context.Background(), store.CreateClientParams{Name: "stream-client", Enabled: true, PriorityClass: domain.PriorityHigh, VLLMPriority: -10, MaxConcurrency: 1, ModelPoolIDs: []int64{pool.ID}})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := apikey.Generate(bytes.NewReader(bytes.Repeat([]byte{7}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateAPIKey(context.Background(), store.CreateAPIKeyParams{ClientID: client.ID, Prefix: plain.Prefix, SecretHash: apikey.Digest(hmacSecret, plain.Value)}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	return gatewayDatabaseSeed{clientKey: plain.Value, clientID: client.ID, modelPoolID: pool.ID}
}

func gatewayPOST(t *testing.T, endpoint, key, parentRequestID, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Request-Id", parentRequestID)
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func gatewayGET(endpoint, username, password string) ([]byte, int, error) {
	request, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	if username != "" {
		request.SetBasicAuth(username, password)
	}
	response, err := (&http.Client{Timeout: 500 * time.Millisecond}).Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return body, response.StatusCode, err
}

func waitForGateway(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("gateway condition was not met before timeout")
}

func assertGatewayRequestID(t *testing.T, requestID string) {
	t.Helper()
	if !validGatewayRequestID(requestID) {
		t.Fatalf("gateway request ID %q is not 16-byte lowercase hex", requestID)
	}
}

func validGatewayRequestID(requestID string) bool {
	if len(requestID) != 32 {
		return false
	}
	for index := 0; index < len(requestID); index++ {
		character := requestID[index]
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func analyticsHTMLRequestRows(body []byte) (map[string][]string, []string, error) {
	document, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	table := findHTMLElement(document, "table", "request-table")
	if table == nil {
		return nil, nil, errors.New("analytics request table is missing")
	}
	rows := make(map[string][]string)
	order := make([]string, 0)
	var visit func(*html.Node) error
	visit = func(node *html.Node) error {
		if node.Type == html.ElementNode && node.Data == "tr" {
			requestIDNode := findHTMLElement(node, "strong", "request-id")
			if requestIDNode != nil {
				requestID := normalizedHTMLText(requestIDNode)
				cells := directHTMLChildren(node, "td")
				if len(cells) != 12 {
					return fmt.Errorf("analytics request %q has %d cells, want 12", requestID, len(cells))
				}
				rows[requestID] = []string{
					normalizedHTMLText(cells[9]), normalizedHTMLText(cells[10]), normalizedHTMLText(cells[11]),
				}
				order = append(order, requestID)
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(table); err != nil {
		return nil, nil, err
	}
	return rows, order, nil
}

func findHTMLElement(node *html.Node, tag, class string) *html.Node {
	if node.Type == html.ElementNode && node.Data == tag && htmlHasClass(node, class) {
		return node
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if found := findHTMLElement(child, tag, class); found != nil {
			return found
		}
	}
	return nil
}

func htmlHasClass(node *html.Node, class string) bool {
	for _, attribute := range node.Attr {
		if attribute.Key == "class" {
			for _, candidate := range strings.Fields(attribute.Val) {
				if candidate == class {
					return true
				}
			}
		}
	}
	return false
}

func directHTMLChildren(node *html.Node, tag string) []*html.Node {
	children := make([]*html.Node, 0)
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if child.Type == html.ElementNode && child.Data == tag {
			children = append(children, child)
		}
	}
	return children
}

func normalizedHTMLText(node *html.Node) string {
	var text strings.Builder
	var visit func(*html.Node)
	visit = func(current *html.Node) {
		if current.Type == html.TextNode {
			text.WriteString(current.Data)
		}
		for child := current.FirstChild; child != nil; child = child.NextSibling {
			visit(child)
		}
	}
	visit(node)
	return strings.Join(strings.Fields(text.String()), " ")
}

func assertSensitiveArtifactsAbsent(t *testing.T, artifacts map[string][]byte, sentinels []string) {
	t.Helper()
	for name, contents := range artifacts {
		for _, sentinel := range sentinels {
			if bytes.Contains(contents, []byte(sentinel)) {
				t.Fatalf("%s contains sensitive body sentinel %q", name, sentinel)
			}
		}
	}
}

func assertSensitiveSQLiteFilesAbsent(t *testing.T, phase, databasePath string, sentinels []string) {
	t.Helper()
	for _, suffix := range []string{"", "-wal"} {
		contents, err := os.ReadFile(databasePath + suffix)
		if err != nil {
			if suffix == "-wal" && os.IsNotExist(err) {
				continue
			}
			t.Fatalf("%s: read SQLite%s: %v", phase, suffix, err)
		}
		for _, sentinel := range sentinels {
			if bytes.Contains(contents, []byte(sentinel)) {
				t.Fatalf("%s: SQLite%s contains sensitive body sentinel %q", phase, suffix, sentinel)
			}
		}
	}
}

func assertUsageRow(t *testing.T, row analytics.RequestRecord, input, output int64, cacheRead *int64) {
	t.Helper()
	if !row.UsageAvailable || row.HTTPStatus != http.StatusOK || row.InputTokens == nil || *row.InputTokens != input ||
		row.OutputTokens == nil || *row.OutputTokens != output || !reflect.DeepEqual(row.CacheReadTokens, cacheRead) {
		t.Fatalf("usage row = %+v, want input=%d output=%d cache=%v", row, input, output, cacheRead)
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

type keyUsageStoreStub struct {
	keyID  int64
	usedAt time.Time
}

func (s *keyUsageStoreStub) TouchKeyLastUsed(_ context.Context, keyID int64, usedAt time.Time) error {
	s.keyID, s.usedAt = keyID, usedAt
	return nil
}

type keyUsageRegistryStub struct {
	keyID  int64
	usedAt time.Time
}

type lifecycleRecorderStub struct {
	done     chan struct{}
	closeErr error
}

func (r *lifecycleRecorderStub) Close(context.Context) error { return r.closeErr }
func (r *lifecycleRecorderStub) Done() <-chan struct{}       { return r.done }

type closeTrackingStore struct{ closed chan struct{} }

func (s *closeTrackingStore) Close() error {
	close(s.closed)
	return nil
}

func (s *keyUsageRegistryStub) MarkKeyUsed(keyID int64, usedAt time.Time) bool {
	s.keyID, s.usedAt = keyID, usedAt
	return true
}
