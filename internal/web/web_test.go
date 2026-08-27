package web_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/html"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
	"github.com/rislanov/vllm-priority-gateway/internal/web"
)

func TestAnalyticsPageRendersCanonicalFiltersSummaryAndPagination(t *testing.T) {
	handler := newAnalyticsWebFixture(t)
	values := url.Values{
		"from":            {"2026-08-26T09:00:00Z"},
		"to":              {"2026-08-26T12:00:00Z"},
		"client_id":       {"1"},
		"model_pool_id":   {"1"},
		"usage_available": {"true"},
		"limit":           {"1"},
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/analytics?"+values.Encode(), nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	document, err := html.Parse(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		`href="/admin/analytics" aria-current="page"`,
		`value="2026-08-26T09:00"`, `value="2026-08-26T12:00"`,
		`value="1" selected`, `value="true" selected`,
		"Requests", "2", "Usage coverage", "100%", "Input tokens", "150",
		"Output tokens", "30", "Cache-read tokens", "40", "Cache-hit ratio", "40%",
		"Coverage is metered requests divided by all matching requests.",
		"Cache metrics use only requests where cache-read usage is known.",
		"req-cache-unknown",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("analytics page missing %q: %s", expected, body)
		}
	}
	if strings.Contains(strings.ToLower(body), "prompt text") || strings.Contains(strings.ToLower(body), "response text") {
		t.Fatalf("analytics page exposed prompt/response text fields: %s", body)
	}
	for _, header := range []string{"Client", "Model", "Requests", "Coverage", "Input", "Output", "Cache read", "Cache hit", "Occurred at", "Request ID", "Backend", "Status", "Duration", "TTFT", "Retries"} {
		if !hasElementText(document, "th", header) {
			t.Fatalf("missing analytics table header %q", header)
		}
	}
	if !allLabelsReferenceControls(document) {
		t.Fatal("analytics page has a label whose for attribute does not reference a control")
	}

	exportURL := attrForElementWithText(document, "a", "Download CSV", "href")
	assertAnalyticsLink(t, exportURL, "/admin/api/analytics/export.csv", values, false)
	nextURL := attrForElementWithText(document, "a", "Next", "href")
	assertAnalyticsLink(t, nextURL, "/admin/analytics", values, true)
}

func TestAnalyticsPageRendersMissingUsageAndHonestEmptyState(t *testing.T) {
	handler := newAnalyticsWebFixture(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/analytics?from=2026-08-26T09%3A00%3A00Z&to=2026-08-26T12%3A00%3A00Z", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	if !strings.Contains(body, "req-unmetered") || !strings.Contains(body, "—") {
		t.Fatalf("missing-usage request or em dash absent: %s", body)
	}
	if strings.Contains(body, "<script>alert(1)</script>") || !strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt;") {
		t.Fatalf("dimension labels were not HTML escaped: %s", body)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/analytics?from=2026-08-20T09%3A00%3A00Z&to=2026-08-20T12%3A00%3A00Z", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "No requests match the active filters.") || !strings.Contains(response.Body.String(), "No series data is available for this range.") {
		t.Fatalf("empty analytics response = %d body=%s", response.Code, response.Body.String())
	}
}

func TestAnalyticsPageProvidesAccessibleChartFallbacksAndSelfHostedScript(t *testing.T) {
	handler := newAnalyticsWebFixture(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/analytics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	document, err := html.Parse(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if countElementsWithAttr(document, "data-analytics-chart") != 3 {
		t.Fatalf("chart containers = %d, want 3: %s", countElementsWithAttr(document, "data-analytics-chart"), body)
	}
	if countElements(document, "caption") < 3 || !strings.Contains(body, "Request volume data") || !strings.Contains(body, "Input and output token data") || !strings.Contains(body, "Cache-known usage data") {
		t.Fatalf("server-rendered chart fallbacks missing: %s", body)
	}
	if !strings.Contains(body, `src="/admin/static/app.js"`) || strings.Contains(body, "https://") || strings.Contains(body, "http://") {
		t.Fatalf("analytics page must use only self-hosted assets: %s", body)
	}

	script := httptest.NewRecorder()
	handler.ServeHTTP(script, httptest.NewRequest(http.MethodGet, "/admin/static/app.js", nil))
	for _, required := range []string{"fetch(", "createElementNS", "textContent", "/admin/api/analytics"} {
		if !strings.Contains(script.Body.String(), required) {
			t.Fatalf("self-hosted chart script missing %q: %s", required, script.Body.String())
		}
	}
	if strings.Contains(script.Body.String(), "innerHTML") {
		t.Fatalf("chart script must not construct markup with innerHTML: %s", script.Body.String())
	}
}

func TestAnalyticsCustomUTCFormRedirectsToStrictCanonicalQuery(t *testing.T) {
	handler := newAnalyticsWebFixture(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/analytics?from_local=2026-08-26T09%3A00&to_local=2026-08-26T12%3A00&client_id=&model_pool_id=&usage_available=&limit=100", nil))
	if response.Code != http.StatusSeeOther {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	location := response.Header().Get("Location")
	parsed, err := url.Parse(location)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Path != "/admin/analytics" || parsed.Query().Get("from") != "2026-08-26T09:00:00Z" || parsed.Query().Get("to") != "2026-08-26T12:00:00Z" {
		t.Fatalf("canonical redirect = %q", location)
	}
	for _, empty := range []string{"from_local", "to_local", "client_id", "model_pool_id", "usage_available"} {
		if parsed.Query().Has(empty) {
			t.Fatalf("canonical redirect retained empty/web-only %q: %s", empty, location)
		}
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, location, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("canonical target status = %d body=%s", response.Code, response.Body.String())
	}
}

func TestAdminPagesHaveSemanticNavigationFormsAndTables(t *testing.T) {
	handler := newWebFixture(t)
	checks := []struct {
		path    string
		headers []string
		text    string
	}{
		{path: "/admin", headers: []string{"Pool", "State", "Pressure", "Backend", "Running", "Waiting", "KV cache"}, text: "Gateway overview"},
		{path: "/admin/clients", headers: []string{"Name", "Class", "vLLM priority", "Max concurrency", "Models", "Status", "Actions"}, text: "Create client"},
		{path: "/admin/keys", headers: []string{"Prefix", "Client", "Created", "Expires", "Last used", "Status", "Actions"}, text: "Generate API key"},
		{path: "/admin/backends", headers: []string{"Name", "Model pool", "URL", "State", "Pressure", "Enabled", "Draining", "Actions"}, text: "Create backend"},
	}
	for _, check := range checks {
		t.Run(check.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, check.path, nil))
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), check.text) {
				t.Fatalf("response = %d body=%s", response.Code, response.Body.String())
			}
			document, err := html.Parse(strings.NewReader(response.Body.String()))
			if err != nil {
				t.Fatal(err)
			}
			if got := countElements(document, "nav"); got != 1 {
				t.Fatalf("nav landmarks = %d", got)
			}
			for _, header := range check.headers {
				if !hasElementText(document, "th", header) {
					t.Fatalf("missing table header %q", header)
				}
			}
			if !allLabelsReferenceControls(document) {
				t.Fatal("page has a label whose for attribute does not reference a control")
			}
		})
	}
}

func TestKeyFormRendersOneTimeSecretRegion(t *testing.T) {
	handler := newWebFixture(t)
	request := httptest.NewRequest(http.MethodPost, "/admin/keys", strings.NewReader("client_id=1&action=create&csrf_token=test"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	location := response.Header().Get("Location")
	if response.Code != http.StatusSeeOther || !strings.HasPrefix(location, "/admin/keys?flash=") {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, location, nil))
	document, err := html.Parse(strings.NewReader(response.Body.String()))
	if err != nil {
		t.Fatal(err)
	}
	if !hasAttr(document, "id", "one-time-secret") || !strings.Contains(response.Body.String(), "llmgw_") {
		t.Fatalf("one-time secret region missing: %s", response.Body.String())
	}
	secret := regexp.MustCompile(`llmgw_[A-Za-z0-9_-]{43}`).FindString(response.Body.String())
	if secret == "" {
		t.Fatalf("complete one-time secret missing: %s", response.Body.String())
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, location, nil))
	if strings.Contains(response.Body.String(), `id="one-time-secret"`) || strings.Contains(response.Body.String(), secret) {
		t.Fatalf("one-time secret survived refresh: %s", response.Body.String())
	}
}

func TestOverlappingKeyCreationsKeepSeparateOneTimeSecrets(t *testing.T) {
	handler := newWebFixture(t)
	locations := make(chan string, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			request := httptest.NewRequest(http.MethodPost, "/admin/keys", strings.NewReader("client_id=1&action=create&csrf_token=test"))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusSeeOther {
				t.Errorf("status = %d body=%s", response.Code, response.Body.String())
				return
			}
			locations <- response.Header().Get("Location")
		}()
	}
	wait.Wait()
	close(locations)

	seen := make(map[string]bool)
	for location := range locations {
		if location == "" || seen[location] {
			t.Fatalf("flash redirect was not unique: %q", location)
		}
		seen[location] = true
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, location, nil))
		if !strings.Contains(response.Body.String(), `id="one-time-secret"`) {
			t.Fatalf("secret for %q was lost: %s", location, response.Body.String())
		}
	}
	if len(seen) != 2 {
		t.Fatalf("one-time secret redirects = %v", seen)
	}
}

func TestClientEditPagePrefillsExistingPolicy(t *testing.T) {
	handler := newWebFixture(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/clients?edit=1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, expected := range []string{"Edit client: payments", `value="payments"`, `value="-100"`, `value="24"`, `value="1" checked`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("edit page missing %q: %s", expected, body)
		}
	}
}

func TestBackendEditPageAndEnableToggle(t *testing.T) {
	handler := newWebFixture(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/backends?edit=1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	for _, expected := range []string{
		"Edit backend: gpu-a", `value="gpu-a"`, `value="http://127.0.0.1:9001"`,
		`value="16"`, `href="/admin/backends?edit=1#backend-editor"`, ">Disable<",
	} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Fatalf("backend edit page missing %q: %s", expected, response.Body.String())
		}
	}

	form := "action=update_backend&id=1&model_pool_id=1&name=gpu-a&base_url=http%3A%2F%2F127.0.0.1%3A9001&capacity_hint=1&running_soft_limit=16"
	request := httptest.NewRequest(http.MethodPost, "/admin/backends", strings.NewReader(form))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "Enable</button>") {
		t.Fatalf("disable response = %d body=%s", response.Code, response.Body.String())
	}
}

func newWebFixture(t *testing.T) http.Handler {
	return newWebFixtureWithUsage(t, nil)
}

func newAnalyticsWebFixture(t *testing.T) http.Handler {
	t.Helper()
	input100, output20, cache40 := int64(100), int64(20), int64(40)
	input50, output10 := int64(50), int64(10)
	return newWebFixtureWithUsage(t, []analytics.RequestRecord{
		{
			OccurredAt: time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC), RequestID: "req-cache-known",
			ClientID: 1, ClientName: "payments", ModelPoolID: 1, ModelName: "qwen-72b", BackendName: "gpu-a",
			HTTPStatus: 200, DurationMS: 320, UsageAvailable: true,
			InputTokens: &input100, OutputTokens: &output20, CacheReadTokens: &cache40,
		},
		{
			OccurredAt: time.Date(2026, 8, 26, 11, 0, 0, 0, time.UTC), RequestID: "req-cache-unknown",
			ClientID: 1, ClientName: "payments", ModelPoolID: 1, ModelName: "qwen-72b", BackendName: "gpu-a",
			HTTPStatus: 200, DurationMS: 280, RetryCount: 1, UsageAvailable: true,
			InputTokens: &input50, OutputTokens: &output10,
		},
		{
			OccurredAt: time.Date(2026, 8, 26, 11, 30, 0, 0, time.UTC), RequestID: "req-unmetered",
			ClientID: 2, ClientName: "<script>alert(1)</script>", ModelPoolID: 2, ModelName: "unmetered-model", BackendName: "gpu-b",
			HTTPStatus: 503, DurationMS: 45,
		},
	})
}

func newWebFixtureWithUsage(t *testing.T, records []analytics.RequestRecord) http.Handler {
	t.Helper()
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	if err := database.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := database.InsertUsageBatch(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	pool, err := database.CreatePool(context.Background(), store.CreatePoolParams{PublicModelName: "qwen-72b", UpstreamModelName: "Qwen/Qwen2.5-72B-Instruct", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	client, err := database.CreateClient(context.Background(), store.CreateClientParams{Name: "payments", Enabled: true, PriorityClass: domain.PriorityCritical, VLLMPriority: -100, MaxConcurrency: 24, ModelPoolIDs: []int64{pool.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if client.ID != 1 {
		t.Fatalf("client id = %d", client.ID)
	}
	_, err = database.CreateBackend(context.Background(), store.CreateBackendParams{ModelPoolID: pool.ID, Name: "gpu-a", BaseURL: "http://127.0.0.1:9001", Enabled: true, CapacityHint: 1, RunningSoftLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	registryValue := registry.New(database)
	if err := registryValue.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Analytics: database, Registry: registryValue, Runtime: webRuntime{},
		HMACSecret: []byte(strings.Repeat("h", 32)), Random: bytes.NewReader(bytes.Repeat([]byte{4}, 4096)),
		Now: func() time.Time { return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := web.New(service)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

type webRuntime struct{}

func (webRuntime) Reconcile([]domain.Backend) error { return nil }
func (webRuntime) Snapshot(id int64, _ time.Time) domain.BackendRuntime {
	return domain.BackendRuntime{BackendID: id, State: domain.BackendHealthy, Healthy: true, MetricsFresh: true, Running: 12, Waiting: 2, KVCacheUsage: .78, Pressure: .72}
}
func (webRuntime) PoolSnapshot(id int64, _ time.Time) domain.PoolRuntime {
	return domain.PoolRuntime{PoolID: id, State: domain.PoolBusy, BestBackendPressure: .72, AvailableBackends: 1}
}

func countElements(node *html.Node, name string) int {
	count := 0
	if node.Type == html.ElementNode && node.Data == name {
		count++
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		count += countElements(child, name)
	}
	return count
}

func hasElementText(node *html.Node, name, text string) bool {
	if node.Type == html.ElementNode && node.Data == name && strings.TrimSpace(nodeText(node)) == text {
		return true
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if hasElementText(child, name, text) {
			return true
		}
	}
	return false
}

func nodeText(node *html.Node) string {
	if node.Type == html.TextNode {
		return node.Data
	}
	var output strings.Builder
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		output.WriteString(nodeText(child))
	}
	return output.String()
}

func allLabelsReferenceControls(document *html.Node) bool {
	ids := map[string]bool{}
	var collect func(*html.Node)
	collect = func(node *html.Node) {
		if node.Type == html.ElementNode {
			for _, attribute := range node.Attr {
				if attribute.Key == "id" {
					ids[attribute.Val] = true
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			collect(child)
		}
	}
	collect(document)
	valid := true
	var check func(*html.Node)
	check = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "label" {
			for _, attribute := range node.Attr {
				if attribute.Key == "for" && !ids[attribute.Val] {
					valid = false
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			check(child)
		}
	}
	check(document)
	return valid
}

func hasAttr(node *html.Node, key, value string) bool {
	if node.Type == html.ElementNode {
		for _, attribute := range node.Attr {
			if attribute.Key == key && attribute.Val == value {
				return true
			}
		}
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if hasAttr(child, key, value) {
			return true
		}
	}
	return false
}

func countElementsWithAttr(node *html.Node, attribute string) int {
	count := 0
	if node.Type == html.ElementNode {
		for _, item := range node.Attr {
			if item.Key == attribute {
				count++
				break
			}
		}
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		count += countElementsWithAttr(child, attribute)
	}
	return count
}

func attrForElementWithText(node *html.Node, element, text, attribute string) string {
	if node.Type == html.ElementNode && node.Data == element && strings.TrimSpace(nodeText(node)) == text {
		for _, item := range node.Attr {
			if item.Key == attribute {
				return item.Val
			}
		}
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if value := attrForElementWithText(child, element, text, attribute); value != "" {
			return value
		}
	}
	return ""
}

func assertAnalyticsLink(t *testing.T, raw, wantPath string, want url.Values, paginated bool) {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Path != wantPath {
		t.Fatalf("link path = %q, want %q (%s)", parsed.Path, wantPath, raw)
	}
	for _, name := range []string{"from", "to", "client_id", "model_pool_id", "usage_available"} {
		if got := parsed.Query().Get(name); got != want.Get(name) {
			t.Fatalf("link %s = %q, want %q (%s)", name, got, want.Get(name), raw)
		}
	}
	if paginated {
		if parsed.Query().Get("limit") != "1" || parsed.Query().Get("offset") != "1" {
			t.Fatalf("pagination link query = %s, want limit=1 offset=1", parsed.RawQuery)
		}
	} else if parsed.Query().Has("limit") || parsed.Query().Has("offset") {
		t.Fatalf("export link unexpectedly contains pagination: %s", raw)
	}
}
