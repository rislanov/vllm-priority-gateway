package web_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/html"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
	"github.com/rislanov/vllm-priority-gateway/internal/web"
)

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
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	document, err := html.Parse(strings.NewReader(response.Body.String()))
	if err != nil {
		t.Fatal(err)
	}
	if !hasAttr(document, "id", "one-time-secret") || !strings.Contains(response.Body.String(), "llmgw_") {
		t.Fatalf("one-time secret region missing: %s", response.Body.String())
	}
}

func newWebFixture(t *testing.T) http.Handler {
	t.Helper()
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	if err := database.Migrate(context.Background()); err != nil {
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
		Store: database, Registry: registryValue, Runtime: webRuntime{},
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
