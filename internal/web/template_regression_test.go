package web_test

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
	"golang.org/x/net/html"
)

func TestTemplatesKeepNavigationAndDashboardMarkerScoped(t *testing.T) {
	handler := newWebFixture(t)
	for _, path := range []string{"/admin", "/admin/analytics", "/admin/clients", "/admin/keys", "/admin/backends"} {
		t.Run(path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			if response.Code != http.StatusOK {
				t.Fatalf("GET %s: %d", path, response.Code)
			}
			document, err := html.Parse(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			current := 0
			walkTemplateElements(document, func(node *html.Node) {
				if node.Data == "a" && nodeAttr(node, "aria-current") == "page" {
					current++
					if nodeAttr(node, "href") != path {
						t.Errorf("wrong current navigation link: %+v", node.Attr)
					}
				}
			})
			if current != 1 || countElementsWithAttr(document, "data-dashboard") != map[bool]int{true: 1, false: 0}[path == "/admin"] {
				t.Fatal("navigation or dashboard refresh scope changed")
			}
		})
	}
}

func TestTemplateControlCommentsDoNotReachRenderedPages(t *testing.T) {
	handler := newWebFixture(t)
	for _, path := range []string{"/admin", "/admin/analytics", "/admin/clients", "/admin/clients?edit=1", "/admin/keys", "/admin/backends", "/admin/backends?edit=1", "/admin/backends?edit_pool=1"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		body := response.Body.String()
		if response.Code != http.StatusOK || !strings.Contains(body, "</html>") {
			t.Errorf("%s: incomplete rendered page, status %d", path, response.Code)
		}
		if strings.Contains(body, "<!--") || strings.Contains(body, "-->") {
			t.Errorf("%s: template control comment leaked into HTML", path)
		}
	}
}

func TestClientPriorityEditorKeepsExactlyOneSelection(t *testing.T) {
	handler := newWebFixture(t)
	for _, priority := range []string{"critical", "high", "normal", "background"} {
		form := "action=update&id=1&name=payments&priority_class=" + priority + "&vllm_priority=0&max_concurrency=24&requests_per_minute=600&tokens_per_minute=60000&model_pool_id=1&enabled=on"
		request := httptest.NewRequest(http.MethodPost, "/admin/clients", strings.NewReader(form))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("update priority %s: %d", priority, response.Code)
		}
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/clients?edit=1", nil))
		document, err := html.Parse(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		selected := 0
		walkTemplateElements(document, func(node *html.Node) {
			if node.Data != "select" || nodeAttr(node, "name") != "priority_class" {
				return
			}
			for option := node.FirstChild; option != nil; option = option.NextSibling {
				if option.Type == html.ElementNode && option.Data == "option" && nodeHasAttr(option, "selected") {
					selected++
					if strings.TrimSpace(nodeText(option)) != priority {
						t.Errorf("priority %s selected %s", priority, nodeText(option))
					}
				}
			}
		})
		if selected != 1 {
			t.Errorf("priority %s: %d selected options, want one", priority, selected)
		}
	}
}

func TestEveryRenderedMutationFormCarriesCSRFToken(t *testing.T) {
	handler := newWebFixture(t)
	token := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	// Include a revoke form as well as the create form on the keys page.
	create := httptest.NewRequest(http.MethodPost, "/admin/keys", strings.NewReader("client_id=1&action=create"))
	create.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, create)
	if created.Code != http.StatusOK {
		t.Fatalf("create fixture key: %d", created.Code)
	}
	for _, path := range []string{"/admin/clients", "/admin/clients?edit=1", "/admin/keys", "/admin/backends", "/admin/backends?edit=1", "/admin/backends?edit_pool=1"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.AddCookie(&http.Cookie{Name: "llmgw_csrf", Value: token})
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		document, err := html.Parse(response.Body)
		if response.Code != http.StatusOK || err != nil {
			t.Fatalf("GET %s: %d, %v", path, response.Code, err)
		}
		forms := 0
		walkTemplateElements(document, func(node *html.Node) {
			if node.Data != "form" || nodeAttr(node, "method") != "post" {
				return
			}
			forms++
			if !hasInputWithAttr(node, "csrf_token", "value", token) {
				t.Errorf("%s: mutation form lacks actual request CSRF token", path)
			}
		})
		if forms == 0 {
			t.Errorf("%s: fixture rendered no mutation forms", path)
		}
	}
}

func TestAnalyticsSeriesKeepsAbsentCacheValuesDistinctFromZero(t *testing.T) {
	zero, ratio := int64(0), float64(0)
	start := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	queryStore := &dashboardQueryStoreStub{dataset: analytics.Dataset{Series: []analytics.SeriesPoint{
		{BucketStart: start},
		{BucketStart: start.Add(time.Minute), CacheReadTokens: &zero},
		{BucketStart: start.Add(2 * time.Minute), CacheHitRatio: &ratio},
		{BucketStart: start.Add(3 * time.Minute), CacheReadTokens: &zero, CacheHitRatio: &ratio},
	}}}
	handler := newWebFixtureWithQueryStore(t, nil, queryStore)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/analytics", nil))
	document, err := html.Parse(response.Body)
	if response.Code != http.StatusOK || err != nil {
		t.Fatalf("render series: %d, %v", response.Code, err)
	}
	index := 0
	walkTemplateElements(document, func(node *html.Node) {
		if !nodeHasAttr(node, "data-analytics-point") {
			return
		}
		if nodeHasAttr(node, "data-cache-read-tokens") != (index == 1 || index == 3) || nodeHasAttr(node, "data-cache-hit-ratio") != (index == 2 || index == 3) {
			t.Errorf("point %d changed cache availability: %+v", index, node.Attr)
		}
		for _, name := range []string{"data-cache-read-tokens", "data-cache-hit-ratio"} {
			if nodeHasAttr(node, name) && nodeAttr(node, name) != "0" {
				t.Errorf("point %d %s lost reported zero", index, name)
			}
		}
		index++
	})
	if index != 4 {
		t.Fatalf("rendered %d points, want 4", index)
	}
}

func walkTemplateElements(node *html.Node, visit func(*html.Node)) {
	if node.Type == html.ElementNode {
		visit(node)
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		walkTemplateElements(child, visit)
	}
}
