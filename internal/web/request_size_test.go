package web_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"golang.org/x/net/html"
)

func TestRequestSizeAdminForms(t *testing.T) {
	handler := newWebFixture(t)
	post := func(path string, form url.Values) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != http.StatusOK {
			t.Fatalf("%s form status = %d", path, response.Code)
		}
	}
	post("/admin/backends", url.Values{
		"action": {"create_pool"}, "public_model_name": {"bounded"}, "upstream_model_name": {"upstream"}, "enabled": {"on"},
		"request_size_policy": {"1"}, "request_size_enabled": {"on"}, "estimation_profile": {"qualified-v1"},
		"max_input_tokens": {"8192"}, "max_output_tokens": {"2048"}, "default_output_tokens": {"512"},
		"max_outstanding_tokens": {"32768"}, "background_max_output_tokens": {"256"},
	})
	post("/admin/backends", url.Values{
		"action": {"update_pool"}, "id": {"2"}, "public_model_name": {"renamed"}, "upstream_model_name": {"upstream"}, "enabled": {"on"},
	})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/backends?edit_pool=2", nil))
	doc, err := html.Parse(strings.NewReader(response.Body.String()))
	if err != nil {
		t.Fatal(err)
	}
	if !hasInput(doc, "max_outstanding_tokens", "number", "0", "32768") ||
		!hasInput(doc, "background_max_output_tokens", "number", "0", "256") ||
		!hasInputWithAttr(doc, "request_size_enabled", "checked", "") ||
		!strings.Contains(response.Body.String(), "Emergency outstanding tokens per replica (0 inherits pool limit)") {
		t.Fatal("pool edit form did not retain the enabled token budget and Background override")
	}
	post("/admin/clients", url.Values{
		"action": {"create"}, "name": {"bounded-client"}, "enabled": {"on"}, "priority_class": {"background"},
		"vllm_priority": {"0"}, "max_concurrency": {"8"}, "requests_per_minute": {"0"}, "tokens_per_minute": {"0"},
		"model_pool_id": {"1", "2"}, "max_input_tokens": {"1024"}, "max_output_tokens": {"0"},
	})
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/clients?edit=2", nil))
	for _, content := range []string{
		"Saved effective token limits by model",
		"Input max 1024, output max 256, output default 256.",
		"Request size admission disabled.",
	} {
		if !strings.Contains(response.Body.String(), content) {
			t.Errorf("client edit form missing %q", content)
		}
	}
}
