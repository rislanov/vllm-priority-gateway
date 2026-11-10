package web_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"golang.org/x/net/html"
)

func TestRequestSizePoolFormPreservesPolicyOnLegacyUpdate(t *testing.T) {
	handler := newWebFixture(t)
	post := func(form url.Values) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/admin/backends", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != http.StatusOK {
			t.Fatalf("pool form status = %d", response.Code)
		}
	}
	post(url.Values{
		"action": {"create_pool"}, "public_model_name": {"bounded"}, "upstream_model_name": {"upstream"}, "enabled": {"on"},
		"request_size_policy": {"1"}, "request_size_enabled": {"on"}, "estimation_profile": {"qualified-v1"},
		"max_input_tokens": {"8192"}, "max_output_tokens": {"2048"}, "default_output_tokens": {"512"},
		"max_outstanding_tokens": {"32768"}, "background_max_output_tokens": {"256"},
	})
	post(url.Values{
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
		!hasInputWithAttr(doc, "request_size_enabled", "checked", "") {
		t.Fatal("pool edit form did not retain the enabled token budget and Background override")
	}
}
