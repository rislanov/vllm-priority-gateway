package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTokenizationPublicRouting(t *testing.T) {
	raw, key := testKey(t)
	h, _ := newFixture(t, fixtureOptions{client: enabledClient(), key: key})
	for _, tc := range []struct {
		path, method, key, body string
		status                  int
	}{{"/tokenize", "POST", "", `{}`, 401}, {"/tokenize", "POST", raw, `[]`, 400}, {"/tokenize", "POST", raw, `{"model":"public-model"}`, 503}, {"/tokenize", "GET", raw, `{}`, 404}, {"/v1/tokenize", "POST", raw, `{}`, 404}} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		if tc.key != "" {
			r.Header.Set("Authorization", "Bearer "+tc.key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Errorf("%s %s: %d body=%s", tc.method, tc.path, w.Code, w.Body)
		}
		if tc.path == "/tokenize" && tc.method == http.MethodPost && w.Header().Get("X-Request-Id") == "" {
			t.Fatal("request ID missing")
		}
	}
}
