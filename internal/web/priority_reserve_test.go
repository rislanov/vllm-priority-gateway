package web_test

import (
	"golang.org/x/net/html"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPoolPriorityReserveForms(t *testing.T) {
	h := newWebFixture(t)
	get := func(path string) *html.Node {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		d, e := html.Parse(strings.NewReader(w.Body.String()))
		if e != nil {
			t.Fatal(e)
		}
		return d
	}
	if !hasInput(get("/admin/backends"), "high_priority_reserve", "number", "0", "0") {
		t.Fatal("reserve create control missing")
	}
	post := func(reserve string) *httptest.ResponseRecorder {
		t.Helper()
		form := "action=update_pool&id=1&public_model_name=qwen-72b&upstream_model_name=upstream&enabled=on&max_gateway_inflight=20&high_priority_reserve=" + reserve
		req := httptest.NewRequest(http.MethodPost, "/admin/backends", strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	if w := post("6"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if !hasInput(get("/admin/backends?edit_pool=1"), "high_priority_reserve", "number", "0", "6") {
		t.Fatal("reserve edit value lost")
	}
	for _, r := range []string{"-1", "21", "1.5", "many"} {
		w := post(r)
		if !strings.Contains(w.Body.String(), "must") {
			t.Fatalf("invalid reserve %s accepted: %s", r, w.Body.String())
		}
	}
	if !hasInput(get("/admin/backends?edit_pool=1"), "high_priority_reserve", "number", "0", "6") {
		t.Fatal("invalid write changed reserve")
	}
}
