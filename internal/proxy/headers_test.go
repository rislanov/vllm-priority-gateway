package proxy_test

import (
	"net/http"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
)

func TestPrepareUpstreamHeadersOverwritesSecuritySensitiveHeaders(t *testing.T) {
	in := http.Header{
		"X-Vllm-Priority": {"-999"},
		"X-Request-Id":    {"client"},
		"Authorization":   {"Bearer client-key"},
		"Connection":      {"keep-alive, X-Remove-Me"},
		"X-Remove-Me":     {"secret"},
		"X-End-To-End":    {"kept"},
	}
	got := proxy.PrepareUpstreamHeaders(in, "gateway-id", -10, "upstream-secret")
	if got.Get("X-Vllm-Priority") != "-10" || got.Get("X-Request-Id") != "gateway-id" {
		t.Fatalf("priority/request ID headers = %v", got)
	}
	if got.Get("Authorization") != "Bearer upstream-secret" {
		t.Fatalf("Authorization = %q", got.Get("Authorization"))
	}
	if got.Get("Connection") != "" || got.Get("X-Remove-Me") != "" {
		t.Fatalf("hop-by-hop headers leaked: %v", got)
	}
	if got.Get("X-End-To-End") != "kept" {
		t.Fatalf("end-to-end header was removed: %v", got)
	}
	if in.Get("Authorization") != "Bearer client-key" {
		t.Fatal("input headers were mutated")
	}
}

func TestPrepareUpstreamHeadersRemovesAuthorizationWithoutUpstreamKey(t *testing.T) {
	got := proxy.PrepareUpstreamHeaders(http.Header{"Authorization": {"Bearer client-key"}}, "id", 0, "")
	if got.Get("Authorization") != "" {
		t.Fatalf("Authorization = %q", got.Get("Authorization"))
	}
}

func TestCopyResponseHeadersRemovesHopByHopTokens(t *testing.T) {
	from := http.Header{
		"Connection":        {"X-Private"},
		"X-Private":         {"remove"},
		"Transfer-Encoding": {"chunked"},
		"Content-Type":      {"text/event-stream"},
	}
	to := make(http.Header)
	proxy.CopyResponseHeaders(to, from)
	if to.Get("Connection") != "" || to.Get("X-Private") != "" || to.Get("Transfer-Encoding") != "" {
		t.Fatalf("hop-by-hop response headers leaked: %v", to)
	}
	if to.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("Content-Type = %q", to.Get("Content-Type"))
	}
}
