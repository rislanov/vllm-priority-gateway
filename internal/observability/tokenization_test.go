package observability

import (
	"bytes"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTokenizationMetricsAreSeparate(t *testing.T) {
	m := NewMetrics()
	var logs bytes.Buffer
	observer := m.NewTokenizationObserver(slog.New(slog.NewJSONHandler(&logs, nil)), func() *registry.Snapshot { return &registry.Snapshot{} }, nil)
	observer.TokenizationComplete(gateway.TokenizationEvent{RequestID: "id", Status: 401, Reason: "invalid_api_key", Duration: time.Millisecond})
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	if !strings.Contains(body, `llmgw_tokenize_requests_total{backend="",model="",status="401"} 1`) || !strings.Contains(body, `llmgw_tokenize_rejections_total{reason="invalid_api_key"} 1`) {
		t.Fatal(body)
	}
	if strings.Contains(body, "llmgw_requests_total{") || strings.Contains(body, "llmgw_input_tokens_total{") {
		t.Fatal("inference metrics touched")
	}
	if !strings.Contains(logs.String(), `"event":"tokenization_complete"`) {
		t.Fatal(logs.String())
	}
}
