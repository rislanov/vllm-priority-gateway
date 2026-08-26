package observability_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/observability"
)

func TestStructuredLoggerWritesSafeCompletionRecord(t *testing.T) {
	var output bytes.Buffer
	logger := observability.NewLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	logger.Complete(gateway.RequestEvent{
		RequestID: "gateway-id", ParentRequestID: "parent-id", Client: "client-a", Model: "qwen",
		Backend: "gpu-1", PriorityClass: domain.PriorityHigh, VLLMPriority: -10,
		PoolState: domain.PoolBusy, BackendPressure: .72, Status: 200,
		Duration: 2 * time.Second, TTFT: 100 * time.Millisecond, RetryCount: 1,
	})
	text := output.String()
	for _, field := range []string{
		`"requestId":"gateway-id"`, `"client":"client-a"`, `"model":"qwen"`,
		`"backend":"gpu-1"`, `"priority":-10`, `"status":200`, `"retryCount":1`,
	} {
		if !strings.Contains(text, field) {
			t.Fatalf("log missing %s: %s", field, text)
		}
	}
	for _, forbidden := range []string{"prompt", "generated_content", "authorization", "api_key"} {
		if strings.Contains(strings.ToLower(text), forbidden) {
			t.Fatalf("unsafe field %q found in log: %s", forbidden, text)
		}
	}
}
