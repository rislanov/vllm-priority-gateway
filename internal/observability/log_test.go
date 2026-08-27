package observability_test

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
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

func TestMultiPropagatesResponseCompleteToCapablePeers(t *testing.T) {
	peer := &responseCompleteObserver{}
	combined := observability.Multi(observability.NewLogger(nil), peer)
	notifier, ok := combined.(gateway.ResponseCompleteObserver)
	if !ok {
		t.Fatal("Multi does not expose response-complete capability")
	}
	notifier.ResponseComplete("request-1")
	notifier.ResponseComplete("request-2")
	if got := peer.RequestIDs(); strings.Join(got, ",") != "request-1,request-2" {
		t.Fatalf("response-complete request IDs = %v", got)
	}
}

type responseCompleteObserver struct {
	mu         sync.Mutex
	requestIDs []string
}

func (*responseCompleteObserver) ClientInflight(gateway.InflightEvent, int)  {}
func (*responseCompleteObserver) BackendInflight(gateway.InflightEvent, int) {}
func (*responseCompleteObserver) Complete(gateway.RequestEvent)              {}

func (o *responseCompleteObserver) ResponseComplete(requestID string) {
	o.mu.Lock()
	o.requestIDs = append(o.requestIDs, requestID)
	o.mu.Unlock()
}

func (o *responseCompleteObserver) RequestIDs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.requestIDs...)
}
