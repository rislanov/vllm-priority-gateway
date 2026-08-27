package observability_test

import (
	"bytes"
	"context"
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

func TestMultiRollsBackEarlierReservationWhenLaterPeerRefuses(t *testing.T) {
	var calls []string
	first := &reservationObserver{name: "first", calls: &calls, accept: true}
	second := &reservationObserver{name: "second", calls: &calls, accept: true, refuseCanceled: true}
	third := &reservationObserver{name: "third", calls: &calls, accept: true}
	combined := observability.Multi(first, observability.NewLogger(nil), second, third)
	reserver, ok := combined.(gateway.ResponseCompleteReserver)
	if !ok {
		t.Fatal("Multi does not expose response-completion reservation capability")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rollback, reserved := reserver.ReserveResponseComplete(ctx, "request-1")
	if reserved {
		t.Fatal("reservation succeeded after a capable peer refused it")
	}
	if rollback != nil {
		t.Fatal("failed aggregate reservation returned a rollback handle")
	}
	if got := strings.Join(calls, ","); got != "reserve:first,reserve:second,rollback:first" {
		t.Fatalf("reservation lifecycle = %q", got)
	}

	calls = nil
	combined = observability.Multi(first, third)
	reserver = combined.(gateway.ResponseCompleteReserver)
	rollback, reserved = reserver.ReserveResponseComplete(context.Background(), "request-2")
	if !reserved || rollback == nil {
		t.Fatal("successful aggregate reservation did not return its rollback handle")
	}
	rollback()
	rollback()
	if got := strings.Join(calls, ","); got != "reserve:first,reserve:third,rollback:third,rollback:first" {
		t.Fatalf("successful reservation rollback lifecycle = %q", got)
	}
}

type responseCompleteObserver struct {
	mu         sync.Mutex
	requestIDs []string
}

type reservationObserver struct {
	name           string
	calls          *[]string
	accept         bool
	refuseCanceled bool
}

func (*reservationObserver) ClientInflight(gateway.InflightEvent, int)  {}
func (*reservationObserver) BackendInflight(gateway.InflightEvent, int) {}
func (*reservationObserver) Complete(gateway.RequestEvent)              {}

func (o *reservationObserver) ReserveResponseComplete(ctx context.Context, _ string) (func(), bool) {
	*o.calls = append(*o.calls, "reserve:"+o.name)
	if !o.accept || (o.refuseCanceled && ctx.Err() != nil) {
		return nil, false
	}
	return func() { *o.calls = append(*o.calls, "rollback:"+o.name) }, true
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
