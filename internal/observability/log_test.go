package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestLoggerIncludesDecisionTelemetry(t *testing.T) {
	var output bytes.Buffer
	logger := observability.NewLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	logger.Complete(gateway.RequestEvent{
		DecisionReason: gateway.DecisionPoolWaitingLimit,
		QueueOutcome:   gateway.QueueRejected,
		QueueWait:      12 * time.Millisecond,
	})

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("decode log record: %v", err)
	}
	if record["decisionReason"] != "pool_waiting_limit" || record["queueOutcome"] != "rejected" {
		t.Fatalf("decision log fields = %+v", record)
	}
	if record["queueWaitMs"] != float64(12) {
		t.Fatalf("queueWaitMs = %#v, want 12", record["queueWaitMs"])
	}
}

func TestMultiCompletesReservedPeersThroughProductionTerminalPath(t *testing.T) {
	var calls []string
	first := &reservationObserver{name: "first", calls: &calls, accept: true}
	second := &reservationObserver{name: "second", calls: &calls, accept: true}
	combined := observability.Multi(observability.NewLogger(nil), first, second)
	reserver, ok := combined.(gateway.ResponseCompleteReserver)
	if !ok {
		t.Fatal("Multi does not expose response-completion reservation capability")
	}
	reservation, rollback, reserved := reserver.ReserveResponseComplete(context.Background(), "request-1")
	if !reserved || reservation == nil || rollback == nil {
		t.Fatal("Multi did not return a complete reservation lifecycle")
	}
	reservation.StageResponseComplete(gateway.RequestEvent{RequestID: "request-1"})
	reserver.ResponseComplete(reservation)
	reserver.ResponseComplete(reservation)
	rollback()

	if got := strings.Join(calls, ","); got != "reserve:first,reserve:second,stage:first,stage:second,complete:first,complete:second" {
		t.Fatalf("reserved production terminal lifecycle = %q", got)
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
	reservation, rollback, reserved := reserver.ReserveResponseComplete(ctx, "request-1")
	if reserved {
		t.Fatal("reservation succeeded after a capable peer refused it")
	}
	if reservation != nil || rollback != nil {
		t.Fatal("failed aggregate reservation returned lifecycle handles")
	}
	if got := strings.Join(calls, ","); got != "reserve:first,reserve:second,rollback:first" {
		t.Fatalf("reservation lifecycle = %q", got)
	}

	calls = nil
	combined = observability.Multi(first, third)
	reserver = combined.(gateway.ResponseCompleteReserver)
	reservation, rollback, reserved = reserver.ReserveResponseComplete(context.Background(), "request-2")
	if !reserved || reservation == nil || rollback == nil {
		t.Fatal("successful aggregate reservation did not return its rollback handle")
	}
	rollback()
	rollback()
	if got := strings.Join(calls, ","); got != "reserve:first,reserve:third,rollback:third,rollback:first" {
		t.Fatalf("successful reservation rollback lifecycle = %q", got)
	}
}

func TestMultiReservationPanicUnwindsAcceptedPeersAndPreservesPanicIdentity(t *testing.T) {
	var calls []string
	panicValue := &struct{ message string }{message: "reserve panic"}
	first := &panicReservationObserver{name: "first", calls: &calls}
	second := &panicReservationObserver{name: "second", calls: &calls, rollbackPanic: "rollback panic"}
	third := &panicReservationObserver{name: "third", calls: &calls, reservePanic: panicValue}
	reserver := observability.Multi(first, second, third).(gateway.ResponseCompleteReserver)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _, _ = reserver.ReserveResponseComplete(context.Background(), "request-1")
	}()

	if recovered != panicValue {
		t.Fatalf("recovered panic = %#v, want original identity %#v", recovered, panicValue)
	}
	want := "reserve:first,reserve:second,reserve:third,rollback:second,rollback:first"
	if got := strings.Join(calls, ","); got != want {
		t.Fatalf("panic reservation lifecycle = %q, want %q", got, want)
	}
}

func TestMultiCompletionPanicRollsBackCurrentAndRemainingPeers(t *testing.T) {
	var calls []string
	panicValue := &struct{ message string }{message: "completion panic"}
	first := &panicReservationObserver{name: "first", calls: &calls}
	second := &panicReservationObserver{name: "second", calls: &calls, completePanic: panicValue}
	third := &panicReservationObserver{name: "third", calls: &calls, rollbackPanic: "rollback panic"}
	reserver := observability.Multi(first, second, third).(gateway.ResponseCompleteReserver)
	reservation, rollback, reserved := reserver.ReserveResponseComplete(context.Background(), "request-1")
	if !reserved || reservation == nil || rollback == nil {
		t.Fatal("Multi did not return the aggregate reservation")
	}
	reservation.StageResponseComplete(gateway.RequestEvent{RequestID: "request-1"})

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		reserver.ResponseComplete(reservation)
	}()
	if recovered != panicValue {
		t.Fatalf("recovered panic = %#v, want original identity %#v", recovered, panicValue)
	}
	reserver.ResponseComplete(reservation)
	rollback()

	want := strings.Join([]string{
		"reserve:first", "reserve:second", "reserve:third",
		"stage:first", "stage:second", "stage:third",
		"complete:first", "complete:second", "rollback:third", "rollback:second",
	}, ",")
	if got := strings.Join(calls, ","); got != want {
		t.Fatalf("panic completion lifecycle = %q, want %q", got, want)
	}
}

func TestMultiDoesNotReservePeerWithoutTerminalCapability(t *testing.T) {
	peer := &reservationOnlyObserver{}
	combined := observability.Multi(peer)
	reserver, ok := combined.(gateway.ResponseCompleteReserver)
	if !ok {
		t.Fatal("Multi does not expose response-completion reservation capability")
	}

	reservation, rollback, reserved := reserver.ReserveResponseComplete(context.Background(), "request-1")
	if !reserved || reservation == nil || rollback == nil {
		t.Fatal("aggregate without complete reservation peers did not return an inert reservation")
	}
	rollback()
	combined.Complete(gateway.RequestEvent{RequestID: "request-1"})

	if peer.reservations != 0 {
		t.Fatalf("reservation-only peer received %d reservation(s), want none", peer.reservations)
	}
	if peer.completions != 1 {
		t.Fatalf("reservation-only peer received %d ordinary completion(s), want one", peer.completions)
	}
}

type reservationObserver struct {
	name           string
	calls          *[]string
	accept         bool
	refuseCanceled bool
}

type reservationOnlyObserver struct {
	reservations int
	completions  int
}

type panicReservationObserver struct {
	name          string
	calls         *[]string
	reservePanic  any
	completePanic any
	rollbackPanic any
}

func (*panicReservationObserver) ClientInflight(gateway.InflightEvent, int)  {}
func (*panicReservationObserver) BackendInflight(gateway.InflightEvent, int) {}
func (*panicReservationObserver) Complete(gateway.RequestEvent)              {}

func (o *panicReservationObserver) ReserveResponseComplete(
	context.Context,
	string,
) (gateway.ResponseCompleteReservation, func(), bool) {
	*o.calls = append(*o.calls, "reserve:"+o.name)
	if o.reservePanic != nil {
		panic(o.reservePanic)
	}
	return &panicReservationProbe{owner: o}, func() {
		*o.calls = append(*o.calls, "rollback:"+o.name)
		if o.rollbackPanic != nil {
			panic(o.rollbackPanic)
		}
	}, true
}

func (o *panicReservationObserver) ResponseComplete(handle gateway.ResponseCompleteReservation) {
	reservation, ok := handle.(*panicReservationProbe)
	if !ok || reservation.owner != o {
		return
	}
	*o.calls = append(*o.calls, "complete:"+o.name)
	if o.completePanic != nil {
		panic(o.completePanic)
	}
}

type panicReservationProbe struct {
	owner *panicReservationObserver
}

func (r *panicReservationProbe) StageResponseComplete(gateway.RequestEvent) {
	*r.owner.calls = append(*r.owner.calls, "stage:"+r.owner.name)
}

func (*reservationOnlyObserver) ClientInflight(gateway.InflightEvent, int)  {}
func (*reservationOnlyObserver) BackendInflight(gateway.InflightEvent, int) {}
func (o *reservationOnlyObserver) Complete(gateway.RequestEvent) {
	o.completions++
}

func (o *reservationOnlyObserver) ReserveResponseComplete(
	context.Context,
	string,
) (gateway.ResponseCompleteReservation, func(), bool) {
	o.reservations++
	return inertResponseReservation{}, func() {}, true
}

func (*reservationObserver) ClientInflight(gateway.InflightEvent, int)  {}
func (*reservationObserver) BackendInflight(gateway.InflightEvent, int) {}
func (*reservationObserver) Complete(gateway.RequestEvent)              {}

func (o *reservationObserver) ReserveResponseComplete(
	ctx context.Context,
	_ string,
) (gateway.ResponseCompleteReservation, func(), bool) {
	*o.calls = append(*o.calls, "reserve:"+o.name)
	if !o.accept || (o.refuseCanceled && ctx.Err() != nil) {
		return nil, nil, false
	}
	return &reservationProbe{owner: o}, func() { *o.calls = append(*o.calls, "rollback:"+o.name) }, true
}

func (o *reservationObserver) ResponseComplete(handle gateway.ResponseCompleteReservation) {
	reservation, ok := handle.(*reservationProbe)
	if !ok || reservation.owner != o {
		return
	}
	*o.calls = append(*o.calls, "complete:"+o.name)
}

type reservationProbe struct {
	owner *reservationObserver
}

func (r *reservationProbe) StageResponseComplete(gateway.RequestEvent) {
	*r.owner.calls = append(*r.owner.calls, "stage:"+r.owner.name)
}

type inertResponseReservation struct{}

func (inertResponseReservation) StageResponseComplete(gateway.RequestEvent) {}
