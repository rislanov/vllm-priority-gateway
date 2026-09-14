package observability

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
)

// Logger writes one safe structured record for each completed public request.
type Logger struct {
	logger *slog.Logger
}

func NewLogger(logger *slog.Logger) *Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return &Logger{logger: logger}
}

func (*Logger) ClientInflight(gateway.InflightEvent, int)  {}
func (*Logger) BackendInflight(gateway.InflightEvent, int) {}

func (l *Logger) Complete(event gateway.RequestEvent) {
	l.logger.LogAttrs(context.Background(), slog.LevelInfo, "request completed",
		slog.String("requestId", event.RequestID),
		slog.String("parentRequestId", event.ParentRequestID),
		slog.String("client", event.Client),
		slog.String("model", event.Model),
		slog.String("backend", event.Backend),
		slog.String("priorityClass", string(event.PriorityClass)),
		slog.Int("priority", event.VLLMPriority),
		slog.String("poolState", string(event.PoolState)),
		slog.Float64("backendPressure", event.BackendPressure),
		slog.Int("status", event.Status),
		slog.String("reason", event.Reason),
		slog.Int64("durationMs", event.Duration.Milliseconds()),
		slog.Int64("ttftMs", event.TTFT.Milliseconds()),
		slog.Bool("disconnect", event.Disconnect),
		slog.Int("retryCount", event.RetryCount),
	)
}

// CoordinationOperation records bounded coordination metadata without request,
// lease, permit, client, or API-key identifiers.
func (l *Logger) CoordinationOperation(backend, operation, outcome string, duration time.Duration, reason coordination.Reason) {
	l.logger.LogAttrs(context.Background(), slog.LevelDebug, "coordination operation",
		slog.String("backend", backend),
		slog.String("operation", operation),
		slog.String("outcome", outcome),
		slog.String("reason", string(reason)),
		slog.Int64("durationMs", duration.Milliseconds()),
	)
}

func (l *Logger) CoordinationRecoveryTransition(from, to coordination.RecoveryState) {
	l.logger.LogAttrs(context.Background(), slog.LevelWarn, "coordination recovery transition",
		slog.String("backend", "postgres"),
		slog.String("from", string(from)),
		slog.String("to", string(to)),
	)
}

type coordinationObservers struct {
	values []coordination.OperationObserver
}

// CoordinationMulti combines bounded coordination observers in declaration order.
func CoordinationMulti(values ...coordination.OperationObserver) coordination.OperationObserver {
	combined := make([]coordination.OperationObserver, 0, len(values))
	for _, observer := range values {
		if observer != nil {
			combined = append(combined, observer)
		}
	}
	return &coordinationObservers{values: combined}
}

func (o *coordinationObservers) CoordinationOperation(backend, operation, outcome string, duration time.Duration, reason coordination.Reason) {
	for _, observer := range o.values {
		observer.CoordinationOperation(backend, operation, outcome, duration, reason)
	}
}

type observers struct {
	values []gateway.Observer
}

type reservedPeer struct {
	observer    gateway.ResponseCompleteObserver
	reservation gateway.ResponseCompleteReservation
	rollback    func()
}

type responseCompleteReservation struct {
	owner    *observers
	mu       sync.Mutex
	peers    []reservedPeer
	staged   bool
	finished bool
}

// Multi combines observers in declaration order and skips nil entries.
func Multi(values ...gateway.Observer) gateway.Observer {
	combined := make([]gateway.Observer, 0, len(values))
	for _, observer := range values {
		if observer != nil {
			combined = append(combined, observer)
		}
	}
	return &observers{values: combined}
}

func (o *observers) ClientInflight(event gateway.InflightEvent, delta int) {
	for _, observer := range o.values {
		observer.ClientInflight(event, delta)
	}
}

func (o *observers) BackendInflight(event gateway.InflightEvent, delta int) {
	for _, observer := range o.values {
		observer.BackendInflight(event, delta)
	}
}

func (o *observers) Complete(event gateway.RequestEvent) {
	for _, observer := range o.values {
		observer.Complete(event)
	}
}

func (o *observers) ResponseComplete(handle gateway.ResponseCompleteReservation) {
	reservation, ok := handle.(*responseCompleteReservation)
	if !ok || reservation.owner != o {
		return
	}
	reservation.complete()
}

func (o *observers) ReserveResponseComplete(
	ctx context.Context,
	requestID string,
) (gateway.ResponseCompleteReservation, func(), bool) {
	reservation := &responseCompleteReservation{owner: o, peers: make([]reservedPeer, 0, len(o.values))}
	for _, observer := range o.values {
		reserver, ok := observer.(gateway.ResponseCompleteReserver)
		if !ok {
			continue
		}
		peerReservation, rollback, reserved := reserver.ReserveResponseComplete(ctx, requestID)
		if !reserved || peerReservation == nil {
			if rollback != nil {
				rollback()
			}
			reservation.rollback()
			return nil, nil, false
		}
		if rollback == nil {
			rollback = func() {}
		}
		reservation.peers = append(reservation.peers, reservedPeer{
			observer: reserver, reservation: peerReservation, rollback: rollback,
		})
	}
	return reservation, reservation.rollback, true
}

func (r *responseCompleteReservation) StageResponseComplete(event gateway.RequestEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.staged || r.finished {
		return
	}
	r.staged = true
	for _, peer := range r.peers {
		peer.reservation.StageResponseComplete(event)
	}
}

func (r *responseCompleteReservation) complete() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return
	}
	r.finished = true
	for _, peer := range r.peers {
		peer.observer.ResponseComplete(peer.reservation)
	}
}

func (r *responseCompleteReservation) rollback() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return
	}
	r.finished = true
	for index := len(r.peers) - 1; index >= 0; index-- {
		r.peers[index].rollback()
	}
}
