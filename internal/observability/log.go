package observability

import (
	"context"
	"log/slog"
	"sync"

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
		slog.String("decisionReason", string(event.DecisionReason)),
		slog.String("queueOutcome", string(event.QueueOutcome)),
		slog.Float64("queueWaitMs", float64(event.QueueWait.Microseconds())/1000),
		slog.Int64("durationMs", event.Duration.Milliseconds()),
		slog.Int64("ttftMs", event.TTFT.Milliseconds()),
		slog.Bool("disconnect", event.Disconnect),
		slog.Int("retryCount", event.RetryCount),
	)
}

type observers struct {
	values []gateway.Observer
}

type reservedPeer struct {
	observer    gateway.ResponseCompleteObserver
	reservation gateway.ResponseCompleteReservation
	rollback    func()
	state       reservedPeerState
}

type reservedPeerState uint8

const (
	peerPending reservedPeerState = iota
	peerCompleting
	peerCompleted
	peerRolledBack
)

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
	defer func() {
		panicValue := recover()
		if panicValue == nil {
			return
		}
		reservation.rollbackAfterPanic()
		panic(panicValue)
	}()
	for _, observer := range o.values {
		reserver, ok := observer.(gateway.ResponseCompleteReserver)
		if !ok {
			continue
		}
		peerReservation, rollback, reserved := reserver.ReserveResponseComplete(ctx, requestID)
		if !reserved || peerReservation == nil {
			rollbacks := make([]func(), 0, len(reservation.peers)+1)
			if rollback != nil {
				rollbacks = append(rollbacks, rollback)
			}
			rollbacks = append(rollbacks, reservation.takeRollbackActions()...)
			if panicValue := invokeRollbackActions(rollbacks); panicValue != nil {
				panic(panicValue)
			}
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
	if r.staged || r.finished {
		r.mu.Unlock()
		return
	}
	r.staged = true
	peers := append([]reservedPeer(nil), r.peers...)
	r.mu.Unlock()
	for _, peer := range peers {
		peer.reservation.StageResponseComplete(event)
	}
}

func (r *responseCompleteReservation) complete() {
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return
	}
	r.finished = true
	r.mu.Unlock()

	for index := range r.peers {
		r.mu.Lock()
		if r.peers[index].state != peerPending {
			r.mu.Unlock()
			continue
		}
		r.peers[index].state = peerCompleting
		peer := r.peers[index]
		r.mu.Unlock()

		panicValue := invokeResponseComplete(peer)
		if panicValue != nil {
			_ = invokeRollbackActions(r.takeRollbackActionsFrom(index))
			panic(panicValue)
		}

		r.mu.Lock()
		if r.peers[index].state == peerCompleting {
			r.peers[index].state = peerCompleted
		}
		r.mu.Unlock()
	}
}

func (r *responseCompleteReservation) rollback() {
	if panicValue := invokeRollbackActions(r.takeRollbackActions()); panicValue != nil {
		panic(panicValue)
	}
}

func (r *responseCompleteReservation) rollbackAfterPanic() {
	_ = invokeRollbackActions(r.takeRollbackActions())
}

func (r *responseCompleteReservation) takeRollbackActions() []func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return nil
	}
	r.finished = true
	return r.takeRollbackActionsLocked(0)
}

func (r *responseCompleteReservation) takeRollbackActionsFrom(start int) []func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.takeRollbackActionsLocked(start)
}

func (r *responseCompleteReservation) takeRollbackActionsLocked(start int) []func() {
	rollbacks := make([]func(), 0, len(r.peers)-start)
	for index := len(r.peers) - 1; index >= start; index-- {
		if r.peers[index].state != peerPending && r.peers[index].state != peerCompleting {
			continue
		}
		r.peers[index].state = peerRolledBack
		rollbacks = append(rollbacks, r.peers[index].rollback)
	}
	return rollbacks
}

func invokeResponseComplete(peer reservedPeer) (panicValue any) {
	defer func() { panicValue = recover() }()
	peer.observer.ResponseComplete(peer.reservation)
	return nil
}

func invokeRollbackActions(rollbacks []func()) (firstPanic any) {
	for _, rollback := range rollbacks {
		if panicValue := invokeRollback(rollback); panicValue != nil && firstPanic == nil {
			firstPanic = panicValue
		}
	}
	return firstPanic
}

func invokeRollback(rollback func()) (panicValue any) {
	defer func() { panicValue = recover() }()
	rollback()
	return nil
}
