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
		slog.Int64("durationMs", event.Duration.Milliseconds()),
		slog.Int64("ttftMs", event.TTFT.Milliseconds()),
		slog.Bool("disconnect", event.Disconnect),
		slog.Int("retryCount", event.RetryCount),
	)
}

type observers []gateway.Observer

// Multi combines observers in declaration order and skips nil entries.
func Multi(values ...gateway.Observer) gateway.Observer {
	combined := make(observers, 0, len(values))
	for _, observer := range values {
		if observer != nil {
			combined = append(combined, observer)
		}
	}
	return combined
}

func (o observers) ClientInflight(event gateway.InflightEvent, delta int) {
	for _, observer := range o {
		observer.ClientInflight(event, delta)
	}
}

func (o observers) BackendInflight(event gateway.InflightEvent, delta int) {
	for _, observer := range o {
		observer.BackendInflight(event, delta)
	}
}

func (o observers) Complete(event gateway.RequestEvent) {
	for _, observer := range o {
		observer.Complete(event)
	}
}

func (o observers) ResponseComplete(requestID string) {
	for _, observer := range o {
		if responseObserver, ok := observer.(gateway.ResponseCompleteObserver); ok {
			responseObserver.ResponseComplete(requestID)
		}
	}
}

func (o observers) ReserveResponseComplete(ctx context.Context, requestID string) (func(), bool) {
	rollbacks := make([]func(), 0, len(o))
	for _, observer := range o {
		reserver, ok := observer.(gateway.ResponseCompleteReserver)
		if !ok {
			continue
		}
		rollback, reserved := reserver.ReserveResponseComplete(ctx, requestID)
		if !reserved {
			for index := len(rollbacks) - 1; index >= 0; index-- {
				rollbacks[index]()
			}
			return nil, false
		}
		if rollback == nil {
			rollback = func() {}
		}
		rollbacks = append(rollbacks, rollback)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for index := len(rollbacks) - 1; index >= 0; index-- {
				rollbacks[index]()
			}
		})
	}, true
}
