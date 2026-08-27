package gateway

import (
	"context"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

// InflightEvent contains the bounded dimensions used by inflight gauges.
type InflightEvent struct {
	Client        string
	Model         string
	Backend       string
	PriorityClass domain.PriorityClass
}

// RequestEvent describes one completed public inference request. It must never
// contain credentials, prompts, generated content, or raw request bodies.
type RequestEvent struct {
	OccurredAt        time.Time
	RequestID         string
	ParentRequestID   string
	ClientID          int64
	ModelPoolID       int64
	Client            string
	Model             string
	Backend           string
	PriorityClass     domain.PriorityClass
	VLLMPriority      int
	PoolState         domain.PoolState
	BackendPressure   float64
	Status            int
	Reason            string
	Duration          time.Duration
	TTFT              time.Duration
	Disconnect        bool
	RetryCount        int
	Usage             *domain.TokenUsage
	UsageParseFailure string
}

// Observer receives synchronous request-lifecycle events. Implementations
// should be lightweight and safe for concurrent use.
type Observer interface {
	ClientInflight(InflightEvent, int)
	BackendInflight(InflightEvent, int)
	Complete(RequestEvent)
}

// ResponseCompleteObserver receives a terminal signal after the handler or
// proxy finishes writing. It runs before ServeHTTP returns, so implementations
// must not wait for response-finalization capacity here.
type ResponseCompleteObserver interface {
	ResponseComplete(requestID string)
}

// ResponseCompleteReserver applies cancellable backpressure before response
// generation. A successful reservation returns an idempotent rollback that
// releases only that reservation if the aggregate lifecycle cannot proceed.
type ResponseCompleteReserver interface {
	ReserveResponseComplete(ctx context.Context, requestID string) (rollback func(), ok bool)
}
