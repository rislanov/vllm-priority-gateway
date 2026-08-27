package gateway

import (
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

// ResponseCompleteObserver receives a signal after the downstream response is
// fully written. Implementations may apply post-response backpressure here.
type ResponseCompleteObserver interface {
	ResponseComplete(requestID string)
}
