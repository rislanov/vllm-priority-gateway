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
	RequestID       string
	ParentRequestID string
	Client          string
	Model           string
	Backend         string
	PriorityClass   domain.PriorityClass
	VLLMPriority    int
	PoolState       domain.PoolState
	BackendPressure float64
	Status          int
	Reason          string
	Duration        time.Duration
	TTFT            time.Duration
	Disconnect      bool
	RetryCount      int
}

// Observer receives synchronous request-lifecycle events. Implementations
// should be lightweight and safe for concurrent use.
type Observer interface {
	ClientInflight(InflightEvent, int)
	BackendInflight(InflightEvent, int)
	Complete(RequestEvent)
}
