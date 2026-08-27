package domain

import "time"

type PriorityClass string

type InferenceOutcome string

const (
	InferenceSuccess InferenceOutcome = "success"
	InferenceFailure InferenceOutcome = "failure"
	InferenceNeutral InferenceOutcome = "neutral"
)

type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

const (
	PriorityCritical   PriorityClass = "critical"
	PriorityHigh       PriorityClass = "high"
	PriorityNormal     PriorityClass = "normal"
	PriorityBackground PriorityClass = "background"
)

func (p PriorityClass) Valid() bool {
	switch p {
	case PriorityCritical, PriorityHigh, PriorityNormal, PriorityBackground:
		return true
	default:
		return false
	}
}

type PoolState string

const (
	PoolNormal      PoolState = "normal"
	PoolBusy        PoolState = "busy"
	PoolSaturated   PoolState = "saturated"
	PoolEmergency   PoolState = "emergency"
	PoolUnavailable PoolState = "unavailable"
)

type BackendState string

const (
	BackendHealthy   BackendState = "healthy"
	BackendBusy      BackendState = "busy"
	BackendSaturated BackendState = "saturated"
	BackendDraining  BackendState = "draining"
	BackendUnhealthy BackendState = "unhealthy"
)

type Client struct {
	ID             int64
	Name           string
	Enabled        bool
	PriorityClass  PriorityClass
	VLLMPriority   int
	MaxConcurrency int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type APIKey struct {
	ID         int64
	ClientID   int64
	Prefix     string
	SecretHash [32]byte
	CreatedAt  time.Time
	ExpiresAt  *time.Time
	RevokedAt  *time.Time
	LastUsedAt *time.Time
}

type ClientModelAccess struct {
	ClientID    int64
	ModelPoolID int64
	Enabled     bool
}

type ModelPool struct {
	ID                 int64
	PublicModelName    string
	UpstreamModelName  string
	Enabled            bool
	MaxGatewayInflight int
	MaxWaiting         int
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type Backend struct {
	ID                int64
	ModelPoolID       int64
	Name              string
	BaseURL           string
	Enabled           bool
	Draining          bool
	CapacityHint      float64
	RunningSoftLimit  float64
	UpstreamAPIKeyEnv string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type BackendRuntime struct {
	BackendID             int64
	State                 BackendState
	CircuitState          CircuitState
	CircuitFailures       int
	CircuitRetryAt        time.Time
	CircuitProbesInFlight int
	CircuitAvailable      bool
	Healthy               bool
	MetricsFresh          bool
	Running               float64
	Waiting               float64
	KVCacheUsage          float64
	RawPressure           float64
	Pressure              float64
	GatewayInflight       int
	LastHealthAt          time.Time
	LastMetricsAt         time.Time
	ConsecutiveFailure    int
	ConsecutiveSuccess    int
}

type PoolRuntime struct {
	PoolID              int64
	State               PoolState
	BestBackendPressure float64
	AvailableBackends   int
	AllBackendsWaiting  bool
}
