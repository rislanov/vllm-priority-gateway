package coordination

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

type ReasonError struct{ Reason Reason }

func (e ReasonError) Error() string { return fmt.Sprintf("coordination: %s", e.Reason) }

type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return "coordination consistency failure" }
func (e PermanentError) Unwrap() error { return e.Err }

type Reason string

const (
	ReasonConcurrencyExhausted    Reason = "concurrency_exhausted"
	ReasonRPMExhausted            Reason = "rpm_exhausted"
	ReasonTPMExhausted            Reason = "tpm_exhausted"
	ReasonStaleConfiguration      Reason = "stale_configuration"
	ReasonStaleBackend            Reason = "stale_backend"
	ReasonCircuitOpen             Reason = "circuit_open"
	ReasonProbeCapacityExhausted  Reason = "probe_capacity_exhausted"
	ReasonCoordinationUnavailable Reason = "coordination_unavailable"
	ReasonLeaseLost               Reason = "lease_lost"
	ReasonStaleOperation          Reason = "stale_operation"
	ReasonIdempotencyConflict     Reason = "idempotency_conflict"
)

type Status struct {
	Backend     string
	Available   bool
	Degraded    bool
	Permanent   bool
	LastSuccess time.Time
	Reason      Reason
}

type AdmissionRequest struct {
	LeaseID                  uuid.UUID
	OperationStartedAt       time.Time
	RequestID                string
	ReplicaID                uuid.UUID
	ConfigurationRevision    int64
	APIKeyID                 int64
	ClientID                 int64
	PoolID                   int64
	ClientPolicyRevision     int64
	EffectiveClientLimit     int
	ConfiguredClientLimit    int
	PoolGatewayInflightLimit int
	RequestsPerMinute        int64
	TokensPerMinute          int64
	LeaseTTL                 time.Duration
}

type LeaseIdentity struct {
	LeaseID   uuid.UUID
	ClientID  int64
	PoolID    int64
	RequestID string
	ReplicaID uuid.UUID
	ExpiresAt time.Time
	TTL       time.Duration
}

func (l LeaseIdentity) Identity() LeaseIdentity { return l }

type AdmissionDecision struct {
	Lease   *LeaseIdentity
	Reason  Reason
	RetryAt *time.Time
}

func (d AdmissionDecision) Admitted() bool { return d.Lease != nil && d.Reason == "" }

type RenewResult struct {
	Lease  LeaseIdentity
	Reason Reason
}
type TokenUsage struct {
	InputTokens     int64
	OutputTokens    int64
	CacheReadTokens int64
}

func (u TokenUsage) Valid() bool {
	return u.InputTokens >= 0 && u.OutputTokens >= 0 && u.CacheReadTokens >= 0 && u.CacheReadTokens <= u.InputTokens
}

type LeaseCompletion struct {
	Lease LeaseIdentity
	Usage *TokenUsage
}

type CompletionResult string

const (
	CompletionReleased         CompletionResult = "released"
	CompletionLeaseLost        CompletionResult = "lease_lost"
	CompletionAlreadyCompleted CompletionResult = "already_completed"
)

type CompleteResult struct {
	LeaseID        uuid.UUID
	Result         CompletionResult
	OriginalResult CompletionResult
	Reason         Reason
}

type AdmissionCoordinator interface {
	Acquire(context.Context, AdmissionRequest) (AdmissionDecision, error)
	Renew(context.Context, []LeaseIdentity) ([]RenewResult, error)
	// Complete applies completions in input order. When it returns an error,
	// results may contain the durably completed input prefix; callers must retry
	// only the remaining suffix.
	Complete(context.Context, []LeaseCompletion) ([]CompleteResult, error)
	Status() Status
}

type ClientRatePolicy struct {
	ClientID          int64
	Revision          int64
	RequestsPerMinute int64
	TokensPerMinute   int64
}

type AdmissionPolicyObserver interface {
	ObserveClientRatePolicy(ClientRatePolicy)
}

type AdmissionRuntime interface {
	PoolInflight(poolID int64) int
	RefreshInflight(context.Context) error
}

// CleanupBatcher removes bounded amounts of expired coordination state.
// More reports that a category filled its batch and may have further work.
type CleanupBatcher interface {
	CleanupBatch(context.Context, int) (more bool, err error)
}

type BackendIdentity struct {
	ID       int64
	Revision int64
	Enabled  bool
	Draining bool
}
type CircuitSnapshot struct {
	Backend        BackendIdentity
	State          domain.CircuitState
	Generation     int64
	FailureCount   int
	RetryAt        time.Time
	ProbesInFlight int
	Available      bool
}
type CircuitAcquireRequest struct {
	AttemptID            uuid.UUID
	AcquisitionStartedAt time.Time
	ReplicaID            uuid.UUID
	Backend              BackendIdentity
	ProbeTTL             time.Duration
}
type CircuitDecision struct {
	Snapshot CircuitSnapshot
	Permit   *ProbeIdentity
	Reason   Reason
}
type ProbeIdentity struct {
	PermitID   uuid.UUID
	Backend    BackendIdentity
	Generation int64
	ReplicaID  uuid.UUID
	ExpiresAt  time.Time
	TTL        time.Duration
}
type CircuitCompletion struct {
	AttemptID         uuid.UUID
	Backend           BackendIdentity
	Generation        int64
	Outcome           domain.InferenceOutcome
	ReportedOutcomeAt time.Time
}
type CircuitCoordinator interface {
	Reconcile(context.Context, []BackendIdentity) error
	Snapshot(backendID int64, at time.Time) CircuitSnapshot
	Acquire(context.Context, CircuitAcquireRequest) (CircuitDecision, error)
	Complete(context.Context, CircuitCompletion) (CircuitSnapshot, error)
	RenewProbes(context.Context, []ProbeIdentity) ([]RenewResult, error)
	Refresh(context.Context) error
	Status() Status
}
