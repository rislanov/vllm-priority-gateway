package coordination

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"time"
)

const (
	TokenizationClientLimit        = "client_concurrency"
	TokenizationBackendLimit       = "backend_capacity"
	TokenizationRateLimit          = "client_rate"
	TokenizationUnavailable        = "coordination_unavailable"
	TokenizationStaleConfiguration = "stale_configuration"
	TokenizationStaleOperation     = "stale_operation"
	TokenizationConflict           = "idempotency_conflict"
	TokenizationLeaseLost          = "lease_lost"
)

type TokenizationPolicy struct {
	ClientMaxInflight  int
	BackendMaxInflight int
	ClientRPM          int64
	Timeout            time.Duration
}

func DefaultTokenizationPolicy() TokenizationPolicy {
	return TokenizationPolicy{4, 8, 120, 30 * time.Second}
}
func (p TokenizationPolicy) Validate() error {
	if p.ClientMaxInflight < 1 || p.ClientMaxInflight > 10000 || p.BackendMaxInflight < 1 || p.BackendMaxInflight > 10000 || p.ClientRPM < 1 || p.ClientRPM > 10000000 || p.Timeout <= 0 || p.Timeout > 5*time.Minute {
		return fmt.Errorf("invalid tokenization policy")
	}
	return nil
}
func (p TokenizationPolicy) LeaseTTL() time.Duration { return p.Timeout + 15*time.Second }
func (p TokenizationPolicy) Burst() float64          { return float64(min(p.ClientRPM, 10)) }

type TokenizationCandidate struct{ BackendID, BackendRevision int64 }
type TokenizationAcquireRequest struct {
	OperationID, ReplicaID                                          uuid.UUID
	RequestID                                                       string
	OperationStartedAt                                              time.Time
	ConfigurationRevision, APIKeyID, ClientID, PoolID, PoolRevision int64
	Candidates                                                      []TokenizationCandidate
}

func (r TokenizationAcquireRequest) Fingerprint() [32]byte {
	b, _ := json.Marshal(r)
	return sha256.Sum256(b)
}
func (r TokenizationAcquireRequest) ValidAt(now time.Time) bool {
	return r.OperationID != uuid.Nil && r.ReplicaID != uuid.Nil && r.ClientID > 0 && r.PoolID > 0 && r.OperationStartedAt.After(now.Add(-24*time.Hour)) && !r.OperationStartedAt.After(now.Add(5*time.Minute))
}

type TokenizationLease struct {
	OperationID, ReplicaID                       uuid.UUID
	ClientID, PoolID, BackendID, BackendRevision int64
	AcquiredAt, ExpiresAt                        time.Time
}
type TokenizationDecision struct {
	Lease   *TokenizationLease
	Reason  string
	RetryAt *time.Time
}

// Matches compares wire identities, ignoring time.Time's process-local location
// and monotonic-clock representation, which are not preserved in JSON/SQL.
func (l TokenizationLease) Matches(other TokenizationLease) bool {
	return l.OperationID == other.OperationID && l.ReplicaID == other.ReplicaID && l.ClientID == other.ClientID && l.PoolID == other.PoolID && l.BackendID == other.BackendID && l.BackendRevision == other.BackendRevision && l.AcquiredAt.Equal(other.AcquiredAt) && l.ExpiresAt.Equal(other.ExpiresAt)
}

type TokenizationCoordinator interface {
	Acquire(context.Context, TokenizationAcquireRequest) (TokenizationDecision, error)
	Complete(context.Context, TokenizationLease) error
	Status() Status
}
