package local_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/contracttest"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
)

func TestAdmissionContract(t *testing.T) {
	contracttest.Admission(t, func(clock *contracttest.Clock) coordination.AdmissionCoordinator {
		return local.NewAdmissionCoordinator(clock.Now)
	})
}

func TestAdmissionBackendNeutralContract(t *testing.T) {
	contracttest.AdmissionBaseline(t, func(_ *testing.T, rpm int64) (coordination.AdmissionCoordinator, func() coordination.AdmissionRequest) {
		coordinator := local.NewAdmissionCoordinator(time.Now)
		return coordinator, func() coordination.AdmissionRequest {
			return coordination.AdmissionRequest{
				LeaseID: uuid.New(), OperationStartedAt: time.Now().UTC(), RequestID: uuid.NewString(), ReplicaID: uuid.New(),
				ConfigurationRevision: 1, APIKeyID: 1, ClientID: 1, PoolID: 1, ClientPolicyRevision: 1,
				EffectiveClientLimit: 1, ConfiguredClientLimit: 1, PoolGatewayInflightLimit: 1,
				RequestsPerMinute: rpm, LeaseTTL: time.Minute,
			}
		}
	})
}

func TestCircuitContract(t *testing.T) {
	contracttest.Circuit(t, func(clock *contracttest.Clock) coordination.CircuitCoordinator {
		coordinator, err := local.NewCircuitCoordinator(circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Hour, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1}, clock.Now)
		if err != nil {
			t.Fatal(err)
		}
		return coordinator
	})
}

func TestCircuitBackendNeutralContract(t *testing.T) {
	contracttest.CircuitBaseline(t, func(t *testing.T) (coordination.CircuitCoordinator, coordination.BackendIdentity) {
		coordinator, err := local.NewCircuitCoordinator(circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Hour, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1}, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		return coordinator, coordination.BackendIdentity{ID: 1, Revision: 1, Enabled: true}
	})
}
