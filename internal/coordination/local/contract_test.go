package local_test

import (
	"testing"
	"time"

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

func TestCircuitContract(t *testing.T) {
	contracttest.Circuit(t, func(clock *contracttest.Clock) coordination.CircuitCoordinator {
		coordinator, err := local.NewCircuitCoordinator(circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Hour, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1}, clock.Now)
		if err != nil {
			t.Fatal(err)
		}
		return coordinator
	})
}
