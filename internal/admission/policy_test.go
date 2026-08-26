package admission_test

import (
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestEffectiveLimitUsesPriorityPolicy(t *testing.T) {
	tests := []struct {
		name  string
		class domain.PriorityClass
		state domain.PoolState
		max   int
		want  int
	}{
		{name: "normal background full", class: domain.PriorityBackground, state: domain.PoolNormal, max: 21, want: 21},
		{name: "busy background half floor", class: domain.PriorityBackground, state: domain.PoolBusy, max: 21, want: 10},
		{name: "saturated background zero", class: domain.PriorityBackground, state: domain.PoolSaturated, max: 20, want: 0},
		{name: "saturated normal half floor", class: domain.PriorityNormal, state: domain.PoolSaturated, max: 3, want: 1},
		{name: "emergency normal zero", class: domain.PriorityNormal, state: domain.PoolEmergency, max: 20, want: 0},
		{name: "emergency high half floor", class: domain.PriorityHigh, state: domain.PoolEmergency, max: 1, want: 0},
		{name: "emergency critical full", class: domain.PriorityCritical, state: domain.PoolEmergency, max: 20, want: 20},
		{name: "unavailable rejects all", class: domain.PriorityCritical, state: domain.PoolUnavailable, max: 20, want: 0},
		{name: "negative max clamps to zero", class: domain.PriorityCritical, state: domain.PoolNormal, max: -1, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := admission.EffectiveLimit(tt.class, tt.state, tt.max); got != tt.want {
				t.Fatalf("EffectiveLimit() = %d, want %d", got, tt.want)
			}
		})
	}
}
