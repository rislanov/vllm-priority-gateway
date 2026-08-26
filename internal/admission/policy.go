package admission

import (
	"math"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func EffectiveLimit(class domain.PriorityClass, state domain.PoolState, maximum int) int {
	if maximum <= 0 {
		return 0
	}
	fraction := admittedFraction(class, state)
	limit := int(math.Floor(float64(maximum) * fraction))
	if limit < 0 {
		return 0
	}
	if limit > maximum {
		return maximum
	}
	return limit
}

func admittedFraction(class domain.PriorityClass, state domain.PoolState) float64 {
	switch state {
	case domain.PoolNormal:
		return 1
	case domain.PoolBusy:
		if class == domain.PriorityBackground {
			return .5
		}
		return 1
	case domain.PoolSaturated:
		switch class {
		case domain.PriorityCritical, domain.PriorityHigh:
			return 1
		case domain.PriorityNormal:
			return .5
		default:
			return 0
		}
	case domain.PoolEmergency:
		switch class {
		case domain.PriorityCritical:
			return 1
		case domain.PriorityHigh:
			return .5
		default:
			return 0
		}
	default:
		return 0
	}
}
