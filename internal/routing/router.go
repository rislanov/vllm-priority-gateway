package routing

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

var ErrNoBackend = errors.New("no eligible backend")

type Candidate struct {
	Backend         domain.Backend
	Pressure        float64
	GatewayInflight int
	Eligible        bool
}

type Source interface {
	Intn(int) int
}

type Router struct {
	epsilon float64
	source  Source
}

func New(epsilon float64, source Source) *Router {
	if epsilon < 0 || math.IsNaN(epsilon) || math.IsInf(epsilon, 0) {
		epsilon = 0
	}
	if source == nil {
		source = NewRandomSource(time.Now().UnixNano())
	}
	return &Router{epsilon: epsilon, source: source}
}

func (r *Router) Select(candidates []Candidate, exclude map[int64]struct{}) (Candidate, error) {
	eligible := make([]Candidate, 0, len(candidates))
	minimumPressure := math.Inf(1)
	for _, candidate := range candidates {
		if !candidate.Eligible || !candidate.Backend.Enabled || candidate.Backend.Draining ||
			math.IsNaN(candidate.Pressure) || math.IsInf(candidate.Pressure, 0) {
			continue
		}
		if _, excluded := exclude[candidate.Backend.ID]; excluded {
			continue
		}
		eligible = append(eligible, candidate)
		if candidate.Pressure < minimumPressure {
			minimumPressure = candidate.Pressure
		}
	}
	if len(eligible) == 0 {
		return Candidate{}, ErrNoBackend
	}

	pressureTies := eligible[:0]
	for _, candidate := range eligible {
		if candidate.Pressure-minimumPressure <= r.epsilon {
			pressureTies = append(pressureTies, candidate)
		}
	}
	minimumInflight := math.MaxInt
	for _, candidate := range pressureTies {
		if candidate.GatewayInflight < minimumInflight {
			minimumInflight = candidate.GatewayInflight
		}
	}
	finalists := pressureTies[:0]
	for _, candidate := range pressureTies {
		if candidate.GatewayInflight == minimumInflight {
			finalists = append(finalists, candidate)
		}
	}
	return finalists[r.source.Intn(len(finalists))], nil
}

type lockedSource struct {
	mu     sync.Mutex
	random *rand.Rand
}

func NewRandomSource(seed int64) Source {
	return &lockedSource{random: rand.New(rand.NewSource(seed))}
}

func (s *lockedSource) Intn(maximum int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.random.Intn(maximum)
}

type fixedSource int

func FixedSource(index int) Source {
	return fixedSource(index)
}

func (s fixedSource) Intn(maximum int) int {
	value := int(s) % maximum
	if value < 0 {
		value += maximum
	}
	return value
}
