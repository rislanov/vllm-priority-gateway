package circuitbreaker

import (
	"fmt"
	"sync"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

type Options struct {
	FailureThreshold  int
	FailureWindow     time.Duration
	OpenCooldown      time.Duration
	HalfOpenMaxProbes int
}

type Snapshot struct {
	State          domain.CircuitState
	FailureCount   int
	RetryAt        time.Time
	ProbesInFlight int
	Available      bool
}

type Breaker struct {
	mu             sync.Mutex
	options        Options
	state          domain.CircuitState
	failures       []time.Time
	openedAt       time.Time
	probesInFlight int
}

func New(options Options) (*Breaker, error) {
	if options.FailureThreshold <= 0 {
		return nil, fmt.Errorf("failure threshold must be positive")
	}
	if options.FailureWindow <= 0 {
		return nil, fmt.Errorf("failure window must be positive")
	}
	if options.OpenCooldown <= 0 {
		return nil, fmt.Errorf("open cooldown must be positive")
	}
	if options.HalfOpenMaxProbes <= 0 {
		return nil, fmt.Errorf("half-open max probes must be positive")
	}
	return &Breaker{options: options, state: domain.CircuitClosed}, nil
}

func (b *Breaker) Acquire(now time.Time) (complete func(domain.InferenceOutcome), ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.transitionOpenToHalfOpen(now)
	if b.state == domain.CircuitClosed {
		b.pruneFailures(now)
		return b.completion(now, false), true
	}
	if b.state != domain.CircuitHalfOpen {
		return nil, false
	}

	if b.probesInFlight >= b.options.HalfOpenMaxProbes {
		return nil, false
	}
	b.probesInFlight++
	return b.completion(now, true), true
}

func (b *Breaker) Snapshot(now time.Time) Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.transitionOpenToHalfOpen(now)
	if b.state == domain.CircuitClosed {
		b.pruneFailures(now)
	}

	snapshot := Snapshot{
		State:          b.state,
		FailureCount:   len(b.failures),
		ProbesInFlight: b.probesInFlight,
	}
	switch b.state {
	case domain.CircuitClosed:
		snapshot.Available = true
	case domain.CircuitOpen:
		snapshot.RetryAt = b.openedAt.Add(b.options.OpenCooldown)
	case domain.CircuitHalfOpen:
		snapshot.Available = b.probesInFlight < b.options.HalfOpenMaxProbes
	}
	return snapshot
}

func (b *Breaker) completion(now time.Time, probe bool) func(domain.InferenceOutcome) {
	var once sync.Once
	return func(outcome domain.InferenceOutcome) {
		once.Do(func() {
			b.complete(now, probe, outcome)
		})
	}
}

func (b *Breaker) complete(now time.Time, probe bool, outcome domain.InferenceOutcome) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if probe {
		b.completeProbe(now, outcome)
		return
	}
	if b.state != domain.CircuitClosed || outcome != domain.InferenceFailure {
		return
	}
	openedAt := b.recordFailure(now)
	if len(b.failures) >= b.options.FailureThreshold {
		b.state = domain.CircuitOpen
		b.openedAt = openedAt
	}
}

func (b *Breaker) completeProbe(now time.Time, outcome domain.InferenceOutcome) {
	if b.state != domain.CircuitHalfOpen {
		return
	}
	if b.probesInFlight > 0 {
		b.probesInFlight--
	}
	switch outcome {
	case domain.InferenceSuccess:
		b.state = domain.CircuitClosed
		b.failures = nil
		b.openedAt = time.Time{}
		b.probesInFlight = 0
	case domain.InferenceFailure:
		openedAt := b.recordFailure(now)
		b.state = domain.CircuitOpen
		b.openedAt = openedAt
		b.probesInFlight = 0
	}
}

func (b *Breaker) transitionOpenToHalfOpen(now time.Time) {
	if b.state != domain.CircuitOpen || now.Before(b.openedAt.Add(b.options.OpenCooldown)) {
		return
	}
	b.state = domain.CircuitHalfOpen
	b.openedAt = time.Time{}
}

func (b *Breaker) recordFailure(failedAt time.Time) time.Time {
	b.failures = append(b.failures, failedAt)
	latest := b.failures[0]
	for _, failure := range b.failures[1:] {
		if failure.After(latest) {
			latest = failure
		}
	}
	b.pruneFailures(latest)
	return latest
}

func (b *Breaker) pruneFailures(now time.Time) {
	cutoff := now.Add(-b.options.FailureWindow)
	kept := b.failures[:0]
	for _, failure := range b.failures {
		if !failure.Before(cutoff) {
			kept = append(kept, failure)
		}
	}
	b.failures = kept
}
