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
	mu                sync.Mutex
	options           Options
	state             domain.CircuitState
	generation        uint64
	failures          []time.Time
	openedAt          time.Time
	probesInFlight    int
	halfOpenSucceeded bool
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

func (b *Breaker) Acquire(now time.Time) (complete func(domain.InferenceOutcome, time.Time), ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.transitionOpenToHalfOpen(now)
	if b.state == domain.CircuitClosed {
		b.pruneFailures(now)
		return b.completion(b.generation, false), true
	}
	if b.state != domain.CircuitHalfOpen {
		return nil, false
	}

	if b.halfOpenSucceeded || b.probesInFlight >= b.options.HalfOpenMaxProbes {
		return nil, false
	}
	b.probesInFlight++
	return b.completion(b.generation, true), true
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
		snapshot.Available = !b.halfOpenSucceeded && b.probesInFlight < b.options.HalfOpenMaxProbes
	}
	return snapshot
}

func (b *Breaker) completion(generation uint64, probe bool) func(domain.InferenceOutcome, time.Time) {
	var once sync.Once
	return func(outcome domain.InferenceOutcome, completedAt time.Time) {
		once.Do(func() {
			b.complete(generation, probe, outcome, completedAt)
		})
	}
}

func (b *Breaker) complete(generation uint64, probe bool, outcome domain.InferenceOutcome, completedAt time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if generation != b.generation {
		return
	}
	if probe {
		b.completeProbe(completedAt, outcome)
		return
	}
	if b.state != domain.CircuitClosed || outcome != domain.InferenceFailure {
		return
	}
	openedAt := b.recordFailure(completedAt)
	if len(b.failures) >= b.options.FailureThreshold {
		b.enterOpen(openedAt)
	}
}

func (b *Breaker) completeProbe(completedAt time.Time, outcome domain.InferenceOutcome) {
	if b.state != domain.CircuitHalfOpen {
		return
	}
	if b.probesInFlight > 0 {
		b.probesInFlight--
	}
	switch outcome {
	case domain.InferenceSuccess:
		b.halfOpenSucceeded = true
	case domain.InferenceFailure:
		b.enterOpen(b.recordFailure(completedAt))
		return
	}
	if b.halfOpenSucceeded && b.probesInFlight == 0 {
		b.enterClosed()
	}
}

func (b *Breaker) transitionOpenToHalfOpen(now time.Time) {
	if b.state != domain.CircuitOpen || now.Before(b.openedAt.Add(b.options.OpenCooldown)) {
		return
	}
	b.state = domain.CircuitHalfOpen
	b.generation++
	b.openedAt = time.Time{}
	b.probesInFlight = 0
	b.halfOpenSucceeded = false
}

func (b *Breaker) enterOpen(openedAt time.Time) {
	b.state = domain.CircuitOpen
	b.generation++
	b.openedAt = openedAt
	b.probesInFlight = 0
	b.halfOpenSucceeded = false
}

func (b *Breaker) enterClosed() {
	b.state = domain.CircuitClosed
	b.generation++
	b.failures = nil
	b.openedAt = time.Time{}
	b.probesInFlight = 0
	b.halfOpenSucceeded = false
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
