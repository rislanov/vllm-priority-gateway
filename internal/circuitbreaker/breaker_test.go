package circuitbreaker

import (
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

var base = time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)

func testOptions() Options {
	return Options{
		FailureThreshold:  3,
		FailureWindow:     10 * time.Second,
		OpenCooldown:      5 * time.Second,
		HalfOpenMaxProbes: 1,
	}
}

func newTestBreaker(t *testing.T) *Breaker {
	t.Helper()
	b, err := New(testOptions())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return b
}

func assertSnapshot(t *testing.T, got Snapshot, state domain.CircuitState, failures int, retryAt time.Time, probes int, available bool) {
	t.Helper()
	if got.State != state || got.FailureCount != failures || !got.RetryAt.Equal(retryAt) || got.ProbesInFlight != probes || got.Available != available {
		t.Fatalf("Snapshot() = %+v, want state=%q failures=%d retryAt=%s probes=%d available=%t", got, state, failures, retryAt, probes, available)
	}
}

func acquireAndComplete(t *testing.T, b *Breaker, now time.Time, outcome domain.InferenceOutcome) {
	t.Helper()
	complete, ok := b.Acquire(now)
	if !ok {
		t.Fatalf("Acquire(%s) unexpectedly rejected", now)
	}
	complete(outcome, now)
}

func trip(t *testing.T, b *Breaker) {
	t.Helper()
	for _, now := range []time.Time{base, base.Add(time.Second), base.Add(2 * time.Second)} {
		acquireAndComplete(t, b, now, domain.InferenceFailure)
	}
}

func TestBreakerOpensWithinRollingWindowAndExpiresOldFailures(t *testing.T) {
	b := newTestBreaker(t)
	assertSnapshot(t, b.Snapshot(base), domain.CircuitClosed, 0, time.Time{}, 0, true)

	acquireAndComplete(t, b, base, domain.InferenceFailure)
	assertSnapshot(t, b.Snapshot(base), domain.CircuitClosed, 1, time.Time{}, 0, true)
	acquireAndComplete(t, b, base.Add(time.Second), domain.InferenceFailure)
	assertSnapshot(t, b.Snapshot(base.Add(time.Second)), domain.CircuitClosed, 2, time.Time{}, 0, true)

	acquireAndComplete(t, b, base.Add(11*time.Second), domain.InferenceNeutral)
	assertSnapshot(t, b.Snapshot(base.Add(11*time.Second)), domain.CircuitClosed, 1, time.Time{}, 0, true)
	acquireAndComplete(t, b, base.Add(11*time.Second), domain.InferenceFailure)
	assertSnapshot(t, b.Snapshot(base.Add(11*time.Second)), domain.CircuitClosed, 2, time.Time{}, 0, true)
	acquireAndComplete(t, b, base.Add(11*time.Second), domain.InferenceFailure)
	assertSnapshot(t, b.Snapshot(base.Add(11*time.Second)), domain.CircuitOpen, 3, base.Add(16*time.Second), 0, false)
}

func TestBreakerCooldownAllowsOnlyOneHalfOpenProbe(t *testing.T) {
	b := newTestBreaker(t)
	trip(t, b)
	assertSnapshot(t, b.Snapshot(base.Add(2*time.Second)), domain.CircuitOpen, 3, base.Add(7*time.Second), 0, false)

	if complete, ok := b.Acquire(base.Add(6 * time.Second)); ok || complete != nil {
		t.Fatal("Acquire() allowed a request while the circuit was open")
	}
	assertSnapshot(t, b.Snapshot(base.Add(6*time.Second)), domain.CircuitOpen, 3, base.Add(7*time.Second), 0, false)

	complete, ok := b.Acquire(base.Add(7 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the first half-open probe")
	}
	assertSnapshot(t, b.Snapshot(base.Add(7*time.Second)), domain.CircuitHalfOpen, 3, time.Time{}, 1, false)
	if rejected, ok := b.Acquire(base.Add(7 * time.Second)); ok || rejected != nil {
		t.Fatal("Acquire() allowed a second half-open probe")
	}
	assertSnapshot(t, b.Snapshot(base.Add(7*time.Second)), domain.CircuitHalfOpen, 3, time.Time{}, 1, false)
	complete(domain.InferenceNeutral, base.Add(7*time.Second))
	assertSnapshot(t, b.Snapshot(base.Add(7*time.Second)), domain.CircuitHalfOpen, 3, time.Time{}, 0, true)
}

func TestSnapshotTransitionsExpiredOpenCircuitToHalfOpenBeforeAcquire(t *testing.T) {
	b := newTestBreaker(t)
	trip(t, b)
	retryAt := base.Add(7 * time.Second)

	assertSnapshot(t, b.Snapshot(retryAt), domain.CircuitHalfOpen, 3, time.Time{}, 0, true)
	complete, ok := b.Acquire(retryAt)
	if !ok || complete == nil {
		t.Fatal("Acquire() did not reserve a probe after Snapshot() exposed the half-open circuit")
	}
	assertSnapshot(t, b.Snapshot(retryAt), domain.CircuitHalfOpen, 3, time.Time{}, 1, false)
}

func TestSnapshotExpiresClosedFailuresWithoutAcquire(t *testing.T) {
	b := newTestBreaker(t)
	acquireAndComplete(t, b, base, domain.InferenceFailure)

	assertSnapshot(t, b.Snapshot(base.Add(11*time.Second)), domain.CircuitClosed, 0, time.Time{}, 0, true)
}

func TestBreakerHalfOpenAllowsConfiguredProbeCapacity(t *testing.T) {
	options := testOptions()
	options.HalfOpenMaxProbes = 2
	b, err := New(options)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	trip(t, b)
	retryAt := base.Add(7 * time.Second)

	first, ok := b.Acquire(retryAt)
	if !ok || first == nil {
		t.Fatal("Acquire() rejected the first half-open probe")
	}
	second, ok := b.Acquire(retryAt)
	if !ok || second == nil {
		t.Fatal("Acquire() rejected the second half-open probe")
	}
	if third, ok := b.Acquire(retryAt); ok || third != nil {
		t.Fatal("Acquire() allowed a third half-open probe")
	}
	assertSnapshot(t, b.Snapshot(retryAt), domain.CircuitHalfOpen, 3, time.Time{}, 2, false)

	first(domain.InferenceNeutral, retryAt)
	assertSnapshot(t, b.Snapshot(retryAt), domain.CircuitHalfOpen, 3, time.Time{}, 1, true)
}

func TestBreakerHalfOpenSuccessClosesAndFailureReopens(t *testing.T) {
	b := newTestBreaker(t)
	trip(t, b)
	complete, ok := b.Acquire(base.Add(7 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the half-open probe")
	}
	complete(domain.InferenceSuccess, base.Add(7*time.Second))
	assertSnapshot(t, b.Snapshot(base.Add(7*time.Second)), domain.CircuitClosed, 0, time.Time{}, 0, true)

	b = newTestBreaker(t)
	trip(t, b)
	complete, ok = b.Acquire(base.Add(7 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the half-open probe")
	}
	complete(domain.InferenceFailure, base.Add(7*time.Second))
	assertSnapshot(t, b.Snapshot(base.Add(7*time.Second)), domain.CircuitOpen, 4, base.Add(12*time.Second), 0, false)
}

func TestBreakerNeutralOutcomeReleasesProbeWithoutHealing(t *testing.T) {
	b := newTestBreaker(t)
	trip(t, b)
	complete, ok := b.Acquire(base.Add(7 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the half-open probe")
	}
	complete(domain.InferenceNeutral, base.Add(7*time.Second))
	assertSnapshot(t, b.Snapshot(base.Add(7*time.Second)), domain.CircuitHalfOpen, 3, time.Time{}, 0, true)

	if next, ok := b.Acquire(base.Add(7 * time.Second)); !ok || next == nil {
		t.Fatal("Acquire() did not release the neutral half-open probe")
	}
	assertSnapshot(t, b.Snapshot(base.Add(7*time.Second)), domain.CircuitHalfOpen, 3, time.Time{}, 1, false)
}

func TestBreakerCompletionIsIdempotent(t *testing.T) {
	b := newTestBreaker(t)
	trip(t, b)
	complete, ok := b.Acquire(base.Add(7 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the half-open probe")
	}
	complete(domain.InferenceFailure, base.Add(7*time.Second))
	complete(domain.InferenceFailure, base.Add(8*time.Second))
	assertSnapshot(t, b.Snapshot(base.Add(7*time.Second)), domain.CircuitOpen, 4, base.Add(12*time.Second), 0, false)
}

func TestBreakerKeepsOnlyRollingFailuresWhenCompletionsArriveOutOfOrder(t *testing.T) {
	b := newTestBreaker(t)
	completeAtZero, ok := b.Acquire(base)
	if !ok {
		t.Fatal("Acquire() rejected the first request")
	}
	completeAtTwenty, ok := b.Acquire(base.Add(20 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the second request")
	}
	completeAtTwentyOne, ok := b.Acquire(base.Add(21 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the third request")
	}

	completeAtTwenty(domain.InferenceFailure, base.Add(20*time.Second))
	completeAtTwentyOne(domain.InferenceFailure, base.Add(21*time.Second))
	completeAtZero(domain.InferenceFailure, base)

	assertSnapshot(t, b.Snapshot(base.Add(21*time.Second)), domain.CircuitClosed, 2, time.Time{}, 0, true)
}

func TestBreakerUsesCompletionTimeForDelayedClosedFailures(t *testing.T) {
	b := newTestBreaker(t)
	first, ok := b.Acquire(base)
	if !ok {
		t.Fatal("Acquire() rejected the first delayed request")
	}
	second, ok := b.Acquire(base.Add(time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the second delayed request")
	}

	first(domain.InferenceFailure, base.Add(20*time.Second))
	second(domain.InferenceFailure, base.Add(21*time.Second))
	complete, ok := b.Acquire(base.Add(22 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the third request")
	}
	complete(domain.InferenceFailure, base.Add(22*time.Second))

	assertSnapshot(t, b.Snapshot(base.Add(22*time.Second)), domain.CircuitOpen, 3, base.Add(27*time.Second), 0, false)
}

func TestBreakerUsesCompletionTimeForDelayedHalfOpenFailure(t *testing.T) {
	b := newTestBreaker(t)
	trip(t, b)
	probe, ok := b.Acquire(base.Add(7 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the half-open probe")
	}

	probe(domain.InferenceFailure, base.Add(30*time.Second))

	assertSnapshot(t, b.Snapshot(base.Add(34*time.Second)), domain.CircuitOpen, 1, base.Add(35*time.Second), 0, false)
}

func TestBreakerHalfOpenSuccessWaitsForOutstandingFailure(t *testing.T) {
	b, retryAt := newConcurrentHalfOpenBreaker(t)
	first, second := acquireHalfOpenPair(t, b, retryAt)

	first(domain.InferenceSuccess, retryAt.Add(time.Second))
	assertSnapshot(t, b.Snapshot(retryAt.Add(time.Second)), domain.CircuitHalfOpen, 3, time.Time{}, 1, false)
	if third, ok := b.Acquire(retryAt.Add(time.Second)); ok || third != nil {
		t.Fatal("Acquire() admitted a new probe while a successful generation was draining")
	}

	second(domain.InferenceFailure, retryAt.Add(2*time.Second))
	first(domain.InferenceFailure, retryAt.Add(3*time.Second))
	assertSnapshot(t, b.Snapshot(retryAt.Add(2*time.Second)), domain.CircuitOpen, 4, retryAt.Add(7*time.Second), 0, false)
}

func TestBreakerHalfOpenStaleSuccessCannotHealLaterGeneration(t *testing.T) {
	b, retryAt := newConcurrentHalfOpenBreaker(t)
	failed, staleSuccess := acquireHalfOpenPair(t, b, retryAt)

	failed(domain.InferenceFailure, retryAt.Add(time.Second))
	nextRetryAt := retryAt.Add(6 * time.Second)
	current, ok := b.Acquire(nextRetryAt)
	if !ok {
		t.Fatal("Acquire() rejected a probe in the next half-open generation")
	}
	staleSuccess(domain.InferenceSuccess, nextRetryAt)
	assertSnapshot(t, b.Snapshot(nextRetryAt), domain.CircuitHalfOpen, 4, time.Time{}, 1, true)

	current(domain.InferenceFailure, nextRetryAt.Add(time.Second))
	assertSnapshot(t, b.Snapshot(nextRetryAt.Add(time.Second)), domain.CircuitOpen, 2, nextRetryAt.Add(6*time.Second), 0, false)
}

func TestBreakerHalfOpenSuccessAndNeutralOrdering(t *testing.T) {
	tests := []struct {
		name          string
		firstOutcome  domain.InferenceOutcome
		secondOutcome domain.InferenceOutcome
		afterFirst    Snapshot
		afterSecond   Snapshot
	}{
		{
			name:          "success then neutral closes after drain",
			firstOutcome:  domain.InferenceSuccess,
			secondOutcome: domain.InferenceNeutral,
			afterFirst:    Snapshot{State: domain.CircuitHalfOpen, FailureCount: 3, ProbesInFlight: 1, Available: false},
			afterSecond:   Snapshot{State: domain.CircuitClosed, Available: true},
		},
		{
			name:          "neutral then success closes",
			firstOutcome:  domain.InferenceNeutral,
			secondOutcome: domain.InferenceSuccess,
			afterFirst:    Snapshot{State: domain.CircuitHalfOpen, FailureCount: 3, ProbesInFlight: 1, Available: true},
			afterSecond:   Snapshot{State: domain.CircuitClosed, Available: true},
		},
		{
			name:          "all neutral remains available",
			firstOutcome:  domain.InferenceNeutral,
			secondOutcome: domain.InferenceNeutral,
			afterFirst:    Snapshot{State: domain.CircuitHalfOpen, FailureCount: 3, ProbesInFlight: 1, Available: true},
			afterSecond:   Snapshot{State: domain.CircuitHalfOpen, FailureCount: 3, Available: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, retryAt := newConcurrentHalfOpenBreaker(t)
			first, second := acquireHalfOpenPair(t, b, retryAt)

			first(tt.firstOutcome, retryAt.Add(time.Second))
			assertSnapshotValue(t, b.Snapshot(retryAt.Add(time.Second)), tt.afterFirst)
			second(tt.secondOutcome, retryAt.Add(2*time.Second))
			assertSnapshotValue(t, b.Snapshot(retryAt.Add(2*time.Second)), tt.afterSecond)
		})
	}
}

func TestBreakerIgnoresStaleClosedFailureAfterRecovery(t *testing.T) {
	b := newTestBreaker(t)
	staleFailure, ok := b.Acquire(base)
	if !ok {
		t.Fatal("Acquire() rejected the delayed closed request")
	}
	trip(t, b)
	probe, ok := b.Acquire(base.Add(7 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the half-open recovery probe")
	}
	probe(domain.InferenceSuccess, base.Add(7*time.Second))

	staleFailure(domain.InferenceFailure, base.Add(8*time.Second))

	assertSnapshot(t, b.Snapshot(base.Add(8*time.Second)), domain.CircuitClosed, 0, time.Time{}, 0, true)
}

func newConcurrentHalfOpenBreaker(t *testing.T) (*Breaker, time.Time) {
	t.Helper()
	options := testOptions()
	options.HalfOpenMaxProbes = 2
	b, err := New(options)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	trip(t, b)
	return b, base.Add(7 * time.Second)
}

func acquireHalfOpenPair(t *testing.T, b *Breaker, at time.Time) (
	func(domain.InferenceOutcome, time.Time),
	func(domain.InferenceOutcome, time.Time),
) {
	t.Helper()
	first, ok := b.Acquire(at)
	if !ok {
		t.Fatal("Acquire() rejected the first half-open probe")
	}
	second, ok := b.Acquire(at)
	if !ok {
		t.Fatal("Acquire() rejected the second half-open probe")
	}
	return first, second
}

func assertSnapshotValue(t *testing.T, got, want Snapshot) {
	t.Helper()
	assertSnapshot(t, got, want.State, want.FailureCount, want.RetryAt, want.ProbesInFlight, want.Available)
}

func TestOptionsRejectInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Options)
	}{
		{name: "zero failure threshold", edit: func(o *Options) { o.FailureThreshold = 0 }},
		{name: "negative failure threshold", edit: func(o *Options) { o.FailureThreshold = -1 }},
		{name: "zero failure window", edit: func(o *Options) { o.FailureWindow = 0 }},
		{name: "negative failure window", edit: func(o *Options) { o.FailureWindow = -time.Second }},
		{name: "zero open cooldown", edit: func(o *Options) { o.OpenCooldown = 0 }},
		{name: "negative open cooldown", edit: func(o *Options) { o.OpenCooldown = -time.Second }},
		{name: "zero half-open probes", edit: func(o *Options) { o.HalfOpenMaxProbes = 0 }},
		{name: "negative half-open probes", edit: func(o *Options) { o.HalfOpenMaxProbes = -1 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := testOptions()
			tt.edit(&options)
			if _, err := New(options); err == nil {
				t.Fatal("New() unexpectedly accepted invalid options")
			}
		})
	}
}
