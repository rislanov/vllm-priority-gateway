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
	complete(outcome)
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
	complete(domain.InferenceNeutral)
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

func TestBreakerHalfOpenSuccessClosesAndFailureReopens(t *testing.T) {
	b := newTestBreaker(t)
	trip(t, b)
	complete, ok := b.Acquire(base.Add(7 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the half-open probe")
	}
	complete(domain.InferenceSuccess)
	assertSnapshot(t, b.Snapshot(base.Add(7*time.Second)), domain.CircuitClosed, 0, time.Time{}, 0, true)

	b = newTestBreaker(t)
	trip(t, b)
	complete, ok = b.Acquire(base.Add(7 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the half-open probe")
	}
	complete(domain.InferenceFailure)
	assertSnapshot(t, b.Snapshot(base.Add(7*time.Second)), domain.CircuitOpen, 4, base.Add(12*time.Second), 0, false)
}

func TestBreakerNeutralOutcomeReleasesProbeWithoutHealing(t *testing.T) {
	b := newTestBreaker(t)
	trip(t, b)
	complete, ok := b.Acquire(base.Add(7 * time.Second))
	if !ok {
		t.Fatal("Acquire() rejected the half-open probe")
	}
	complete(domain.InferenceNeutral)
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
	complete(domain.InferenceFailure)
	complete(domain.InferenceFailure)
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

	completeAtTwenty(domain.InferenceFailure)
	completeAtTwentyOne(domain.InferenceFailure)
	completeAtZero(domain.InferenceFailure)

	assertSnapshot(t, b.Snapshot(base.Add(21*time.Second)), domain.CircuitClosed, 2, time.Time{}, 0, true)
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
