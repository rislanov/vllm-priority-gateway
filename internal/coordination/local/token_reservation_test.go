package local_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/contracttest"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
)

func TestTokenReservationOwnerMismatch(t *testing.T) {
	for _, field := range []struct {
		name   string
		change func(*coordination.LeaseIdentity)
	}{
		{"client", func(lease *coordination.LeaseIdentity) { lease.ClientID++ }},
		{"pool", func(lease *coordination.LeaseIdentity) { lease.PoolID++ }},
		{"replica", func(lease *coordination.LeaseIdentity) { lease.ReplicaID = uuid.New() }},
		{"request", func(lease *coordination.LeaseIdentity) { lease.RequestID += "-other" }},
	} {
		t.Run(field.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 11, 10, 21, 0, 0, 0, time.UTC)
			coordinator := local.NewAdmissionCoordinator(func() time.Time { return now })
			req := request(uuid.New(), now)
			admitted, err := coordinator.Acquire(ctx, req)
			if err != nil || !admitted.Admitted() {
				t.Fatalf("acquire = %+v, %v", admitted, err)
			}
			owner := admitted.Lease.Identity()
			other := owner
			field.change(&other)
			now = now.Add(time.Second)
			renewed, err := coordinator.Renew(ctx, []coordination.LeaseIdentity{other})
			if err != nil || renewed[0].Reason != coordination.ReasonLeaseLost {
				t.Errorf("foreign renewal = %+v, %v", renewed, err)
			}
			replay, err := coordinator.Acquire(ctx, req)
			if err != nil || !replay.Admitted() || !replay.Lease.ExpiresAt.Equal(owner.ExpiresAt) {
				t.Errorf("foreign renewal affected owner lease: %+v, %v", replay, err)
			}
			result, err := coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: other, Usage: &coordination.TokenUsage{InputTokens: 1000}}})
			if err != nil || result[0].Reason != coordination.ReasonLeaseLost {
				t.Errorf("foreign completion = %+v, %v", result, err)
			}
			result, err = coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: owner}})
			if err != nil || result[0].Result != coordination.CompletionReleased {
				t.Errorf("owner completion = %+v, %v", result, err)
			}
			next, err := coordinator.Acquire(ctx, request(uuid.New(), now))
			if err != nil || !next.Admitted() {
				t.Errorf("foreign completion charged usage: %+v, %v", next, err)
			}
		})
	}
}

func TestLocalTokenReservationContract(t *testing.T) {
	contracttest.TokenReservationContract(t, func(t *testing.T) (coordination.AdmissionCoordinator, func(int, int64) coordination.AdmissionRequest) {
		now := time.Date(2026, 11, 10, 21, 0, 0, 0, time.UTC)
		return local.NewAdmissionCoordinator(func() time.Time { return now }), func(client int, tokens int64) coordination.AdmissionRequest {
			r := tokenRequest(now, tokens)
			r.ClientID += int64(client)
			return r
		}
	})
}

func tokenRequest(now time.Time, tokens int64) coordination.AdmissionRequest {
	r := request(uuid.New(), now)
	r.EffectiveClientLimit, r.ConfiguredClientLimit, r.PoolGatewayInflightLimit = 10, 10, 10
	r.RequestsPerMinute, r.TokensPerMinute = 0, 0
	r.TokenReservation = coordination.TokenReservation{
		Enabled: true, ReservedTokens: tokens, InputEstimate: tokens - 1, OutputAllowance: 1,
		PromptCount: 1, Samples: 1, ClientBudget: 800, PoolBudget: 1000, PoolPolicyRevision: 1,
		EstimatorKind: "upstream", EstimationProfile: "qualified-v1",
	}
	return r
}

func TestTokenReservationLifecycle(t *testing.T) {
	ctx := context.Background()
	clock := contracttest.NewClock(time.Date(2026, 11, 10, 21, 0, 0, 0, time.UTC))
	c := local.NewAdmissionCoordinator(clock.Now)
	acquire := func(r coordination.AdmissionRequest) coordination.AdmissionDecision {
		t.Helper()
		d, err := c.Acquire(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	r := tokenRequest(clock.Now(), 600)
	r.TokensPerMinute = 250
	first := acquire(r)
	if !first.Admitted() {
		t.Fatal(first)
	}
	complete := func(lease coordination.LeaseIdentity, usage *coordination.TokenUsage, want coordination.CompletionResult) {
		t.Helper()
		results, err := c.Complete(ctx, []coordination.LeaseCompletion{{Lease: lease, Usage: usage}})
		if err != nil || len(results) != 1 || results[0].Result != want {
			t.Fatalf("complete = %+v, %v; want %q", results, err, want)
		}
	}
	assertOutstanding := func(want int64) {
		t.Helper()
		if got := c.PoolOutstandingTokens(r.PoolID); got != want {
			t.Fatalf("outstanding = %d; want %d", got, want)
		}
	}
	assertOutstanding(600)
	usage := &coordination.TokenUsage{InputTokens: 150, OutputTokens: 100}
	complete(*first.Lease, usage, coordination.CompletionReleased)
	complete(*first.Lease, usage, coordination.CompletionAlreadyCompleted)
	assertOutstanding(0)
	r = tokenRequest(clock.Now(), 600)
	r.TokensPerMinute = 250
	if got := acquire(r); got.Reason != coordination.ReasonTPMExhausted {
		t.Fatalf("actual usage did not debit the full TPM balance: %+v", got)
	}
	clock.Add(time.Second) // A single charge refills above one, a duplicate charge does not.

	r = tokenRequest(clock.Now(), 600)
	r.TokensPerMinute = 250
	r.LeaseTTL = time.Second
	second := acquire(r)
	if !second.Admitted() {
		t.Fatalf("duplicate completion appears to have depleted the TPM budget: %+v", second)
	}
	clock.Add(time.Minute)
	assertOutstanding(0)
	complete(*second.Lease, usage, coordination.CompletionLeaseLost)
	complete(*second.Lease, usage, coordination.CompletionAlreadyCompleted)
	assertOutstanding(0)
	r = tokenRequest(clock.Now(), 600)
	r.TokensPerMinute = 250
	if got := acquire(r); got.Reason != coordination.ReasonTPMExhausted {
		t.Fatalf("valid late completion did not debit TPM: %+v", got)
	}
	clock.Add(time.Second)

	r = tokenRequest(clock.Now(), 600)
	r.TokensPerMinute = 250
	third := acquire(r)
	if !third.Admitted() {
		t.Fatalf("late completion did not preserve the expected refill: %+v", third)
	}
	complete(*third.Lease, nil, coordination.CompletionReleased)
	assertOutstanding(0)
	r.LeaseID = uuid.New()
	if got := acquire(r); !got.Admitted() {
		t.Fatalf("unknown usage debited estimated tokens: %+v", got)
	}
}

func TestTokenReservationTelemetry(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 10, 21, 0, 0, 0, time.UTC)
	clock := contracttest.NewClock(now)
	c := local.NewAdmissionCoordinator(clock.Now)
	var events reservationEvents
	c.SetTokenReservationObserver(&events)
	req := tokenRequest(now, 600)
	acquire := func(r coordination.AdmissionRequest) coordination.LeaseIdentity {
		t.Helper()
		result, err := c.Acquire(ctx, r)
		if err != nil || !result.Admitted() {
			t.Fatalf("acquire = %+v, %v", result, err)
		}
		return *result.Lease
	}
	first := acquire(req)
	acquire(req)
	complete := func(lease coordination.LeaseIdentity, usage *coordination.TokenUsage) {
		t.Helper()
		if _, err := c.Complete(ctx, []coordination.LeaseCompletion{{Lease: lease, Usage: usage}}); err != nil {
			t.Fatal(err)
		}
	}
	usage := &coordination.TokenUsage{InputTokens: 150, OutputTokens: 100}
	complete(first, usage)
	complete(first, usage)
	complete(acquire(tokenRequest(now, 600)), nil)
	acquire(tokenRequest(now, 600))
	clock.Add(req.LeaseTTL)
	if got := c.OutstandingTokenSnapshot()[req.PoolID]; got != 0 {
		t.Fatalf("expired reservation still holds %d tokens", got)
	}
	want := []coordination.TokenReservationAction{
		coordination.ReservationAcquired, coordination.ReservationReleased, coordination.ReservationReconciled,
		coordination.ReservationAcquired, coordination.ReservationReleased, coordination.ReservationReconciled,
		coordination.ReservationAcquired, coordination.ReservationExpired,
	}
	if len(events) != len(want) {
		t.Fatalf("events = %+v", events)
	}
	for i, action := range want {
		event := events[i]
		if event.Action != action || event.ReservedTokens != 600 || event.PoolID != req.PoolID || event.ClientID != req.ClientID {
			t.Errorf("event %d = %+v, want %s", i, event, action)
		}
	}
	if known, unknown := events[2], events[5]; !known.UsageKnown || known.ActualInput+known.ActualOutput != 250 || unknown.UsageKnown {
		t.Fatalf("known usage = %+v, unknown usage = %+v", known, unknown)
	}
}

type reservationEvents []coordination.TokenReservationEvent

func (e *reservationEvents) TokenReservationChanged(event coordination.TokenReservationEvent) {
	*e = append(*e, event)
}
