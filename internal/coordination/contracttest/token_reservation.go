package contracttest

import (
	"context"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

// TokenReservationFactory provisions two clients in one pool with a pool
// budget of 1000, client budgets of 800, and a nonbinding concurrency limit.
type TokenReservationFactory func(*testing.T) (coordination.AdmissionCoordinator, func(client int, tokens int64) coordination.AdmissionRequest)

func TokenReservationContract(t *testing.T, factory TokenReservationFactory) {
	t.Helper()
	t.Run("shared budget and replay", func(t *testing.T) {
		coordinator, request := factory(t)
		ctx := context.Background()
		acquire := func(req coordination.AdmissionRequest, reason coordination.Reason, scope coordination.AdmissionScope) coordination.AdmissionDecision {
			t.Helper()
			decision, err := coordinator.Acquire(ctx, req)
			if err != nil || decision.Reason != reason || decision.TokenBudgetScope != scope || decision.Admitted() != (reason == "") {
				t.Fatalf("acquire = %+v, %v; want reason %q scope %q", decision, err, reason, scope)
			}
			return decision
		}
		firstRequest := request(0, 600)
		first := acquire(firstRequest, "", "")
		for range 100 {
			if replay := acquire(firstRequest, "", ""); replay.Lease.LeaseID != first.Lease.LeaseID {
				t.Fatal("replay created a second lease")
			}
		}
		denied := request(0, 201)
		acquire(denied, coordination.ReasonTokenBudgetExhausted, coordination.AdmissionClientScope)
		acquire(denied, coordination.ReasonTokenBudgetExhausted, coordination.AdmissionClientScope)
		changed := firstRequest
		changed.TokenReservation.EstimationProfile += "-other"
		acquire(changed, coordination.ReasonIdempotencyConflict, "")
		acquire(request(1, 400), "", "")
		acquire(request(1, 2), coordination.ReasonTokenBudgetExhausted, coordination.AdmissionPoolScope)
		results, err := coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: *first.Lease}})
		if err != nil || len(results) != 1 || results[0].Result != coordination.CompletionReleased {
			t.Fatalf("complete = %+v, %v", results, err)
		}
		acquire(request(0, 600), "", "")
		results, err = coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: *first.Lease}})
		if err != nil || len(results) != 1 || results[0].Result != coordination.CompletionAlreadyCompleted {
			t.Fatalf("complete replay = %+v, %v", results, err)
		}
		acquire(request(1, 2), coordination.ReasonTokenBudgetExhausted, coordination.AdmissionPoolScope)
	})
}
