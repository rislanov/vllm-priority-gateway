package coordination

import (
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestEmergencyTokenCaps(t *testing.T) {
	emergency := NewEmergencyAdmission(1, 3)
	reserve := func(amount int64) TokenReservation {
		return TokenReservation{
			Enabled: true, ReservedTokens: amount, InputEstimate: amount - 1, OutputAllowance: 1, PromptCount: 1, Samples: 1,
			ClientBudget: 800, PoolBudget: 1000, PoolPolicyRevision: 1, EstimatorKind: "upstream", EstimationProfile: "test",
		}
	}
	try := func(class domain.PriorityClass, client, pool, amount int64, want Reason, scope AdmissionScope) func() {
		t.Helper()
		release, reason, gotScope := emergency.AcquireTokens(class, client, pool, 3, reserve(amount), 1000)
		if reason != want || gotScope != scope || (release != nil) != (want == "") {
			t.Fatalf("emergency admission reason=%q scope=%q, want %q %q", reason, gotScope, want, scope)
		}
		return release
	}
	try(domain.PriorityNormal, 1, 1, 600, ReasonCoordinationUnavailable, "")
	try(domain.PriorityBackground, 1, 1, 600, ReasonCoordinationUnavailable, "")
	first := try(domain.PriorityHigh, 1, 1, 600, "", "")
	try(domain.PriorityHigh, 1, 2, 300, ReasonTokenBudgetExhausted, AdmissionClientScope)
	second := try(domain.PriorityCritical, 2, 1, 400, "", "")
	try(domain.PriorityCritical, 3, 2, 400, ReasonCoordinationUnavailable, "")
	try(domain.PriorityHigh, 3, 1, 2, ReasonTokenBudgetExhausted, AdmissionPoolScope)
	first()
	first()
	replacement := try(domain.PriorityHigh, 1, 1, 600, "", "")
	try(domain.PriorityHigh, 3, 1, 2, ReasonTokenBudgetExhausted, AdmissionPoolScope)
	replacement()
	second()
	legacy, ok := emergency.AcquireForPool(domain.PriorityHigh, 1, 1, 3)
	if !ok || !emergency.HasLegacyPoolLeases(1) {
		t.Fatal("legacy emergency admission must be visible to the enable gate")
	}
	try(domain.PriorityHigh, 2, 1, 200, ReasonTokenPolicyTransition, "")
	legacy()
	if emergency.HasLegacyPoolLeases(1) {
		t.Fatal("legacy emergency request did not drain")
	}
	try(domain.PriorityHigh, 2, 1, 200, "", "")()
}
