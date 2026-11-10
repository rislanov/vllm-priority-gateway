package coordination

import (
	"strings"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

// TokenReservation is the immutable accounting input bound to an admission
// receipt. InputEstimate is the sum of the per-prompt estimates with a floor
// of one per prompt. No request content is retained in the receipt.
type TokenReservation struct {
	Enabled                                                              bool
	InputEstimate, OutputAllowance, PromptCount, Samples, ReservedTokens int64
	ClientBudget, PoolBudget                                             int64
	PoolPolicyRevision                                                   int64
	EstimatorKind, EstimationProfile                                     string
}

func (r TokenReservation) Valid() bool {
	if !r.Enabled {
		return r == (TokenReservation{})
	}
	const max = domain.MaxRequestSizeTokens
	if r.PromptCount < 1 || r.PromptCount > 32 || r.Samples < 1 || r.Samples > 32 ||
		r.InputEstimate < r.PromptCount || r.InputEstimate > max ||
		r.OutputAllowance < 1 || r.OutputAllowance > max ||
		r.PoolBudget < 1 || r.PoolBudget > max || r.ClientBudget < 0 || r.ClientBudget > max ||
		r.PoolPolicyRevision < 1 || strings.TrimSpace(r.EstimationProfile) == "" {
		return false
	}
	switch r.EstimatorKind {
	case "upstream", "token_ids", domain.TokenFallbackUTF8Bytes, "mixed":
	default:
		return false
	}
	// Check the bound before multiplying by either dimension.
	perSample := max / r.Samples
	return r.InputEstimate <= perSample &&
		r.OutputAllowance <= (perSample-r.InputEstimate)/r.PromptCount &&
		r.ReservedTokens == (r.InputEstimate+r.OutputAllowance*r.PromptCount)*r.Samples
}

// ExhaustedScope checks budget headroom without overflowing the counters.
func (r TokenReservation) ExhaustedScope(client, pool int64) AdmissionScope {
	if !r.Enabled {
		return ""
	}
	if r.PoolBudget-pool < r.ReservedTokens {
		return AdmissionPoolScope
	}
	if r.ClientBudget > 0 && r.ClientBudget-client < r.ReservedTokens {
		return AdmissionClientScope
	}
	return ""
}
