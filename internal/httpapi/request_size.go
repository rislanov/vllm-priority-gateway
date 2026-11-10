package httpapi

import (
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

// RequestSizeAdmissionDTO is the flat policy object used by the Admin API.
type RequestSizeAdmissionDTO struct {
	Enabled bool `json:"enabled"`
	domain.RequestSizeLimits
	ClassLimits                   domain.ClassRequestSizeLimits `json:"classLimits"`
	MaxOutstandingTokens          int64                         `json:"maxOutstandingTokens"`
	EmergencyMaxOutstandingTokens int64                         `json:"emergencyMaxOutstandingTokens"`
	FallbackMode                  string                        `json:"fallbackMode"`
	EstimationProfile             string                        `json:"estimationProfile"`
	FallbackOverheadTokens        int64                         `json:"fallbackOverheadTokens"`
}

func requestSizeAdmissionDTO(p domain.RequestSizePolicy) RequestSizeAdmissionDTO {
	return RequestSizeAdmissionDTO{
		Enabled: p.Enabled, RequestSizeLimits: p.Limits, ClassLimits: p.ClassLimits,
		MaxOutstandingTokens: p.MaxOutstandingTokens, EmergencyMaxOutstandingTokens: p.EmergencyMaxOutstandingTokens,
		FallbackMode: p.FallbackMode, EstimationProfile: p.EstimationProfile, FallbackOverheadTokens: p.FallbackOverheadTokens,
	}
}

func (input PoolInput) storeParams() store.CreatePoolParams {
	p := store.CreatePoolParams{
		PublicModelName: input.PublicModelName, UpstreamModelName: input.UpstreamModelName,
		Enabled: input.Enabled, MaxGatewayInflight: input.MaxGatewayInflight,
		HighPriorityReserve: input.HighPriorityReserve, MaxWaiting: input.MaxWaiting,
	}
	if size := input.RequestSizeAdmission; size != nil {
		policy := size.Policy()
		p.RequestSizeAdmission = &policy
	}
	return p
}

// Policy converts the API representation to the admission policy.
func (size RequestSizeAdmissionDTO) Policy() domain.RequestSizePolicy {
	return domain.RequestSizePolicy{
		Enabled: size.Enabled, Limits: size.RequestSizeLimits, ClassLimits: size.ClassLimits,
		MaxOutstandingTokens: size.MaxOutstandingTokens, EmergencyMaxOutstandingTokens: size.EmergencyMaxOutstandingTokens,
		FallbackMode: size.FallbackMode, EstimationProfile: size.EstimationProfile, FallbackOverheadTokens: size.FallbackOverheadTokens,
	}
}
