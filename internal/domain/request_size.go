package domain

import (
	"fmt"
	"strings"
)

const MaxRequestSizeTokens int64 = 1_000_000_000_000

const (
	TokenFallbackReject    = "reject"
	TokenFallbackUTF8Bytes = "utf8_bytes"
)

type RequestSizeLimits struct {
	MaxInputTokens      int64 `json:"maxInputTokens"`
	MaxOutputTokens     int64 `json:"maxOutputTokens"`
	DefaultOutputTokens int64 `json:"defaultOutputTokens"`
}

type ClassRequestSizeLimits struct {
	Critical   RequestSizeLimits `json:"critical"`
	High       RequestSizeLimits `json:"high"`
	Normal     RequestSizeLimits `json:"normal"`
	Background RequestSizeLimits `json:"background"`
}

type RequestSizePolicy struct {
	Enabled                                             bool
	Limits                                              RequestSizeLimits
	ClassLimits                                         ClassRequestSizeLimits
	MaxOutstandingTokens, EmergencyMaxOutstandingTokens int64
	FallbackMode, EstimationProfile                     string
	FallbackOverheadTokens                              int64
}

type EffectiveRequestSizePolicy struct {
	Enabled                                       bool
	Limits                                        RequestSizeLimits
	ClientBudget, PoolBudget, EmergencyPoolBudget int64
	FallbackMode, EstimationProfile               string
	FallbackOverheadTokens                        int64
}

func (l RequestSizeLimits) validate() error {
	if err := validateTokenAmount("maxInputTokens", l.MaxInputTokens); err != nil {
		return err
	}
	if err := validateTokenAmount("maxOutputTokens", l.MaxOutputTokens); err != nil {
		return err
	}
	if err := validateTokenAmount("defaultOutputTokens", l.DefaultOutputTokens); err != nil {
		return err
	}
	if l.MaxOutputTokens > 0 && l.DefaultOutputTokens > l.MaxOutputTokens {
		return fmt.Errorf("defaultOutputTokens must not exceed maxOutputTokens")
	}
	return nil
}

func validateTokenAmount(name string, value int64) error {
	if value < 0 || value > MaxRequestSizeTokens {
		return fmt.Errorf("%s must be between 0 and %d", name, MaxRequestSizeTokens)
	}
	return nil
}

func validateClientRequestSize(client Client) error {
	if err := client.RequestSizeLimits.validate(); err != nil {
		return err
	}
	return validateTokenAmount("maxOutstandingTokens", client.MaxOutstandingTokens)
}

func (p RequestSizePolicy) Validate() error {
	if err := p.Limits.validate(); err != nil {
		return err
	}
	for _, entry := range []struct {
		class  PriorityClass
		limits RequestSizeLimits
	}{
		{PriorityCritical, p.ClassLimits.Critical},
		{PriorityHigh, p.ClassLimits.High},
		{PriorityNormal, p.ClassLimits.Normal},
		{PriorityBackground, p.ClassLimits.Background},
	} {
		if err := entry.limits.validate(); err != nil {
			return fmt.Errorf("%s request size limits: %w", entry.class, err)
		}
	}
	if err := validateTokenAmount("maxOutstandingTokens", p.MaxOutstandingTokens); err != nil {
		return err
	}
	if err := validateTokenAmount("emergencyMaxOutstandingTokens", p.EmergencyMaxOutstandingTokens); err != nil {
		return err
	}
	if p.EmergencyMaxOutstandingTokens > p.MaxOutstandingTokens {
		return fmt.Errorf("emergencyMaxOutstandingTokens must not exceed maxOutstandingTokens")
	}
	if err := validateTokenAmount("fallbackOverheadTokens", p.FallbackOverheadTokens); err != nil {
		return err
	}
	switch p.FallbackMode {
	case "", TokenFallbackReject:
	case TokenFallbackUTF8Bytes:
		if p.FallbackOverheadTokens == 0 {
			return fmt.Errorf("utf8_bytes fallback requires positive fallbackOverheadTokens")
		}
	default:
		return fmt.Errorf("fallbackMode must be reject or utf8_bytes")
	}
	if p.Enabled && (p.MaxOutstandingTokens == 0 || p.Limits.MaxInputTokens == 0 ||
		p.Limits.MaxOutputTokens == 0 || p.Limits.DefaultOutputTokens == 0 || strings.TrimSpace(p.EstimationProfile) == "") {
		return fmt.Errorf("requestSizeAdmission requires positive pool limits and an estimationProfile")
	}
	return nil
}

func ResolveRequestSizePolicy(client Client, pool ModelPool) (EffectiveRequestSizePolicy, error) {
	policy := pool.RequestSizeAdmission
	if !policy.Enabled {
		return EffectiveRequestSizePolicy{}, nil
	}
	if err := policy.Validate(); err != nil {
		return EffectiveRequestSizePolicy{}, err
	}
	if err := validateClientRequestSize(client); err != nil {
		return EffectiveRequestSizePolicy{}, err
	}
	var class RequestSizeLimits
	switch client.PriorityClass {
	case PriorityCritical:
		class = policy.ClassLimits.Critical
	case PriorityHigh:
		class = policy.ClassLimits.High
	case PriorityNormal:
		class = policy.ClassLimits.Normal
	case PriorityBackground:
		class = policy.ClassLimits.Background
	default:
		return EffectiveRequestSizePolicy{}, fmt.Errorf("invalid priority class for request size admission")
	}

	override := client.RequestSizeLimits
	input := minPositiveLimit(policy.Limits.MaxInputTokens, class.MaxInputTokens, override.MaxInputTokens)
	output := minPositiveLimit(policy.Limits.MaxOutputTokens, class.MaxOutputTokens, override.MaxOutputTokens)
	defaultOutput := override.DefaultOutputTokens
	if defaultOutput == 0 {
		defaultOutput = class.DefaultOutputTokens
	}
	if defaultOutput == 0 {
		defaultOutput = policy.Limits.DefaultOutputTokens
	}
	fallback := policy.FallbackMode
	if fallback == "" {
		fallback = TokenFallbackReject
	}
	return EffectiveRequestSizePolicy{
		Enabled:      true,
		Limits:       RequestSizeLimits{input, output, min(defaultOutput, output)},
		ClientBudget: client.MaxOutstandingTokens, PoolBudget: policy.MaxOutstandingTokens,
		EmergencyPoolBudget: minPositiveLimit(policy.MaxOutstandingTokens, policy.EmergencyMaxOutstandingTokens),
		FallbackMode:        fallback, EstimationProfile: policy.EstimationProfile,
		FallbackOverheadTokens: policy.FallbackOverheadTokens,
	}, nil
}

func minPositiveLimit(base int64, overrides ...int64) int64 {
	for _, limit := range overrides {
		if limit > 0 {
			base = min(base, limit)
		}
	}
	return base
}
