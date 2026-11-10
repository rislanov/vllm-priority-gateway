package web

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
)

func optionalTokenField(form url.Values, name string) (*int64, error) {
	raw := strings.TrimSpace(form.Get(name))
	if raw == "" {
		return nil, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 || value > domain.MaxRequestSizeTokens {
		return nil, fmt.Errorf("%s must be an integer between 0 and %d", strings.ReplaceAll(name, "_", " "), domain.MaxRequestSizeTokens)
	}
	return &value, nil
}

func readPoolRequestSize(form url.Values) (*httpapi.RequestSizeAdmissionDTO, error) {
	if !form.Has("request_size_policy") {
		return nil, nil
	}
	policy := &httpapi.RequestSizeAdmissionDTO{
		Enabled:           form.Get("request_size_enabled") == "on",
		FallbackMode:      form.Get("fallback_mode"),
		EstimationProfile: strings.TrimSpace(form.Get("estimation_profile")),
	}
	for _, fields := range []struct {
		prefix string
		limits *domain.RequestSizeLimits
	}{
		{"", &policy.RequestSizeLimits},
		{"critical_", &policy.ClassLimits.Critical},
		{"high_", &policy.ClassLimits.High},
		{"normal_", &policy.ClassLimits.Normal},
		{"background_", &policy.ClassLimits.Background},
	} {
		for _, field := range []struct {
			name   string
			target *int64
		}{
			{"max_input_tokens", &fields.limits.MaxInputTokens},
			{"max_output_tokens", &fields.limits.MaxOutputTokens},
			{"default_output_tokens", &fields.limits.DefaultOutputTokens},
		} {
			value, err := optionalTokenField(form, fields.prefix+field.name)
			if err != nil {
				return nil, err
			}
			if value != nil {
				*field.target = *value
			}
		}
	}
	for _, field := range []struct {
		name   string
		target *int64
	}{
		{"max_outstanding_tokens", &policy.MaxOutstandingTokens},
		{"emergency_max_outstanding_tokens", &policy.EmergencyMaxOutstandingTokens},
		{"fallback_overhead_tokens", &policy.FallbackOverheadTokens},
	} {
		value, err := optionalTokenField(form, field.name)
		if err != nil {
			return nil, err
		}
		if value != nil {
			*field.target = *value
		}
	}
	return policy, nil
}

func effectiveRequestSize(client *httpapi.AdminClient, pool httpapi.AdminPool) *domain.EffectiveRequestSizePolicy {
	effective, err := domain.ResolveRequestSizePolicy(domain.Client{
		PriorityClass: client.PriorityClass, RequestSizeLimits: client.RequestSizeLimits,
		MaxOutstandingTokens: client.MaxOutstandingTokens,
	}, domain.ModelPool{RequestSizeAdmission: pool.RequestSizeAdmission.Policy()})
	if err != nil {
		return nil
	}
	return &effective
}
