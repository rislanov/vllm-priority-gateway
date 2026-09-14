package domain_test

import (
	"strings"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestBackendValidateNormalizesSafeURL(t *testing.T) {
	backend := domain.Backend{
		Name:             "gpu-1",
		BaseURL:          "http://127.0.0.1:8000/",
		CapacityHint:     1,
		RunningSoftLimit: 8,
	}

	if err := backend.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if backend.BaseURL != "http://127.0.0.1:8000" {
		t.Fatalf("BaseURL = %q", backend.BaseURL)
	}
}

func TestBackendValidateRejectsUnsafeBaseURL(t *testing.T) {
	tests := []string{
		"http://user:pass@host:8000",
		"http://host:8000?x=1",
		"http://host:8000/#fragment",
		"ftp://host:8000",
		"/relative",
	}
	for _, baseURL := range tests {
		t.Run(baseURL, func(t *testing.T) {
			backend := domain.Backend{
				Name:             "gpu-1",
				BaseURL:          baseURL,
				CapacityHint:     1,
				RunningSoftLimit: 8,
			}
			if err := backend.Validate(); err == nil {
				t.Fatal("expected unsafe backend URL to be rejected")
			}
		})
	}
}

func TestClientValidateRejectsInvalidPolicy(t *testing.T) {
	tests := []domain.Client{
		{Name: "", PriorityClass: domain.PriorityNormal, MaxConcurrency: 1},
		{Name: "client", PriorityClass: "super", MaxConcurrency: 1},
		{Name: "client", PriorityClass: domain.PriorityNormal, MaxConcurrency: -1},
	}
	for _, client := range tests {
		if err := client.Validate(); err == nil {
			t.Fatalf("Validate(%+v) unexpectedly succeeded", client)
		}
	}
}

func TestClientValidateEnforcesProductionPolicyBounds(t *testing.T) {
	valid := domain.Client{Name: "client", PriorityClass: domain.PriorityNormal, MaxConcurrency: domain.MaxClientConcurrency, RequestsPerMinute: domain.MaxRequestsPerMinute, TokensPerMinute: domain.MaxTokensPerMinute}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() boundary error = %v", err)
	}
	for name, mutate := range map[string]func(*domain.Client){
		"concurrency": func(client *domain.Client) { client.MaxConcurrency++ },
		"rpm":         func(client *domain.Client) { client.RequestsPerMinute++ },
		"tpm":         func(client *domain.Client) { client.TokensPerMinute++ },
	} {
		t.Run(name, func(t *testing.T) {
			client := valid
			mutate(&client)
			if err := client.Validate(); err == nil {
				t.Fatalf("Validate(%+v) accepted above-maximum policy", client)
			}
		})
	}
}

func TestClientValidateUpdatePermitsOnlyUnchangedOrLoweredGrandfatheredConcurrency(t *testing.T) {
	previous := domain.Client{Name: "client", PriorityClass: domain.PriorityNormal, MaxConcurrency: domain.MaxClientConcurrency + 1}
	for _, value := range []int{previous.MaxConcurrency, domain.MaxClientConcurrency} {
		updated := previous
		updated.MaxConcurrency = value
		if err := updated.ValidateUpdate(previous); err != nil {
			t.Fatalf("ValidateUpdate(%d) error = %v", value, err)
		}
	}
	for _, value := range []int{previous.MaxConcurrency + 1, domain.MaxClientConcurrency + 2} {
		updated := previous
		updated.MaxConcurrency = value
		if err := updated.ValidateUpdate(previous); err == nil {
			t.Fatalf("ValidateUpdate(%d) accepted changed grandfathered value", value)
		}
	}
}

func TestModelPoolValidateRequiresBothNames(t *testing.T) {
	for _, pool := range []domain.ModelPool{
		{PublicModelName: "", UpstreamModelName: "upstream"},
		{PublicModelName: "public", UpstreamModelName: ""},
	} {
		if err := pool.Validate(); err == nil {
			t.Fatalf("Validate(%+v) unexpectedly succeeded", pool)
		}
	}
}

func TestModelPoolValidateAcceptsNonNegativeSafetyLimits(t *testing.T) {
	for _, limits := range []struct {
		maxGatewayInflight int
		maxWaiting         int
	}{
		{maxGatewayInflight: 0, maxWaiting: 0},
		{maxGatewayInflight: 17, maxWaiting: 9},
	} {
		pool := domain.ModelPool{
			PublicModelName: "public", UpstreamModelName: "upstream",
			MaxGatewayInflight: limits.maxGatewayInflight, MaxWaiting: limits.maxWaiting,
		}
		if err := pool.Validate(); err != nil {
			t.Fatalf("Validate(%+v) error = %v", pool, err)
		}
	}
}

func TestModelPoolValidateRejectsNegativeSafetyLimits(t *testing.T) {
	for _, limits := range []struct {
		maxGatewayInflight int
		maxWaiting         int
	}{
		{maxGatewayInflight: -1, maxWaiting: 0},
		{maxGatewayInflight: 0, maxWaiting: -1},
	} {
		pool := domain.ModelPool{
			PublicModelName: "public", UpstreamModelName: "upstream",
			MaxGatewayInflight: limits.maxGatewayInflight, MaxWaiting: limits.maxWaiting,
		}
		if err := pool.Validate(); err == nil {
			t.Fatalf("Validate(%+v) unexpectedly succeeded", pool)
		}
	}
}

func TestModelPoolValidateEnforcesProductionInflightBound(t *testing.T) {
	pool := domain.ModelPool{PublicModelName: "public", UpstreamModelName: "upstream", MaxGatewayInflight: domain.MaxPoolGatewayInflight}
	if err := pool.Validate(); err != nil {
		t.Fatalf("Validate() boundary error = %v", err)
	}
	pool.MaxGatewayInflight++
	if err := pool.Validate(); err == nil {
		t.Fatal("Validate() accepted max gateway inflight above production bound")
	}
}

func TestModelPoolValidateUpdatePermitsOnlyUnchangedOrLoweredGrandfatheredInflight(t *testing.T) {
	previous := domain.ModelPool{PublicModelName: "public", UpstreamModelName: "upstream", MaxGatewayInflight: domain.MaxPoolGatewayInflight + 1}
	unchanged := previous
	if err := unchanged.ValidateUpdate(previous); err != nil {
		t.Fatalf("unchanged grandfathered value: %v", err)
	}
	lowered := previous
	lowered.MaxGatewayInflight = domain.MaxPoolGatewayInflight
	if err := lowered.ValidateUpdate(previous); err != nil {
		t.Fatalf("lowered grandfathered value: %v", err)
	}
	increased := previous
	increased.MaxGatewayInflight++
	if err := increased.ValidateUpdate(previous); err == nil {
		t.Fatal("changed grandfathered value unexpectedly accepted")
	}
}

func TestModelPoolValidateRejectsUnusablePublicName(t *testing.T) {
	pool := domain.ModelPool{
		PublicModelName:   strings.Repeat("x", domain.MaxPublicModelNameBytes+1),
		UpstreamModelName: "upstream",
	}
	if err := pool.Validate(); err == nil {
		t.Fatal("Validate() accepted a public model name that requests cannot encode")
	}
}
