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

func TestModelPoolValidateRejectsUnusablePublicName(t *testing.T) {
	pool := domain.ModelPool{
		PublicModelName:   strings.Repeat("x", domain.MaxPublicModelNameBytes+1),
		UpstreamModelName: "upstream",
	}
	if err := pool.Validate(); err == nil {
		t.Fatal("Validate() accepted a public model name that requests cannot encode")
	}
}
