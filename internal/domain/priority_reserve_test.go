package domain_test

import (
	"fmt"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"testing"
)

func TestModelPoolPriorityReserveValidation(t *testing.T) {
	for _, tt := range []struct {
		limit, reserve int
		valid          bool
	}{
		{0, 0, true}, {20, 0, true}, {20, 6, true}, {20, 20, true}, {20, -1, false}, {20, 21, false}, {0, 1, false},
	} {
		t.Run(fmt.Sprintf("%d/%d", tt.limit, tt.reserve), func(t *testing.T) {
			pool := domain.ModelPool{PublicModelName: "model", UpstreamModelName: "upstream", MaxGatewayInflight: tt.limit, HighPriorityReserve: tt.reserve}
			if err := pool.Validate(); (err == nil) != tt.valid {
				t.Fatalf("Validate()=%v valid=%v", err, tt.valid)
			}
			if err := pool.ValidateUpdate(domain.ModelPool{MaxGatewayInflight: 20}); (err == nil) != tt.valid {
				t.Fatalf("ValidateUpdate()=%v", err)
			}
		})
	}
	pool := domain.ModelPool{PublicModelName: "model", UpstreamModelName: "upstream", MaxGatewayInflight: 100001, HighPriorityReserve: 6}
	if err := pool.ValidateUpdate(domain.ModelPool{MaxGatewayInflight: 100001}); err != nil {
		t.Fatal(err)
	}
}
