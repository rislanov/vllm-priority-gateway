package postgres_test

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/contracttest"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
)

func TestPostgresLargeTokenCompletion(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createRateFixture(t, store, 0, 1)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	contracttest.LargeTokenCompletion(t, coordinator, func() coordination.AdmissionRequest {
		return admissionRequest(fixture, uuid.New())
	})
}
