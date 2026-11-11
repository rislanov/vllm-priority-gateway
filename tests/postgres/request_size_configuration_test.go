package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

func TestPostgresRequestSizePolicyRoundTrip(t *testing.T) {
	db := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	ctx := context.Background()
	policy := domain.RequestSizePolicy{
		Enabled: true, EstimationProfile: "qualified-v1", MaxOutstandingTokens: 32768,
		Limits:      domain.RequestSizeLimits{MaxInputTokens: 8192, MaxOutputTokens: 2048, DefaultOutputTokens: 512},
		ClassLimits: domain.ClassRequestSizeLimits{Background: domain.RequestSizeLimits{MaxOutputTokens: 256}},
	}
	pool, err := db.CreatePool(ctx, store.CreatePoolParams{
		PublicModelName: "bounded", UpstreamModelName: "upstream", Enabled: true, RequestSizeAdmission: &policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	var maximum int64 = 64
	client, err := db.CreateClient(ctx, store.CreateClientParams{
		Name: "client", Enabled: true, PriorityClass: domain.PriorityBackground,
		MaxOutputTokens: &maximum, ModelPoolIDs: []int64{pool.ID},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.UpdatePool(ctx, pool.ID, store.UpdatePoolParams{
		PublicModelName: "renamed", UpstreamModelName: "upstream", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.UpdateClient(ctx, client.ID, store.UpdateClientParams{
		Name: "renamed client", Enabled: true, PriorityClass: domain.PriorityBackground, ModelPoolIDs: []int64{pool.ID},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gotPool, gotClient := snapshot.Pools[0], snapshot.Clients[0]
	if gotPool.RequestSizeAdmission != policy || gotPool.Revision <= pool.Revision ||
		gotClient.RequestSizeLimits.MaxOutputTokens != 64 || gotClient.Revision <= client.Revision {
		t.Fatalf("snapshot lost request size settings or revision: pool=%+v client=%+v", gotPool, gotClient)
	}
}
