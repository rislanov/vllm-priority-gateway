package store_test

import (
	"context"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

func TestSQLiteUpdateAndListConfiguration(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	firstPool, err := db.CreatePool(ctx, store.CreatePoolParams{PublicModelName: "b-model", UpstreamModelName: "b-upstream", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	secondPool, err := db.CreatePool(ctx, store.CreatePoolParams{PublicModelName: "a-model", UpstreamModelName: "a-upstream", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	client, err := db.CreateClient(ctx, store.CreateClientParams{
		Name: "old-client", Enabled: true, PriorityClass: domain.PriorityNormal,
		MaxConcurrency: 1, ModelPoolIDs: []int64{firstPool.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	backend, err := db.CreateBackend(ctx, store.CreateBackendParams{
		ModelPoolID: firstPool.ID, Name: "old-backend", BaseURL: "http://127.0.0.1:8000",
		Enabled: true, CapacityHint: 1, RunningSoftLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}

	updatedClient, err := db.UpdateClient(ctx, client.ID, store.UpdateClientParams{
		Name: "new-client", Enabled: false, PriorityClass: domain.PriorityHigh,
		VLLMPriority: -10, MaxConcurrency: 5, ModelPoolIDs: []int64{secondPool.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if updatedClient.Name != "new-client" || updatedClient.Enabled || updatedClient.CreatedAt.IsZero() || !updatedClient.UpdatedAt.Equal(updatedClient.CreatedAt) && updatedClient.UpdatedAt.Before(updatedClient.CreatedAt) {
		t.Fatalf("updated client = %+v", updatedClient)
	}
	updatedPool, err := db.UpdatePool(ctx, firstPool.ID, store.UpdatePoolParams{
		PublicModelName: "c-model", UpstreamModelName: "c-upstream", Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updatedPool.PublicModelName != "c-model" || updatedPool.Enabled {
		t.Fatalf("updated pool = %+v", updatedPool)
	}
	updatedBackend, err := db.UpdateBackend(ctx, backend.ID, store.UpdateBackendParams{
		ModelPoolID: secondPool.ID, Name: "new-backend", BaseURL: "https://gpu.internal/vllm/",
		Enabled: false, Draining: true, CapacityHint: 2, RunningSoftLimit: 12,
		UpstreamAPIKeyEnv: "VLLM_GPU_KEY",
	})
	if err != nil {
		t.Fatal(err)
	}
	if updatedBackend.BaseURL != "https://gpu.internal/vllm" || updatedBackend.ModelPoolID != secondPool.ID || !updatedBackend.Draining {
		t.Fatalf("updated backend = %+v", updatedBackend)
	}

	clients, err := db.ListClients(ctx)
	if err != nil || len(clients) != 1 || clients[0].Name != "new-client" {
		t.Fatalf("ListClients() = %+v, %v", clients, err)
	}
	pools, err := db.ListPools(ctx)
	if err != nil || len(pools) != 2 || pools[0].PublicModelName != "a-model" || pools[1].PublicModelName != "c-model" {
		t.Fatalf("ListPools() = %+v, %v", pools, err)
	}
	backends, err := db.ListBackends(ctx)
	if err != nil || len(backends) != 1 || backends[0].Name != "new-backend" {
		t.Fatalf("ListBackends() = %+v, %v", backends, err)
	}
	snapshot, err := db.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Access) != 1 || snapshot.Access[0].ModelPoolID != secondPool.ID {
		t.Fatalf("model access = %+v", snapshot.Access)
	}
}

func TestSetClientModelAccessReplacesAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	first, _ := db.CreatePool(ctx, store.CreatePoolParams{PublicModelName: "first", UpstreamModelName: "first", Enabled: true})
	second, _ := db.CreatePool(ctx, store.CreatePoolParams{PublicModelName: "second", UpstreamModelName: "second", Enabled: true})
	client, _ := db.CreateClient(ctx, store.CreateClientParams{Name: "client", Enabled: true, PriorityClass: domain.PriorityNormal, MaxConcurrency: 1, ModelPoolIDs: []int64{first.ID}})

	if err := db.SetClientModelAccess(ctx, client.ID, []int64{second.ID, second.ID}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Access) != 1 || snapshot.Access[0].ModelPoolID != second.ID {
		t.Fatalf("model access = %+v", snapshot.Access)
	}
}
