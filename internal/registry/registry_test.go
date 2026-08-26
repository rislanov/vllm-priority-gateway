package registry_test

import (
	"context"
	"errors"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
)

func TestReloadBuildsIndexedImmutableSnapshot(t *testing.T) {
	loader := &sequenceLoader{results: []loadResult{{data: registry.Data{
		Revision: 4,
		Clients:  []domain.Client{{ID: 1, Name: "client", Enabled: true}},
		Pools:    []domain.ModelPool{{ID: 2, PublicModelName: "public", Enabled: true}},
		Backends: []domain.Backend{{ID: 3, ModelPoolID: 2, Name: "gpu", Enabled: true}},
		Keys:     []domain.APIKey{{ID: 4, ClientID: 1, Prefix: "llmgw_abcd"}},
		Access:   []domain.ClientModelAccess{{ClientID: 1, ModelPoolID: 2, Enabled: true}},
	}}}}
	reg := registry.New(loader)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
	snapshot := reg.Snapshot()
	if snapshot.Revision != 4 || snapshot.PoolsByName["public"].ID != 2 || snapshot.BackendsByPool[2][0].ID != 3 || snapshot.KeyCandidates["llmgw_abcd"][0].ID != 4 || !snapshot.Access[1][2] {
		t.Fatalf("indexed snapshot = %+v", snapshot)
	}
	loader.results[0].data.Clients[0].Name = "mutated"
	if reg.Snapshot().Clients[1].Name != "client" {
		t.Fatal("published snapshot aliases loader data")
	}
}

func TestReloadFailurePreservesPublishedSnapshot(t *testing.T) {
	loader := &sequenceLoader{results: []loadResult{
		{data: registry.Data{Revision: 1}},
		{err: errors.New("database unavailable")},
	}}
	reg := registry.New(loader)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := reg.Reload(context.Background()); err == nil {
		t.Fatal("expected reload error")
	}
	if got := reg.Snapshot().Revision; got != 1 {
		t.Fatalf("Revision = %d, want 1", got)
	}
}

type loadResult struct {
	data registry.Data
	err  error
}

type sequenceLoader struct {
	results []loadResult
	next    int
}

func (l *sequenceLoader) LoadSnapshot(context.Context) (registry.Data, error) {
	result := l.results[l.next]
	if l.next < len(l.results)-1 {
		l.next++
	}
	return result.data, result.err
}
