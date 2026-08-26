package registry_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

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

func TestConcurrentReloadNeverPublishesAnOlderRevision(t *testing.T) {
	loader := &overlappingLoader{firstStarted: make(chan struct{}), releaseFirst: make(chan struct{})}
	reg := registry.New(loader)
	firstDone := make(chan error, 1)
	go func() { firstDone <- reg.Reload(context.Background()) }()
	<-loader.firstStarted
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := reg.Snapshot().Revision; got != 2 {
		t.Fatalf("revision after newer reload = %d", got)
	}
	close(loader.releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if got := reg.Snapshot().Revision; got != 2 {
		t.Fatalf("older reload rolled revision back to %d", got)
	}
}

func TestMarkKeyRevokedPublishesFailClosedOverlay(t *testing.T) {
	loader := &sequenceLoader{results: []loadResult{{data: registry.Data{
		Revision: 3,
		Clients:  []domain.Client{{ID: 1, Name: "client", Enabled: true}},
		Keys:     []domain.APIKey{{ID: 7, ClientID: 1, Prefix: "llmgw_abcd"}},
	}}}}
	reg := registry.New(loader)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	revokedAt := time.Unix(1_700_000_000, 0).UTC()
	if !reg.MarkKeyRevoked(7, revokedAt) {
		t.Fatal("key was not found")
	}
	key := reg.Snapshot().KeyCandidates["llmgw_abcd"][0]
	if key.RevokedAt == nil || !key.RevokedAt.Equal(revokedAt) || reg.Snapshot().Revision != 3 {
		t.Fatalf("overlay snapshot = %+v", reg.Snapshot())
	}
}

func TestConcurrentReloadPreservesFailClosedRevocationOverlay(t *testing.T) {
	loader := &staleKeyReloadLoader{
		key:           domain.APIKey{ID: 7, ClientID: 1, Prefix: "llmgw_abcd"},
		reloadStarted: make(chan struct{}),
		releaseReload: make(chan struct{}),
	}
	reg := registry.New(loader)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	reloadDone := make(chan error, 1)
	go func() { reloadDone <- reg.Reload(context.Background()) }()
	<-loader.reloadStarted
	revokedAt := time.Unix(1_700_000_000, 0).UTC()
	if !reg.MarkKeyRevoked(loader.key.ID, revokedAt) {
		t.Fatal("key was not found")
	}
	close(loader.releaseReload)
	if err := <-reloadDone; err != nil {
		t.Fatal(err)
	}

	snapshot := reg.Snapshot()
	key := snapshot.KeyCandidates[loader.key.Prefix][0]
	if snapshot.Revision != 2 || key.RevokedAt == nil || !key.RevokedAt.Equal(revokedAt) {
		t.Fatalf("stale reload replaced fail-closed overlay: revision=%d key=%+v", snapshot.Revision, key)
	}
}

func TestConcurrentReloadPreservesLatestUsageOverlay(t *testing.T) {
	loader := &staleKeyReloadLoader{
		key:           domain.APIKey{ID: 7, ClientID: 1, Prefix: "llmgw_abcd"},
		reloadStarted: make(chan struct{}),
		releaseReload: make(chan struct{}),
	}
	reg := registry.New(loader)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	reloadDone := make(chan error, 1)
	go func() { reloadDone <- reg.Reload(context.Background()) }()
	<-loader.reloadStarted
	usedAt := time.Unix(1_700_000_100, 0).UTC()
	if !reg.MarkKeyUsed(loader.key.ID, usedAt) {
		t.Fatal("key was not found")
	}
	close(loader.releaseReload)
	if err := <-reloadDone; err != nil {
		t.Fatal(err)
	}

	key := reg.Snapshot().KeyCandidates[loader.key.Prefix][0]
	if key.LastUsedAt == nil || !key.LastUsedAt.Equal(usedAt) {
		t.Fatalf("stale reload replaced latest usage overlay: %+v", key)
	}
}

func TestMarkKeyUsedPublishesRuntimeTimestampWithoutRevisionChange(t *testing.T) {
	loader := &sequenceLoader{results: []loadResult{{data: registry.Data{
		Revision: 3,
		Clients:  []domain.Client{{ID: 1, Name: "client", Enabled: true}},
		Keys:     []domain.APIKey{{ID: 7, ClientID: 1, Prefix: "llmgw_abcd"}},
	}}}}
	reg := registry.New(loader)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	usedAt := time.Unix(1_700_000_100, 0).UTC()
	if !reg.MarkKeyUsed(7, usedAt) {
		t.Fatal("key was not found")
	}
	key := reg.Snapshot().KeyCandidates["llmgw_abcd"][0]
	if key.LastUsedAt == nil || !key.LastUsedAt.Equal(usedAt) || reg.Snapshot().Revision != 3 {
		t.Fatalf("usage snapshot = %+v", reg.Snapshot())
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

type overlappingLoader struct {
	calls        atomic.Int64
	firstStarted chan struct{}
	releaseFirst chan struct{}
}

type staleKeyReloadLoader struct {
	calls         atomic.Int64
	key           domain.APIKey
	reloadStarted chan struct{}
	releaseReload chan struct{}
}

func (l *overlappingLoader) LoadSnapshot(context.Context) (registry.Data, error) {
	if l.calls.Add(1) == 1 {
		close(l.firstStarted)
		<-l.releaseFirst
		return registry.Data{Revision: 1}, nil
	}
	return registry.Data{Revision: 2}, nil
}

func (l *staleKeyReloadLoader) LoadSnapshot(context.Context) (registry.Data, error) {
	revision := l.calls.Add(1)
	if revision == 2 {
		close(l.reloadStarted)
		<-l.releaseReload
	}
	return registry.Data{
		Revision: revision,
		Clients:  []domain.Client{{ID: 1, Name: "client", Enabled: true}},
		Keys:     []domain.APIKey{l.key},
	}, nil
}

func (l *sequenceLoader) LoadSnapshot(context.Context) (registry.Data, error) {
	result := l.results[l.next]
	if l.next < len(l.results)-1 {
		l.next++
	}
	return result.data, result.err
}
