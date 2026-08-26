package registry

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

type Data struct {
	Revision int64
	Clients  []domain.Client
	Keys     []domain.APIKey
	Pools    []domain.ModelPool
	Access   []domain.ClientModelAccess
	Backends []domain.Backend
}

type Snapshot struct {
	Revision       int64
	Clients        map[int64]domain.Client
	KeyCandidates  map[string][]domain.APIKey
	PoolsByID      map[int64]domain.ModelPool
	PoolsByName    map[string]domain.ModelPool
	Access         map[int64]map[int64]bool
	BackendsByID   map[int64]domain.Backend
	BackendsByPool map[int64][]domain.Backend
}

type Loader interface {
	LoadSnapshot(context.Context) (Data, error)
}

type Registry struct {
	loader  Loader
	current atomic.Pointer[Snapshot]
}

func New(loader Loader) *Registry {
	registry := &Registry{loader: loader}
	empty := newSnapshot()
	registry.current.Store(&empty)
	return registry
}

func (r *Registry) Reload(ctx context.Context) error {
	data, err := r.loader.LoadSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("load registry snapshot: %w", err)
	}
	snapshot, err := indexData(data)
	if err != nil {
		return err
	}
	r.current.Store(&snapshot)
	return nil
}

func (r *Registry) Snapshot() *Snapshot {
	return r.current.Load()
}

func newSnapshot() Snapshot {
	return Snapshot{
		Clients:        make(map[int64]domain.Client),
		KeyCandidates:  make(map[string][]domain.APIKey),
		PoolsByID:      make(map[int64]domain.ModelPool),
		PoolsByName:    make(map[string]domain.ModelPool),
		Access:         make(map[int64]map[int64]bool),
		BackendsByID:   make(map[int64]domain.Backend),
		BackendsByPool: make(map[int64][]domain.Backend),
	}
}

func indexData(data Data) (Snapshot, error) {
	snapshot := newSnapshot()
	snapshot.Revision = data.Revision

	for _, client := range data.Clients {
		if _, exists := snapshot.Clients[client.ID]; exists {
			return Snapshot{}, fmt.Errorf("duplicate client ID %d", client.ID)
		}
		snapshot.Clients[client.ID] = client
	}
	for _, pool := range data.Pools {
		if _, exists := snapshot.PoolsByID[pool.ID]; exists {
			return Snapshot{}, fmt.Errorf("duplicate model pool ID %d", pool.ID)
		}
		if _, exists := snapshot.PoolsByName[pool.PublicModelName]; exists {
			return Snapshot{}, fmt.Errorf("duplicate public model name %q", pool.PublicModelName)
		}
		snapshot.PoolsByID[pool.ID] = pool
		snapshot.PoolsByName[pool.PublicModelName] = pool
	}
	for _, key := range data.Keys {
		if _, exists := snapshot.Clients[key.ClientID]; !exists {
			return Snapshot{}, fmt.Errorf("API key %d references missing client %d", key.ID, key.ClientID)
		}
		snapshot.KeyCandidates[key.Prefix] = append(snapshot.KeyCandidates[key.Prefix], key)
	}
	for _, access := range data.Access {
		if _, exists := snapshot.Clients[access.ClientID]; !exists {
			return Snapshot{}, fmt.Errorf("model access references missing client %d", access.ClientID)
		}
		if _, exists := snapshot.PoolsByID[access.ModelPoolID]; !exists {
			return Snapshot{}, fmt.Errorf("model access references missing pool %d", access.ModelPoolID)
		}
		if snapshot.Access[access.ClientID] == nil {
			snapshot.Access[access.ClientID] = make(map[int64]bool)
		}
		snapshot.Access[access.ClientID][access.ModelPoolID] = access.Enabled
	}
	for _, backend := range data.Backends {
		if _, exists := snapshot.PoolsByID[backend.ModelPoolID]; !exists {
			return Snapshot{}, fmt.Errorf("backend %d references missing pool %d", backend.ID, backend.ModelPoolID)
		}
		if _, exists := snapshot.BackendsByID[backend.ID]; exists {
			return Snapshot{}, fmt.Errorf("duplicate backend ID %d", backend.ID)
		}
		snapshot.BackendsByID[backend.ID] = backend
		snapshot.BackendsByPool[backend.ModelPoolID] = append(snapshot.BackendsByPool[backend.ModelPoolID], backend)
	}
	return snapshot, nil
}
