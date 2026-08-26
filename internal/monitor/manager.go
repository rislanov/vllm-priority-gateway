package monitor

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/pressure"
)

type managedWorker struct {
	worker     *Worker
	cancel     context.CancelFunc
	done       chan struct{}
	generation uint64
}

type Manager struct {
	ctx          context.Context
	cancel       context.CancelFunc
	observerDone chan struct{}
	options      Options

	mu          sync.Mutex
	workers     map[int64]*managedWorker
	poolMachine map[int64]*pressure.PoolMachine
	poolRuntime map[int64]domain.PoolRuntime
	nextGen     uint64
	shutdown    bool
}

func NewManager(ctx context.Context, options Options) *Manager {
	managerCtx, cancel := context.WithCancel(ctx)
	manager := &Manager{
		ctx: managerCtx, cancel: cancel, observerDone: make(chan struct{}), options: options,
		workers: make(map[int64]*managedWorker), poolMachine: make(map[int64]*pressure.PoolMachine),
		poolRuntime: make(map[int64]domain.PoolRuntime),
	}
	go manager.runPoolObserver()
	return manager
}

func (m *Manager) Reconcile(backends []domain.Backend) error {
	desired := make(map[int64]domain.Backend, len(backends))
	for _, backend := range backends {
		if !backend.Enabled {
			continue
		}
		if _, exists := desired[backend.ID]; exists {
			return fmt.Errorf("duplicate backend ID %d", backend.ID)
		}
		if err := backend.Validate(); err != nil {
			return fmt.Errorf("backend %d: %w", backend.ID, err)
		}
		desired[backend.ID] = backend
	}

	m.mu.Lock()
	if m.shutdown {
		m.mu.Unlock()
		return context.Canceled
	}
	var stopped []*managedWorker
	for id, managed := range m.workers {
		backend, keep := desired[id]
		if keep && backend == managed.worker.Backend() {
			delete(desired, id)
			continue
		}
		managed.cancel()
		stopped = append(stopped, managed)
		delete(m.workers, id)
	}
	for id, backend := range desired {
		worker, err := NewWorker(backend, m.options)
		if err != nil {
			m.mu.Unlock()
			waitWorkers(stopped)
			return err
		}
		workerCtx, cancel := context.WithCancel(m.ctx)
		m.nextGen++
		managed := &managedWorker{worker: worker, cancel: cancel, done: make(chan struct{}), generation: m.nextGen}
		m.workers[id] = managed
		go func() {
			defer close(managed.done)
			worker.Run(workerCtx)
		}()
	}
	m.mu.Unlock()
	waitWorkers(stopped)
	m.observePools(time.Now())
	return nil
}

func (m *Manager) Snapshot(backendID int64, at time.Time) domain.BackendRuntime {
	m.mu.Lock()
	managed := m.workers[backendID]
	m.mu.Unlock()
	if managed == nil {
		return domain.BackendRuntime{BackendID: backendID, State: domain.BackendUnhealthy}
	}
	return managed.worker.Snapshot(at)
}

func (m *Manager) PoolSnapshot(poolID int64, at time.Time) domain.PoolRuntime {
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime, exists := m.poolRuntime[poolID]
	if !exists {
		return domain.PoolRuntime{PoolID: poolID, State: domain.PoolUnavailable}
	}
	return runtime
}

func (m *Manager) runPoolObserver() {
	defer close(m.observerDone)
	interval := m.options.MetricsInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case at := <-ticker.C:
			m.observePools(at)
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Manager) observePools(at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shutdown {
		return
	}
	active := make(map[int64]struct{})
	for _, managed := range m.workers {
		active[managed.worker.Backend().ModelPoolID] = struct{}{}
	}
	for poolID := range m.poolMachine {
		if _, exists := active[poolID]; !exists {
			delete(m.poolMachine, poolID)
			delete(m.poolRuntime, poolID)
		}
	}
	for poolID := range active {
		m.poolRuntime[poolID] = m.observePoolLocked(poolID, at)
	}
}

func (m *Manager) observePoolLocked(poolID int64, at time.Time) domain.PoolRuntime {
	best := math.Inf(1)
	available := 0
	allWaiting := true
	for _, managed := range m.workers {
		if managed.worker.Backend().ModelPoolID != poolID {
			continue
		}
		snapshot := managed.worker.Snapshot(at)
		if !snapshot.Healthy || !snapshot.MetricsFresh || managed.worker.Backend().Draining {
			continue
		}
		available++
		if snapshot.Pressure < best {
			best = snapshot.Pressure
		}
		if snapshot.Waiting <= 0 {
			allWaiting = false
		}
	}
	machine := m.poolMachine[poolID]
	if machine == nil {
		machine = pressure.NewPoolMachine(m.options.PoolThresholds)
		m.poolMachine[poolID] = machine
	}
	state := machine.Observe(at, best, allWaiting && available > 0, available > 0)
	if available == 0 {
		best = 0
		allWaiting = false
	}
	return domain.PoolRuntime{
		PoolID: poolID, State: state, BestBackendPressure: best,
		AvailableBackends: available, AllBackendsWaiting: allWaiting,
	}
}

func (m *Manager) IncrementInflight(backendID int64) (func(), bool) {
	m.mu.Lock()
	managed := m.workers[backendID]
	m.mu.Unlock()
	if managed == nil {
		return nil, false
	}
	managed.worker.incrementInflight(1)
	var once sync.Once
	return func() { once.Do(func() { managed.worker.incrementInflight(-1) }) }, true
}

func (m *Manager) WorkerCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.workers)
}

func (m *Manager) HasWorker(id int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.workers[id] != nil
}

func (m *Manager) WorkerGeneration(id int64) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.workers[id] == nil {
		return 0
	}
	return m.workers[id].generation
}

func (m *Manager) Shutdown() {
	m.mu.Lock()
	if m.shutdown {
		m.mu.Unlock()
		return
	}
	m.shutdown = true
	m.cancel()
	workers := make([]*managedWorker, 0, len(m.workers))
	for _, managed := range m.workers {
		managed.cancel()
		workers = append(workers, managed)
	}
	m.workers = make(map[int64]*managedWorker)
	m.mu.Unlock()
	waitWorkers(workers)
	<-m.observerDone
}

func waitWorkers(workers []*managedWorker) {
	for _, managed := range workers {
		<-managed.done
	}
}
