package monitor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/pressure"
)

type managedWorker struct {
	worker     *Worker
	cancel     context.CancelFunc
	done       chan struct{}
	generation uint64
	breaker    *circuitbreaker.Breaker
}

type circuitReplayDropObserver interface {
	CoordinationCircuitReplayDropped(reason string, count int)
}

type Manager struct {
	ctx            context.Context
	cancel         context.CancelFunc
	observerDone   chan struct{}
	probeRenewDone chan struct{}
	options        Options

	mu             sync.Mutex
	workers        map[int64]*managedWorker
	poolMachine    map[int64]*pressure.PoolMachine
	poolRuntime    map[int64]domain.PoolRuntime
	poolInflight   map[int64]int
	nextGen        uint64
	shutdown       bool
	probes         map[uuid.UUID]coordination.ProbeIdentity
	circuitBacklog []coordination.CircuitCompletion

	replayMu   sync.Mutex
	replayDone chan struct{}
}

func NewManager(ctx context.Context, options Options) *Manager {
	if options.Circuit == (circuitbreaker.Options{}) {
		options.Circuit = circuitbreaker.Options{
			FailureThreshold: 5, FailureWindow: 30 * time.Second,
			OpenCooldown: 15 * time.Second, HalfOpenMaxProbes: 1,
		}
	}
	if options.ProbeRenewInterval <= 0 {
		options.ProbeRenewInterval = options.ProbeTTL / 3
		if options.ProbeRenewInterval <= 0 {
			options.ProbeRenewInterval = 30 * time.Second
		}
	}
	managerCtx, cancel := context.WithCancel(ctx)
	manager := &Manager{
		ctx: managerCtx, cancel: cancel, observerDone: make(chan struct{}), probeRenewDone: make(chan struct{}), options: options,
		workers: make(map[int64]*managedWorker), poolMachine: make(map[int64]*pressure.PoolMachine),
		poolRuntime: make(map[int64]domain.PoolRuntime), poolInflight: make(map[int64]int),
		probes: make(map[uuid.UUID]coordination.ProbeIdentity),
	}
	go manager.runPoolObserver()
	go manager.runProbeRenewer()
	return manager
}

func (m *Manager) Reconcile(backends []domain.Backend) error {
	if m.options.CircuitCoordinator != nil {
		identities := make([]coordination.BackendIdentity, 0, len(backends))
		for _, backend := range backends {
			identities = append(identities, coordination.BackendIdentity{ID: backend.ID, Revision: backend.Revision, Enabled: backend.Enabled, Draining: backend.Draining})
		}
		if err := m.options.CircuitCoordinator.Reconcile(m.ctx, identities); err != nil {
			return fmt.Errorf("reconcile distributed circuits: %w", err)
		}
	}
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
		breaker, err := circuitbreaker.New(m.options.Circuit)
		if err != nil {
			m.mu.Unlock()
			waitWorkers(stopped)
			return fmt.Errorf("backend %d circuit breaker: %w", id, err)
		}
		worker, err := NewWorker(backend, m.options)
		if err != nil {
			m.mu.Unlock()
			waitWorkers(stopped)
			return err
		}
		workerCtx, cancel := context.WithCancel(m.ctx)
		m.nextGen++
		managed := &managedWorker{worker: worker, cancel: cancel, done: make(chan struct{}), generation: m.nextGen, breaker: breaker}
		m.workers[id] = managed
		go func() {
			defer close(managed.done)
			worker.Run(workerCtx)
		}()
	}
	m.mu.Unlock()
	waitWorkers(stopped)
	m.observePoolRuntime(time.Now())
	return nil
}

func (m *Manager) Snapshot(backendID int64, at time.Time) domain.BackendRuntime {
	m.mu.Lock()
	managed := m.workers[backendID]
	m.mu.Unlock()
	if managed == nil {
		return domain.BackendRuntime{BackendID: backendID, State: domain.BackendUnhealthy}
	}
	return m.snapshotManaged(managed, at)
}

func (m *Manager) PoolSnapshot(poolID int64, at time.Time) domain.PoolRuntime {
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime, exists := m.poolRuntime[poolID]
	if !exists {
		runtime = domain.PoolRuntime{PoolID: poolID, State: domain.PoolUnavailable}
	}
	runtime.GatewayInflight = m.poolInflight[poolID]
	if m.options.AdmissionRuntime != nil {
		runtime.GatewayInflight = m.options.AdmissionRuntime.PoolInflight(poolID)
	}
	runtime.TotalWaiting = m.currentPoolWaitingLocked(poolID, at)
	return runtime
}

func (m *Manager) currentPoolWaitingLocked(poolID int64, at time.Time) float64 {
	total := float64(0)
	for _, managed := range m.workers {
		backend := managed.worker.Backend()
		if backend.ModelPoolID != poolID || backend.Draining {
			continue
		}
		snapshot := managed.worker.Snapshot(at)
		if snapshot.Healthy && snapshot.MetricsFresh {
			total += snapshot.Waiting
		}
	}
	return total
}

func (m *Manager) AcquirePool(poolID int64, maximum int) (func(), bool) {
	m.mu.Lock()
	if maximum < 0 || maximum > 0 && m.poolInflight[poolID] >= maximum {
		m.mu.Unlock()
		return nil, false
	}
	m.poolInflight[poolID]++
	m.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			if current := m.poolInflight[poolID]; current > 1 {
				m.poolInflight[poolID] = current - 1
			} else {
				delete(m.poolInflight, poolID)
			}
			m.mu.Unlock()
		})
	}, true
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
	if m.options.CircuitCoordinator != nil {
		if m.options.CircuitCoordinator.Refresh(m.ctx) == nil {
			_ = m.ReplayCircuitFailures(m.ctx)
		}
	}
	if m.options.AdmissionRuntime != nil {
		_ = m.options.AdmissionRuntime.RefreshInflight(m.ctx)
	}
	m.observePoolRuntime(at)
}

func (m *Manager) observePoolRuntime(at time.Time) {
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

func (m *Manager) runProbeRenewer() {
	defer close(m.probeRenewDone)
	if m.options.CircuitCoordinator == nil {
		return
	}
	ticks := m.options.ProbeRenewTicks
	stop := func() {}
	if ticks == nil {
		ticker := time.NewTicker(m.options.ProbeRenewInterval)
		ticks = ticker.C
		stop = ticker.Stop
	}
	defer stop()
	for {
		select {
		case <-ticks:
			m.renewProbes()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Manager) observePoolLocked(poolID int64, at time.Time) domain.PoolRuntime {
	best := math.Inf(1)
	available := 0
	allWaiting := true
	totalWaiting := float64(0)
	for _, managed := range m.workers {
		if managed.worker.Backend().ModelPoolID != poolID {
			continue
		}
		snapshot := m.snapshotManaged(managed, at)
		if !snapshot.Healthy || !snapshot.MetricsFresh || managed.worker.Backend().Draining {
			continue
		}
		totalWaiting += snapshot.Waiting
		if !snapshot.CircuitAvailable {
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
		GatewayInflight: m.poolInflight[poolID], TotalWaiting: totalWaiting,
	}
}

func (m *Manager) AcquireBackend(expected domain.Backend, at time.Time) (func(domain.InferenceOutcome), bool) {
	m.mu.Lock()
	managed := m.workers[expected.ID]
	if managed == nil || managed.worker.Backend() != expected {
		m.mu.Unlock()
		return nil, false
	}
	if m.options.CircuitCoordinator != nil {
		m.mu.Unlock()
		attemptID := uuid.New()
		identity := coordination.BackendIdentity{ID: expected.ID, Revision: expected.Revision, Enabled: expected.Enabled, Draining: expected.Draining}
		decision, err := m.options.CircuitCoordinator.Acquire(m.ctx, coordination.CircuitAcquireRequest{AttemptID: attemptID, AcquisitionStartedAt: at.UTC(), ReplicaID: m.options.ReplicaID, Backend: identity, ProbeTTL: m.options.ProbeTTL})
		if err != nil {
			var permanent coordination.PermanentError
			if errors.As(err, &permanent) {
				return nil, false
			}
			cached := m.options.CircuitCoordinator.Snapshot(expected.ID, at)
			if cached.State != domain.CircuitClosed {
				return nil, false
			}
			m.mu.Lock()
			managed = m.workers[expected.ID]
			if managed == nil || managed.worker.Backend() != expected {
				m.mu.Unlock()
				return nil, false
			}
			completeLocal, localOK := managed.breaker.Acquire(at)
			if !localOK || managed.breaker.Snapshot(at).State != domain.CircuitClosed {
				m.mu.Unlock()
				return nil, false
			}
			managed.worker.incrementInflight(1)
			m.mu.Unlock()
			var once sync.Once
			return func(outcome domain.InferenceOutcome) {
				once.Do(func() {
					reported := time.Now().UTC()
					completeLocal(outcome, reported)
					if outcome == domain.InferenceFailure {
						m.bufferCircuitFailure(coordination.CircuitCompletion{AttemptID: attemptID, Backend: identity, Generation: cached.Generation, Outcome: outcome, ReportedOutcomeAt: reported})
					}
					managed.worker.incrementInflight(-1)
				})
			}, true
		}
		if decision.Reason != "" {
			return nil, false
		}
		m.mu.Lock()
		managed = m.workers[expected.ID]
		if managed == nil || managed.worker.Backend() != expected {
			m.mu.Unlock()
			return nil, false
		}
		managed.worker.incrementInflight(1)
		if decision.Permit != nil {
			m.probes[decision.Permit.PermitID] = *decision.Permit
		}
		m.mu.Unlock()
		var once sync.Once
		return func(outcome domain.InferenceOutcome) {
			once.Do(func() {
				reported := time.Now().UTC()
				completion := coordination.CircuitCompletion{AttemptID: attemptID, Backend: identity, Generation: decision.Snapshot.Generation, Outcome: outcome, ReportedOutcomeAt: reported}
				if _, err := m.options.CircuitCoordinator.Complete(context.WithoutCancel(m.ctx), completion); err != nil && outcome == domain.InferenceFailure && replayableCircuitError(err) {
					m.bufferCircuitFailure(completion)
				}
				m.mu.Lock()
				delete(m.probes, attemptID)
				managed.worker.incrementInflight(-1)
				m.mu.Unlock()
			})
		}, true
	}
	completeCircuit, ok := managed.breaker.Acquire(at)
	if !ok {
		m.mu.Unlock()
		return nil, false
	}
	managed.worker.incrementInflight(1)
	m.mu.Unlock()
	var once sync.Once
	return func(outcome domain.InferenceOutcome) {
		once.Do(func() {
			completeCircuit(outcome, time.Now())
			managed.worker.incrementInflight(-1)
		})
	}, true
}

func snapshotManaged(managed *managedWorker, at time.Time) domain.BackendRuntime {
	snapshot := managed.worker.Snapshot(at)
	if managed.breaker == nil {
		return snapshot
	}
	circuit := managed.breaker.Snapshot(at)
	snapshot.CircuitState = circuit.State
	snapshot.CircuitFailures = circuit.FailureCount
	snapshot.CircuitRetryAt = circuit.RetryAt
	snapshot.CircuitProbesInFlight = circuit.ProbesInFlight
	snapshot.CircuitAvailable = circuit.Available
	return snapshot
}

func (m *Manager) snapshotManaged(managed *managedWorker, at time.Time) domain.BackendRuntime {
	if m.options.CircuitCoordinator == nil {
		return snapshotManaged(managed, at)
	}
	snapshot := managed.worker.Snapshot(at)
	circuit := m.options.CircuitCoordinator.Snapshot(managed.worker.Backend().ID, at)
	if !m.options.CircuitCoordinator.Status().Available && circuit.State == domain.CircuitClosed {
		local := managed.breaker.Snapshot(at)
		if local.State != domain.CircuitClosed {
			circuit.Available = false
		}
	}
	snapshot.CircuitState = circuit.State
	snapshot.CircuitFailures = circuit.FailureCount
	snapshot.CircuitRetryAt = circuit.RetryAt
	snapshot.CircuitProbesInFlight = circuit.ProbesInFlight
	snapshot.CircuitAvailable = circuit.Available
	return snapshot
}

// ReplayCircuitFailures delivers still-relevant, immutable failure events and
// retains only those whose durable completion is still unavailable.
func (m *Manager) ReplayCircuitFailures(ctx context.Context) error {
	done, err := m.beginCircuitReplay(ctx)
	if err != nil {
		return err
	}
	defer m.endCircuitReplay(done)

	m.mu.Lock()
	pending := m.circuitBacklog
	m.circuitBacklog = nil
	m.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	now := time.Now().UTC()
	kept := pending[:0]
	var replayErr error
	expired := 0
	for _, completion := range pending {
		if !completion.ReportedOutcomeAt.After(now.Add(-24 * time.Hour)) {
			expired++
			continue
		}
		if _, err := m.options.CircuitCoordinator.Complete(ctx, completion); err != nil && replayableCircuitError(err) {
			kept = append(kept, completion)
			replayErr = errors.Join(replayErr, err)
		}
	}
	m.mu.Lock()
	merged := append(append([]coordination.CircuitCompletion(nil), kept...), m.circuitBacklog...)
	overflow := 0
	if len(merged) > 4096 {
		overflow = len(merged) - 4096
		merged = merged[len(merged)-4096:]
	}
	m.circuitBacklog = merged
	m.mu.Unlock()
	m.observeCircuitReplayDropped("expired", expired)
	m.observeCircuitReplayDropped("overflow", overflow)
	return replayErr
}

func (m *Manager) beginCircuitReplay(ctx context.Context) (chan struct{}, error) {
	for {
		m.replayMu.Lock()
		if m.replayDone == nil {
			done := make(chan struct{})
			m.replayDone = done
			m.replayMu.Unlock()
			return done, nil
		}
		active := m.replayDone
		m.replayMu.Unlock()
		select {
		case <-active:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (m *Manager) endCircuitReplay(done chan struct{}) {
	m.replayMu.Lock()
	if m.replayDone == done {
		m.replayDone = nil
		close(done)
	}
	m.replayMu.Unlock()
}

func (m *Manager) bufferCircuitFailure(completion coordination.CircuitCompletion) {
	m.mu.Lock()
	m.circuitBacklog = append(m.circuitBacklog, completion)
	dropped := 0
	if len(m.circuitBacklog) > 4096 {
		dropped = len(m.circuitBacklog) - 4096
		m.circuitBacklog = m.circuitBacklog[len(m.circuitBacklog)-4096:]
	}
	m.mu.Unlock()
	m.observeCircuitReplayDropped("overflow", dropped)
}

func (m *Manager) observeCircuitReplayDropped(reason string, count int) {
	if count <= 0 {
		return
	}
	if observer, ok := m.options.Observer.(circuitReplayDropObserver); ok {
		observer.CoordinationCircuitReplayDropped(reason, count)
	}
}

func replayableCircuitError(err error) bool {
	var reason coordination.ReasonError
	return !errors.As(err, &reason)
}

func (m *Manager) renewProbes() {
	m.mu.Lock()
	probes := make([]coordination.ProbeIdentity, 0, len(m.probes))
	for _, probe := range m.probes {
		probes = append(probes, probe)
	}
	m.mu.Unlock()
	if len(probes) == 0 {
		return
	}
	results, err := m.options.CircuitCoordinator.RenewProbes(m.ctx, probes)
	if err != nil {
		if m.options.Observer != nil {
			m.options.Observer.CoordinationRenewFailure()
		}
		return
	}
	m.mu.Lock()
	lost := 0
	for _, result := range results {
		if result.Reason == coordination.ReasonLeaseLost {
			delete(m.probes, result.Lease.LeaseID)
			lost++
		}
	}
	m.mu.Unlock()
	for index := 0; index < lost; index++ {
		if m.options.Observer != nil {
			m.options.Observer.CoordinationLeaseLost()
		}
	}
}

func (m *Manager) WorkerCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.workers)
}

func (m *Manager) CircuitReplayBacklog() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.circuitBacklog)
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
	<-m.probeRenewDone
}

func waitWorkers(workers []*managedWorker) {
	for _, managed := range workers {
		<-managed.done
	}
}
