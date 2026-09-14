package coordination

import (
	"context"
	"errors"
	"sync"
	"time"
)

type RecoveryState string

const (
	RecoveryReady          RecoveryState = "ready"
	RecoveryDegraded       RecoveryState = "degraded"
	RecoveryPermanentFault RecoveryState = "permanent_fault"
)

// RecoverySteps are deliberately explicit so no caller can accidentally skip
// or reorder part of the lossless-failover recovery contract.
type RecoverySteps struct {
	Ping                func(context.Context) error
	CheckFingerprint    func(context.Context) error
	ReloadConfiguration func(context.Context) error
	ReconcileLeases     func(context.Context) error
	ReplayFailures      func(context.Context) error
	RefreshCircuits     func(context.Context) error
}

type RecoveryStatus struct {
	State       RecoveryState
	LastError   error
	LastSuccess time.Time
}

type RecoveryObserver interface {
	CoordinationRecoveryTransition(from, to RecoveryState)
}

type RecoveryManager struct {
	steps RecoverySteps
	now   func() time.Time

	recoverMu sync.Mutex
	mu        sync.RWMutex
	status    RecoveryStatus
	failures  uint64
	observer  RecoveryObserver
}

func NewRecoveryManager(steps RecoverySteps, now func() time.Time, observers ...RecoveryObserver) *RecoveryManager {
	if now == nil {
		now = time.Now
	}
	var observer RecoveryObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &RecoveryManager{steps: steps, now: now, status: RecoveryStatus{State: RecoveryReady}, observer: observer}
}

func (m *RecoveryManager) Status() RecoveryStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

func (m *RecoveryManager) MarkDegraded(err error) {
	m.mu.Lock()
	if m.status.State == RecoveryPermanentFault {
		m.mu.Unlock()
		return
	}
	previous := m.status.State
	m.status.State = RecoveryDegraded
	m.status.LastError = err
	m.failures++
	m.mu.Unlock()
	m.observeTransition(previous, RecoveryDegraded)
}

func (m *RecoveryManager) MarkPermanent(err error) {
	m.mu.Lock()
	previous := m.status.State
	m.status.State = RecoveryPermanentFault
	m.status.LastError = err
	m.failures++
	m.mu.Unlock()
	m.observeTransition(previous, RecoveryPermanentFault)
}

func (m *RecoveryManager) CoordinationFailure(err error) {
	var permanent PermanentError
	if errors.As(err, &permanent) {
		m.MarkPermanent(err)
		return
	}
	m.MarkDegraded(err)
}

func (m *RecoveryManager) Recover(ctx context.Context) error {
	m.recoverMu.Lock()
	defer m.recoverMu.Unlock()

	m.mu.RLock()
	status := m.status
	startingFailures := m.failures
	m.mu.RUnlock()
	if status.State == RecoveryPermanentFault {
		return PermanentError{Err: status.LastError}
	}
	steps := []func(context.Context) error{
		m.steps.Ping,
		m.steps.CheckFingerprint,
		m.steps.ReloadConfiguration,
		m.steps.ReconcileLeases,
		m.steps.ReplayFailures,
		m.steps.RefreshCircuits,
	}
	for _, step := range steps {
		if step == nil {
			continue
		}
		if err := step(ctx); err != nil {
			var permanent PermanentError
			if errors.As(err, &permanent) {
				m.MarkPermanent(err)
			} else {
				m.MarkDegraded(err)
			}
			return err
		}
	}
	m.mu.Lock()
	if m.failures != startingFailures {
		status := m.status
		m.mu.Unlock()
		if status.State == RecoveryPermanentFault {
			return PermanentError{Err: status.LastError}
		}
		return status.LastError
	}
	previous := m.status.State
	m.status = RecoveryStatus{State: RecoveryReady, LastSuccess: m.now().UTC()}
	m.mu.Unlock()
	m.observeTransition(previous, RecoveryReady)
	return nil
}

func (m *RecoveryManager) observeTransition(from, to RecoveryState) {
	if from != to && m.observer != nil {
		m.observer.CoordinationRecoveryTransition(from, to)
	}
}
