package pressure

import (
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

type Thresholds struct {
	Busy              float64
	Saturated         float64
	Emergency         float64
	BusyRecovery      float64
	SaturatedRecovery float64
	EmergencyRecovery float64
	EnterWindow       time.Duration
	RecoveryWindow    time.Duration
}

type PoolMachine struct {
	thresholds     Thresholds
	current        domain.PoolState
	candidate      domain.PoolState
	candidateSince time.Time
	candidateSet   bool
	lastObservedAt time.Time
}

func NewPoolMachine(thresholds Thresholds) *PoolMachine {
	return &PoolMachine{thresholds: thresholds, current: domain.PoolNormal}
}

func (m *PoolMachine) Observe(at time.Time, bestPressure float64, allWaiting, available bool) domain.PoolState {
	if !m.lastObservedAt.IsZero() && at.Before(m.lastObservedAt) {
		return m.current
	}
	m.lastObservedAt = at
	if !available {
		m.current = domain.PoolUnavailable
		m.resetCandidate()
		return m.current
	}
	instantaneous := m.instantaneous(bestPressure, allWaiting)
	if m.current == domain.PoolUnavailable {
		m.current = instantaneous
		m.resetCandidate()
		return m.current
	}

	target, window := m.target(instantaneous, bestPressure, allWaiting)
	if target == m.current {
		m.resetCandidate()
		return m.current
	}
	if !m.candidateSet || m.candidate != target {
		m.candidate = target
		m.candidateSince = at
		m.candidateSet = true
		return m.current
	}
	if at.Sub(m.candidateSince) >= window {
		m.current = target
		m.resetCandidate()
	}
	return m.current
}

func (m *PoolMachine) State() domain.PoolState {
	return m.current
}

func (m *PoolMachine) instantaneous(best float64, allWaiting bool) domain.PoolState {
	switch {
	case best >= m.thresholds.Emergency:
		return domain.PoolEmergency
	case best >= m.thresholds.Saturated || allWaiting:
		return domain.PoolSaturated
	case best >= m.thresholds.Busy:
		return domain.PoolBusy
	default:
		return domain.PoolNormal
	}
}

func (m *PoolMachine) target(instantaneous domain.PoolState, best float64, allWaiting bool) (domain.PoolState, time.Duration) {
	if stateRank(instantaneous) > stateRank(m.current) {
		return instantaneous, m.thresholds.EnterWindow
	}
	switch m.current {
	case domain.PoolEmergency:
		if best < m.thresholds.EmergencyRecovery {
			return domain.PoolSaturated, m.thresholds.RecoveryWindow
		}
	case domain.PoolSaturated:
		if best < m.thresholds.SaturatedRecovery && !allWaiting {
			return domain.PoolBusy, m.thresholds.RecoveryWindow
		}
	case domain.PoolBusy:
		if best < m.thresholds.BusyRecovery {
			return domain.PoolNormal, m.thresholds.RecoveryWindow
		}
	}
	return m.current, 0
}

func (m *PoolMachine) resetCandidate() {
	m.candidate = ""
	m.candidateSince = time.Time{}
	m.candidateSet = false
}

func stateRank(state domain.PoolState) int {
	switch state {
	case domain.PoolNormal:
		return 0
	case domain.PoolBusy:
		return 1
	case domain.PoolSaturated:
		return 2
	case domain.PoolEmergency:
		return 3
	default:
		return -1
	}
}
