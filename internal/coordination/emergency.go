package coordination

import (
	"sync"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

// EmergencyAdmission is deliberately process-local. It is used only while the
// durable coordinator is unavailable, so its configured class caps are also
// the documented per-replica outage bounds.
type EmergencyAdmission struct {
	mu       sync.Mutex
	critical int
	high     int
	byClass  map[domain.PriorityClass]int
	byClient map[int64]int
}

func NewEmergencyAdmission(critical, high int) *EmergencyAdmission {
	return &EmergencyAdmission{critical: critical, high: high, byClass: make(map[domain.PriorityClass]int), byClient: make(map[int64]int)}
}

func (e *EmergencyAdmission) Acquire(class domain.PriorityClass, clientID int64, clientLimit int) (func(), bool) {
	if e == nil || clientLimit <= 0 {
		return nil, false
	}
	cap := 0
	switch class {
	case domain.PriorityCritical:
		cap = e.critical
	case domain.PriorityHigh:
		cap = e.high
	default:
		return nil, false
	}
	if cap <= 0 {
		return nil, false
	}
	e.mu.Lock()
	if e.byClass[class] >= cap || e.byClient[clientID] >= clientLimit {
		e.mu.Unlock()
		return nil, false
	}
	e.byClass[class]++
	e.byClient[clientID]++
	e.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			e.mu.Lock()
			e.byClass[class]--
			e.byClient[clientID]--
			e.mu.Unlock()
		})
	}, true
}

func (e *EmergencyAdmission) CanServe(class domain.PriorityClass, clientID int64, clientLimit int) bool {
	if e == nil || clientLimit <= 0 {
		return false
	}
	cap := 0
	switch class {
	case domain.PriorityCritical:
		cap = e.critical
	case domain.PriorityHigh:
		cap = e.high
	default:
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return cap > 0 && e.byClass[class] < cap && e.byClient[clientID] < clientLimit
}
