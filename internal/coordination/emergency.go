package coordination

import (
	"sync"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

// EmergencyAdmission accounts for admissions on this replica during a
// recoverable coordination outage. Existing emergency work stays in this
// accounting until its request releases the allocation, even after recovery.
type EmergencyAdmission struct {
	mu                sync.Mutex
	critical          int
	high              int
	byClass           map[domain.PriorityClass]int
	byClient          map[int64]int
	clientOutstanding map[int64]int64
	poolOutstanding   map[int64]int64
	poolLegacyLeases  map[int64]int
}

func NewEmergencyAdmission(critical, high int) *EmergencyAdmission {
	return &EmergencyAdmission{
		critical: critical, high: high,
		byClass: make(map[domain.PriorityClass]int), byClient: make(map[int64]int),
		clientOutstanding: make(map[int64]int64), poolOutstanding: make(map[int64]int64), poolLegacyLeases: make(map[int64]int),
	}
}

func (e *EmergencyAdmission) Acquire(class domain.PriorityClass, clientID int64, clientLimit int) (func(), bool) {
	return e.AcquireForPool(class, clientID, 0, clientLimit)
}

func (e *EmergencyAdmission) AcquireForPool(class domain.PriorityClass, clientID, poolID int64, clientLimit int) (func(), bool) {
	release, reason, _ := e.acquire(class, clientID, poolID, clientLimit, TokenReservation{})
	return release, reason == ""
}

// AcquireTokens checks class, concurrency, and token caps atomically.
// The returned reason and scope distinguish a budget denial from an outage.
func (e *EmergencyAdmission) AcquireTokens(class domain.PriorityClass, clientID, poolID int64, clientLimit int, reservation TokenReservation, emergencyPoolBudget int64) (func(), Reason, AdmissionScope) {
	if !reservation.Enabled || !reservation.Valid() || emergencyPoolBudget <= 0 || emergencyPoolBudget > reservation.PoolBudget {
		return nil, ReasonStaleConfiguration, ""
	}
	reservation.PoolBudget = emergencyPoolBudget
	if reservation.ClientBudget == 0 {
		reservation.ClientBudget = emergencyPoolBudget
	} else {
		reservation.ClientBudget = min(reservation.ClientBudget, emergencyPoolBudget)
	}
	return e.acquire(class, clientID, poolID, clientLimit, reservation)
}

func (e *EmergencyAdmission) acquire(class domain.PriorityClass, clientID, poolID int64, clientLimit int, reservation TokenReservation) (func(), Reason, AdmissionScope) {
	if e == nil || clientLimit <= 0 {
		return nil, ReasonCoordinationUnavailable, ""
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.canServe(class, clientID, clientLimit) {
		return nil, ReasonCoordinationUnavailable, ""
	}
	if reservation.Enabled && e.poolLegacyLeases[poolID] > 0 {
		return nil, ReasonTokenPolicyTransition, ""
	}
	if scope := reservation.ExhaustedScope(e.clientOutstanding[clientID], e.poolOutstanding[poolID]); scope != "" {
		return nil, ReasonTokenBudgetExhausted, scope
	}
	e.byClass[class]++
	e.byClient[clientID]++
	if reservation.Enabled {
		e.clientOutstanding[clientID] += reservation.ReservedTokens
		e.poolOutstanding[poolID] += reservation.ReservedTokens
	} else if poolID > 0 {
		e.poolLegacyLeases[poolID]++
	}

	return sync.OnceFunc(func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.byClass[class]--
		if e.byClient[clientID] == 1 {
			delete(e.byClient, clientID)
		} else {
			e.byClient[clientID]--
		}
		if reservation.Enabled {
			subtractEmergencyTokens(e.clientOutstanding, clientID, reservation.ReservedTokens)
			subtractEmergencyTokens(e.poolOutstanding, poolID, reservation.ReservedTokens)
		} else if poolID > 0 {
			if e.poolLegacyLeases[poolID] == 1 {
				delete(e.poolLegacyLeases, poolID)
			} else {
				e.poolLegacyLeases[poolID]--
			}
		}
	}), "", ""
}

func subtractEmergencyTokens(counter map[int64]int64, id, tokens int64) {
	counter[id] -= tokens
	if counter[id] == 0 {
		delete(counter, id)
	}
}

func (e *EmergencyAdmission) CanServe(class domain.PriorityClass, clientID int64, clientLimit int) bool {
	if e == nil || clientLimit <= 0 {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.canServe(class, clientID, clientLimit)
}

func (e *EmergencyAdmission) canServe(class domain.PriorityClass, clientID int64, clientLimit int) bool {
	var cap int
	switch class {
	case domain.PriorityCritical:
		cap = e.critical
	case domain.PriorityHigh:
		cap = e.high
	}
	return cap > 0 && e.byClass[class] < cap && e.byClient[clientID] < clientLimit
}

func (e *EmergencyAdmission) HasLegacyPoolLeases(poolID int64) bool {
	if e == nil {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.poolLegacyLeases[poolID] > 0
}

func (e *EmergencyAdmission) PoolOutstandingTokens(poolID int64) int64 {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.poolOutstanding[poolID]
}
