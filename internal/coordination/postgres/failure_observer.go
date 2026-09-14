package postgres

import (
	"errors"
	"sync"

	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

type failurePublisher struct {
	mu       sync.RWMutex
	observer coordination.CoordinationFailureObserver
}

func (p *failurePublisher) set(observer coordination.CoordinationFailureObserver) {
	p.mu.Lock()
	p.observer = observer
	p.mu.Unlock()
}

func (p *failurePublisher) report(err error) {
	if err == nil {
		return
	}
	p.mu.RLock()
	observer := p.observer
	p.mu.RUnlock()
	if observer != nil {
		observer.CoordinationFailure(err)
	}
}

func (p *failurePublisher) unavailable() {
	p.report(errors.New("PostgreSQL coordination unavailable"))
}

func (p *failurePublisher) permanent() {
	p.report(coordination.PermanentError{Err: errors.New("PostgreSQL coordination consistency fault")})
}
