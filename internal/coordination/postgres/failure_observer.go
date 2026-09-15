package postgres

import (
	"context"
	"errors"
	"sync"

	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

type coordinatorHealth interface {
	setStatus(bool, coordination.Reason)
	setPermanent()
}

// recordCoordinationError is shared by every database operation so errors
// returned while reading rows receive the same treatment as query/commit errors.
// A canceled caller still receives its error, without changing shared health.
func recordCoordinationError(parent context.Context, err error, health coordinatorHealth) error {
	if err == nil || coordination.IsCallerCancellation(parent, err) {
		return err
	}
	var reason coordination.ReasonError
	if errors.As(err, &reason) {
		return err
	}
	if permanentCoordinationError(err) {
		health.setPermanent()
		var permanent coordination.PermanentError
		if !errors.As(err, &permanent) {
			return coordination.PermanentError{Err: err}
		}
	} else {
		health.setStatus(false, coordination.ReasonCoordinationUnavailable)
	}
	return err
}

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
