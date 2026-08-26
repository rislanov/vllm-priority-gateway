package admission

import "sync"

type Limiter struct {
	mu       sync.Mutex
	inflight map[int64]int
}

func NewLimiter() *Limiter {
	return &Limiter{inflight: make(map[int64]int)}
}

func (l *Limiter) Acquire(clientID int64, limit int) (*Lease, bool) {
	if limit <= 0 {
		return nil, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inflight[clientID] >= limit {
		return nil, false
	}
	l.inflight[clientID]++
	return &Lease{limiter: l, clientID: clientID}, true
}

func (l *Limiter) InFlight(clientID int64) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inflight[clientID]
}

type Lease struct {
	limiter  *Limiter
	clientID int64
	once     sync.Once
}

func (l *Lease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.limiter.mu.Lock()
		defer l.limiter.mu.Unlock()
		remaining := l.limiter.inflight[l.clientID] - 1
		if remaining <= 0 {
			delete(l.limiter.inflight, l.clientID)
			return
		}
		l.limiter.inflight[l.clientID] = remaining
	})
}
