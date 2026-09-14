package coordination

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type LeaseManagerOptions struct {
	RenewInterval     time.Duration
	RenewTicks        <-chan time.Time
	CompletionBacklog int
	ShutdownTimeout   time.Duration
	Observer          LeaseManagerObserver
}

type LeaseManagerObserver interface {
	CoordinationRenewFailure()
	CoordinationLeaseLost()
}
type LeaseManagerDropObserver interface {
	CoordinationCompletionDropped()
}
type LeaseManager struct {
	ctx             context.Context
	cancel          context.CancelFunc
	coordinator     AdmissionCoordinator
	ticks           <-chan time.Time
	stopTicker      func()
	completions     chan LeaseCompletion
	backlogCap      int
	pending         []pendingCompletion
	mu              sync.Mutex
	active          map[[16]byte]LeaseIdentity
	dropped         atomic.Uint64
	done            chan struct{}
	close           sync.Once
	closeErr        error
	shutdownTimeout time.Duration
	observer        LeaseManagerObserver
}
type pendingCompletion struct {
	items   []LeaseCompletion
	next    time.Time
	backoff time.Duration
}
type LeaseHandle struct {
	manager   *LeaseManager
	lease     LeaseIdentity
	completed atomic.Bool
}

func NewLeaseManager(parent context.Context, c AdmissionCoordinator, o LeaseManagerOptions) *LeaseManager {
	ctx, cancel := context.WithCancel(parent)
	if o.CompletionBacklog <= 0 {
		o.CompletionBacklog = 4096
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 2 * time.Second
	}
	ticks := o.RenewTicks
	stop := func() {}
	if ticks == nil {
		if o.RenewInterval <= 0 {
			o.RenewInterval = 30 * time.Second
		}
		ticker := time.NewTicker(o.RenewInterval)
		ticks = ticker.C
		stop = ticker.Stop
	}
	m := &LeaseManager{ctx: ctx, cancel: cancel, coordinator: c, ticks: ticks, stopTicker: stop, completions: make(chan LeaseCompletion, o.CompletionBacklog), backlogCap: o.CompletionBacklog, active: make(map[[16]byte]LeaseIdentity), done: make(chan struct{}), shutdownTimeout: o.ShutdownTimeout, observer: o.Observer}
	go m.run()
	return m
}
func (m *LeaseManager) Track(l LeaseIdentity) *LeaseHandle {
	m.mu.Lock()
	m.active[l.LeaseID] = l
	m.mu.Unlock()
	return &LeaseHandle{manager: m, lease: l}
}
func (h *LeaseHandle) Complete(usage *TokenUsage) bool {
	if h == nil || !h.completed.CompareAndSwap(false, true) {
		return false
	}
	item := LeaseCompletion{Lease: h.lease, Usage: usage}
	select {
	case h.manager.completions <- item:
		return true
	default:
		h.manager.remove(h.lease)
		h.manager.observeDropped(1)
		return false
	}
}
func (m *LeaseManager) DroppedCompletions() uint64 { return m.dropped.Load() }
func (m *LeaseManager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.active)
}
func (m *LeaseManager) PendingCompletions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := len(m.completions)
	for _, pending := range m.pending {
		count += len(pending.items)
	}
	return count
}
func (m *LeaseManager) Close() error {
	m.close.Do(func() {
		m.cancel()
		m.stopTicker()
		<-m.done
		m.closeErr = m.drainShutdown()
	})
	return m.closeErr
}

// Reconcile renews every locally tracked lease as one batch. It is the lease
// reconciliation barrier used before PostgreSQL recovery is declared ready.
func (m *LeaseManager) Reconcile(ctx context.Context) error {
	return m.renewContext(ctx)
}

func (m *LeaseManager) drainShutdown() error {
	items := make([]LeaseCompletion, 0, len(m.completions))
	for {
		select {
		case item := <-m.completions:
			items = append(items, item)
		default:
			goto pending
		}
	}
pending:
	m.mu.Lock()
	for _, retry := range m.pending {
		items = append(items, retry.items...)
	}
	m.pending = nil
	m.mu.Unlock()
	if len(items) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.shutdownTimeout)
	defer cancel()
	backoff := 10 * time.Millisecond
	var lastErr error
	for {
		results, err := m.coordinator.Complete(ctx, items)
		items = m.remainingCompletions(items, results, err)
		if len(items) == 0 {
			return nil
		}
		lastErr = err
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			for _, item := range items {
				m.remove(item.Lease)
			}
			m.observeDropped(len(items))
			return fmt.Errorf("drain lease completions: %w", lastErr)
		case <-timer.C:
		}
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
}
func (m *LeaseManager) run() {
	defer close(m.done)
	retryTicker := time.NewTicker(25 * time.Millisecond)
	defer retryTicker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.ticks:
			m.renew()
		case item := <-m.completions:
			m.complete(item)
		case now := <-retryTicker.C:
			m.retryPending(now)
		}
	}
}
func (m *LeaseManager) renew() {
	_ = m.renewContext(m.ctx)
}

func (m *LeaseManager) renewContext(ctx context.Context) error {
	m.mu.Lock()
	batch := make([]LeaseIdentity, 0, len(m.active))
	for _, l := range m.active {
		batch = append(batch, l)
	}
	m.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}
	results, err := m.coordinator.Renew(ctx, batch)
	if err != nil {
		if m.observer != nil {
			m.observer.CoordinationRenewFailure()
		}
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range results {
		if r.Reason == ReasonLeaseLost {
			delete(m.active, r.Lease.LeaseID)
			if m.observer != nil {
				m.observer.CoordinationLeaseLost()
			}
		} else if r.Lease.LeaseID != [16]byte{} {
			m.active[r.Lease.LeaseID] = r.Lease
		}
	}
	return nil
}
func (m *LeaseManager) complete(first LeaseCompletion) {
	batch := []LeaseCompletion{first}
	for len(batch) < 64 {
		select {
		case item := <-m.completions:
			batch = append(batch, item)
		default:
			goto deliver
		}
	}
deliver:
	results, err := m.coordinator.Complete(m.ctx, batch)
	remaining := m.remainingCompletions(batch, results, err)
	if len(remaining) == 0 {
		return
	}
	m.queueRetry(pendingCompletion{items: remaining, next: time.Now().Add(10 * time.Millisecond), backoff: 10 * time.Millisecond})
}

func (m *LeaseManager) retryPending(now time.Time) {
	m.mu.Lock()
	due := make([]pendingCompletion, 0, len(m.pending))
	kept := m.pending[:0]
	for _, pending := range m.pending {
		if !pending.next.After(now) {
			due = append(due, pending)
		} else {
			kept = append(kept, pending)
		}
	}
	m.pending = kept
	m.mu.Unlock()
	for _, pending := range due {
		results, err := m.coordinator.Complete(m.ctx, pending.items)
		pending.items = m.remainingCompletions(pending.items, results, err)
		if len(pending.items) == 0 {
			continue
		}
		pending.backoff *= 2
		if pending.backoff > 5*time.Second {
			pending.backoff = 5 * time.Second
		}
		pending.next = now.Add(pending.backoff)
		m.queueRetry(pending)
	}
}

func (m *LeaseManager) remainingCompletions(items []LeaseCompletion, results []CompleteResult, err error) []LeaseCompletion {
	completed := len(items)
	completedResults := results
	if err != nil {
		completed = len(results)
		if completed > len(items) {
			completed = 0
		}
		for i := 0; i < completed; i++ {
			if results[i].LeaseID != items[i].Lease.LeaseID {
				completed = 0
				break
			}
		}
		completedResults = results[:completed]
	}
	m.observeLost(completedResults)
	for _, item := range items[:completed] {
		m.remove(item.Lease)
	}
	return items[completed:]
}

func (m *LeaseManager) queueRetry(pending pendingCompletion) {
	m.mu.Lock()
	queued := 0
	for _, existing := range m.pending {
		queued += len(existing.items)
	}
	if queued+len(pending.items) <= m.backlogCap {
		m.pending = append(m.pending, pending)
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	for _, item := range pending.items {
		m.remove(item.Lease)
	}
	m.observeDropped(len(pending.items))
}

func (m *LeaseManager) observeDropped(count int) {
	if count <= 0 {
		return
	}
	m.dropped.Add(uint64(count))
	if observer, ok := m.observer.(LeaseManagerDropObserver); ok {
		for range count {
			observer.CoordinationCompletionDropped()
		}
	}
}
func (m *LeaseManager) remove(l LeaseIdentity) {
	m.mu.Lock()
	delete(m.active, l.LeaseID)
	m.mu.Unlock()
}

func (m *LeaseManager) observeLost(results []CompleteResult) {
	if m.observer == nil {
		return
	}
	for _, result := range results {
		if result.Result == CompletionLeaseLost || result.Reason == ReasonLeaseLost ||
			(result.Result == CompletionAlreadyCompleted && result.OriginalResult == CompletionLeaseLost) {
			m.observer.CoordinationLeaseLost()
		}
	}
}
