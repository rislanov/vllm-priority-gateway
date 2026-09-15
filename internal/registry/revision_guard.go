package registry

import (
	"errors"
	"sync/atomic"
)

type RevisionGuard struct {
	highest atomic.Int64
	faulted atomic.Bool
}

func (g *RevisionGuard) ObservePublished(revision int64) {
	for {
		current := g.highest.Load()
		if revision <= current || g.highest.CompareAndSwap(current, revision) {
			return
		}
	}
}
func (g *RevisionGuard) Highest() int64 { return g.highest.Load() }
func (g *RevisionGuard) Faulted() bool  { return g.faulted.Load() }
func (g *RevisionGuard) VerifyFresh(capturedHighest, freshRevision int64) error {
	if g.faulted.Load() {
		return errors.New("configuration revision consistency fault")
	}
	if freshRevision < capturedHighest {
		g.faulted.Store(true)
		return errors.New("configuration revision regressed on writable primary")
	}
	g.ObservePublished(freshRevision)
	return nil
}
