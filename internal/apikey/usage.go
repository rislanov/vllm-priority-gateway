package apikey

import (
	"context"
	"time"
)

type UsageStore interface {
	TouchKeyLastUsed(context.Context, int64, time.Time) error
}

type usageEvent struct {
	keyID int64
	at    time.Time
}

// UsageRecorder keeps best-effort key activity writes off the request path.
// Successful writes for the same key are limited to one per minute.
type UsageRecorder struct {
	cancel context.CancelFunc
	done   chan struct{}
	events chan usageEvent
}

func NewUsageRecorder(parent context.Context, destination UsageStore) *UsageRecorder {
	ctx, cancel := context.WithCancel(parent)
	recorder := &UsageRecorder{cancel: cancel, done: make(chan struct{}), events: make(chan usageEvent, 256)}
	go func() {
		defer close(recorder.done)
		last := make(map[int64]time.Time)
		for {
			select {
			case event := <-recorder.events:
				if previous := last[event.keyID]; !previous.IsZero() && event.at.Sub(previous) < time.Minute {
					continue
				}
				if destination.TouchKeyLastUsed(ctx, event.keyID, event.at) == nil {
					last[event.keyID] = event.at
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return recorder
}

func (r *UsageRecorder) Record(keyID int64, usedAt time.Time) {
	select {
	case r.events <- usageEvent{keyID: keyID, at: usedAt}:
	default:
	}
}

func (r *UsageRecorder) Close() {
	r.cancel()
	<-r.done
}
