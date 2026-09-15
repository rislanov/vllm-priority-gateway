package apikey

import (
	"context"
	"testing"
	"time"
)

type usageStoreFunc func(context.Context, int64, time.Time) error

func (f usageStoreFunc) TouchKeyLastUsed(ctx context.Context, keyID int64, at time.Time) error {
	return f(ctx, keyID, at)
}

func TestUsageRecorderDoesNotBlockRequestsWhenStorageAndQueueAreFull(t *testing.T) {
	started := make(chan struct{})
	recorder := NewUsageRecorder(context.Background(), usageStoreFunc(func(ctx context.Context, _ int64, _ time.Time) error {
		select {
		case <-started:
		default:
			close(started)
		}
		<-ctx.Done()
		return ctx.Err()
	}))
	defer recorder.Close()
	recorder.Record(1, time.Unix(1_700_000_000, 0))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("activity writer did not start")
	}
	finished := make(chan struct{})
	go func() {
		for i := 0; i < 1024; i++ {
			recorder.Record(1, time.Unix(1_700_000_001, 0))
		}
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("request activity recording blocked behind slow storage")
	}
}

func TestUsageRecorderCoalescesSuccessfulWritesPerKey(t *testing.T) {
	writes := make(chan usageEvent, 4)
	recorder := NewUsageRecorder(context.Background(), usageStoreFunc(func(_ context.Context, keyID int64, at time.Time) error {
		writes <- usageEvent{keyID: keyID, at: at}
		return nil
	}))
	defer recorder.Close()
	assertWrite := func(keyID int64, at time.Time) {
		t.Helper()
		select {
		case event := <-writes:
			if event.keyID != keyID || !event.at.Equal(at) {
				t.Fatalf("stored activity=%+v, want key=%d at=%s", event, keyID, at)
			}
		case <-time.After(time.Second):
			t.Fatal("activity write did not finish")
		}
	}
	at := time.Unix(1_700_000_000, 0)
	recorder.Record(1, at)
	assertWrite(1, at)
	recorder.Record(1, at.Add(30*time.Second))
	recorder.Record(2, at.Add(30*time.Second))
	// The second key is a FIFO barrier: the first key's redundant activity
	// has already been processed when this durable write arrives.
	assertWrite(2, at.Add(30*time.Second))
	recorder.Record(1, at.Add(time.Minute))
	assertWrite(1, at.Add(time.Minute))
}
