package analytics

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A stuck storage call must not permanently occupy the writer and prevent
// subsequent inference requests from reserving their completion slots.
func TestRecorderStalledWriteReleasesBackpressure(t *testing.T) {
	releaseInsert := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseInsert) })
	store := newFakeRecordStore()
	store.insertBlock = releaseInsert
	var failures atomic.Int64
	recorder := newRecorder(store, 0, func() {
		failures.Add(1)
		release()
	}, recorderSettings{queueCapacity: 1, batchSize: 1})
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = recorder.Close(ctx) // The failed batch remains an observable error.
	})
	reserveAndCompleteResponse(t, recorder, recordEvent("stalled"))
	awaitSignal(t, store.insertStarted, "stalled storage write")
	reserveAndCompleteResponse(t, recorder, recordEvent("queued"))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, rollback, ok := recorder.ReserveResponseComplete(ctx, "next-inference")
	if !ok {
		t.Fatal("stalled analytics writer prevented the next inference reservation")
	}
	rollback()
	if failures.Load() != 1 {
		t.Fatalf("failed batch count = %d, want 1", failures.Load())
	}
	awaitSignal(t, store.insertStarted, "writer recovery")
}
