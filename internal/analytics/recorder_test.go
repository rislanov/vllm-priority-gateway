package analytics

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
)

func TestRecorderFlushesAtBatchThreshold(t *testing.T) {
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 4, batchSize: 2})
	t.Cleanup(func() { closeRecorder(t, recorder) })

	recorder.Complete(recordEvent("request-1"))
	recorder.Complete(recordEvent("request-2"))

	batch := awaitBatch(t, store)
	if got := requestIDs(batch); !reflect.DeepEqual(got, []string{"request-1", "request-2"}) {
		t.Fatalf("batch request IDs = %v", got)
	}
}

func TestRecorderFlushesPendingRowsOnTimer(t *testing.T) {
	flush := make(chan time.Time)
	dequeued := make(chan struct{}, 1)
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{
		queueCapacity: 4,
		batchSize:     3,
		flush:         flush,
		afterDequeue:  func() { dequeued <- struct{}{} },
	})
	t.Cleanup(func() { closeRecorder(t, recorder) })

	recorder.Complete(recordEvent("timed-request"))
	awaitSignal(t, dequeued, "recorder dequeue")
	flush <- time.Unix(1_800_000_000, 0)

	batch := awaitBatch(t, store)
	if got := requestIDs(batch); !reflect.DeepEqual(got, []string{"timed-request"}) {
		t.Fatalf("timed batch request IDs = %v", got)
	}
}

func TestRecorderAppliesBackpressureWhenQueueIsFull(t *testing.T) {
	releaseInsert := make(chan struct{})
	enqueueAttempts := make(chan struct{}, 4)
	store := newFakeRecordStore()
	store.insertBlock = releaseInsert
	recorder := newRecorder(store, 0, nil, recorderSettings{
		queueCapacity: 1,
		batchSize:     1,
		beforeEnqueue: func() { enqueueAttempts <- struct{}{} },
	})
	t.Cleanup(func() { closeRecorder(t, recorder) })

	recorder.Complete(recordEvent("request-in-writer"))
	awaitSignal(t, enqueueAttempts, "first enqueue attempt")
	awaitSignal(t, store.insertStarted, "writer start")
	recorder.Complete(recordEvent("request-in-queue"))
	awaitSignal(t, enqueueAttempts, "second enqueue attempt")

	returned := make(chan struct{})
	go func() {
		recorder.Complete(recordEvent("request-blocked"))
		close(returned)
	}()
	awaitSignal(t, enqueueAttempts, "third enqueue attempt")
	select {
	case <-returned:
		t.Fatal("Complete returned while the recorder queue was saturated")
	default:
	}

	close(releaseInsert)
	awaitSignal(t, returned, "blocked Complete return")
}

func TestRecorderCloseDrainsAndFlushesPendingRows(t *testing.T) {
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 8, batchSize: 10})
	for _, id := range []string{"request-1", "request-2", "request-3"} {
		recorder.Complete(recordEvent(id))
	}

	closeRecorder(t, recorder)
	batch := awaitBatch(t, store)
	if got := requestIDs(batch); !reflect.DeepEqual(got, []string{"request-1", "request-2", "request-3"}) {
		t.Fatalf("drained batch request IDs = %v", got)
	}
}

func TestRecorderContinuesAfterFailedBatchAndReportsEachLostRow(t *testing.T) {
	store := newFakeRecordStore()
	writeErr := errors.New("database unavailable")
	store.insertErrors[1] = writeErr
	var failures atomic.Int64
	var logs bytes.Buffer
	recorder := newRecorder(store, 0, func() { failures.Add(1) }, recorderSettings{
		queueCapacity: 8,
		batchSize:     2,
		logger:        slog.New(slog.NewJSONHandler(&logs, nil)),
	})

	for _, id := range []string{"failed-1", "failed-2", "later-1", "later-2"} {
		event := recordEvent(id)
		event.Client = "secret-client-name"
		event.Model = "secret-model-name"
		recorder.Complete(event)
	}
	first := awaitBatch(t, store)
	second := awaitBatch(t, store)
	if got := requestIDs(first); !reflect.DeepEqual(got, []string{"failed-1", "failed-2"}) {
		t.Fatalf("failed batch request IDs = %v", got)
	}
	if got := requestIDs(second); !reflect.DeepEqual(got, []string{"later-1", "later-2"}) {
		t.Fatalf("later batch request IDs = %v", got)
	}
	if failures.Load() != 2 {
		t.Fatalf("persistence failure callbacks = %d, want 2 lost rows", failures.Load())
	}
	if output := logs.String(); !strings.Contains(output, `"count":2`) || !strings.Contains(output, `"firstRequestId":"failed-1"`) ||
		strings.Contains(output, "secret-client-name") || strings.Contains(output, "secret-model-name") {
		t.Fatalf("unsafe or incomplete persistence log = %s", output)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := recorder.Close(ctx); !errors.Is(err, writeErr) {
		t.Fatalf("Close() error = %v, want %v", err, writeErr)
	}
}

func TestRecorderCleansUpRetentionAtStartup(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	retention := 90 * 24 * time.Hour
	store := newFakeRecordStore()
	recorder := newRecorder(store, retention, nil, recorderSettings{
		queueCapacity: 1,
		batchSize:     1,
		now:           func() time.Time { return now },
	})
	t.Cleanup(func() { closeRecorder(t, recorder) })

	cutoff := awaitCleanup(t, store)
	if want := now.Add(-retention); !cutoff.Equal(want) {
		t.Fatalf("startup cleanup cutoff = %s, want %s", cutoff, want)
	}
}

func TestRecorderRunsRetentionCleanupAtMostHourly(t *testing.T) {
	startedAt := time.Unix(1_800_000_000, 0).UTC()
	cleanupTicks := make(chan time.Time)
	tickHandled := make(chan struct{}, 4)
	store := newFakeRecordStore()
	recorder := newRecorder(store, 24*time.Hour, nil, recorderSettings{
		queueCapacity:    1,
		batchSize:        1,
		cleanup:          cleanupTicks,
		now:              func() time.Time { return startedAt },
		afterCleanupTick: func() { tickHandled <- struct{}{} },
	})
	t.Cleanup(func() { closeRecorder(t, recorder) })
	_ = awaitCleanup(t, store)

	cleanupTicks <- startedAt.Add(30 * time.Minute)
	awaitSignal(t, tickHandled, "sub-hour cleanup tick")
	if calls := store.cleanupCallCount(); calls != 1 {
		t.Fatalf("cleanup calls after 30 minutes = %d, want startup only", calls)
	}

	cleanupTicks <- startedAt.Add(time.Hour)
	cutoff := awaitCleanup(t, store)
	awaitSignal(t, tickHandled, "hourly cleanup tick")
	if want := startedAt.Add(time.Hour - 24*time.Hour); !cutoff.Equal(want) {
		t.Fatalf("hourly cleanup cutoff = %s, want %s", cutoff, want)
	}

	cleanupTicks <- startedAt.Add(time.Hour)
	awaitSignal(t, tickHandled, "duplicate hourly cleanup tick")
	if calls := store.cleanupCallCount(); calls != 2 {
		t.Fatalf("cleanup calls at same hour = %d, want 2 total", calls)
	}
}

func TestRecorderDisablesRetentionCleanupAtZero(t *testing.T) {
	cleanupTicks := make(chan time.Time)
	started := make(chan struct{})
	tickHandled := make(chan struct{}, 1)
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{
		queueCapacity:    1,
		batchSize:        1,
		cleanup:          cleanupTicks,
		onStart:          func() { close(started) },
		afterCleanupTick: func() { tickHandled <- struct{}{} },
	})
	t.Cleanup(func() { closeRecorder(t, recorder) })
	awaitSignal(t, started, "recorder start")

	cleanupTicks <- time.Unix(1_800_000_000, 0)
	awaitSignal(t, tickHandled, "disabled cleanup tick")
	if calls := store.cleanupCallCount(); calls != 0 {
		t.Fatalf("cleanup calls with retention disabled = %d", calls)
	}
}

func TestRecorderIgnoresMissingStableIDsAndPersistsMissingUsage(t *testing.T) {
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 4, batchSize: 1})
	t.Cleanup(func() { closeRecorder(t, recorder) })

	missingClient := recordEvent("missing-client")
	missingClient.ClientID = 0
	recorder.Complete(missingClient)
	missingModel := recordEvent("missing-model")
	missingModel.ModelPoolID = 0
	recorder.Complete(missingModel)
	valid := recordEvent("unmetered")
	valid.Usage = nil
	recorder.Complete(valid)

	batch := awaitBatch(t, store)
	if len(batch) != 1 || batch[0].RequestID != "unmetered" {
		t.Fatalf("persisted batch = %+v", batch)
	}
	if batch[0].UsageAvailable || batch[0].InputTokens != nil || batch[0].OutputTokens != nil || batch[0].CacheReadTokens != nil {
		t.Fatalf("unmetered row has usage values: %+v", batch[0])
	}
}

func TestRecorderCompleteAndCloseAreSafeConcurrently(t *testing.T) {
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 8, batchSize: 4})
	start := make(chan struct{})
	var callers sync.WaitGroup
	for i := 0; i < 64; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			<-start
			recorder.Complete(recordEvent("concurrent-request"))
		}()
	}
	close(start)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := recorder.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	callers.Wait()
}

func recordEvent(requestID string) gateway.RequestEvent {
	cacheRead := int64(3)
	return gateway.RequestEvent{
		OccurredAt:      time.UnixMilli(1_725_000_000_456).UTC(),
		RequestID:       requestID,
		ParentRequestID: "parent-id",
		ClientID:        17,
		ModelPoolID:     23,
		Client:          "client-name",
		Model:           "model-name",
		Backend:         "gpu-a",
		Status:          200,
		Duration:        340 * time.Millisecond,
		TTFT:            125 * time.Millisecond,
		RetryCount:      2,
		Disconnect:      true,
		Usage: &domain.TokenUsage{
			InputTokens: 7, OutputTokens: 5, CacheReadTokens: &cacheRead,
		},
	}
}

func requestIDs(records []RequestRecord) []string {
	ids := make([]string, len(records))
	for i := range records {
		ids[i] = records[i].RequestID
	}
	return ids
}

func closeRecorder(t *testing.T, recorder *Recorder) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := recorder.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func awaitBatch(t *testing.T, store *fakeRecordStore) []RequestRecord {
	t.Helper()
	select {
	case batch := <-store.batchCalls:
		return batch
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for usage batch")
		return nil
	}
}

func awaitCleanup(t *testing.T, store *fakeRecordStore) time.Time {
	t.Helper()
	select {
	case cutoff := <-store.cleanupCalls:
		return cutoff
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retention cleanup")
		return time.Time{}
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type fakeRecordStore struct {
	mu            sync.Mutex
	insertCalls   int
	insertErrors  map[int]error
	insertBlock   <-chan struct{}
	insertStarted chan struct{}
	batchCalls    chan []RequestRecord
	cleanupCalls  chan time.Time
	cleanupCount  int
}

func newFakeRecordStore() *fakeRecordStore {
	return &fakeRecordStore{
		insertErrors:  make(map[int]error),
		insertStarted: make(chan struct{}, 16),
		batchCalls:    make(chan []RequestRecord, 16),
		cleanupCalls:  make(chan time.Time, 16),
	}
}

func (s *fakeRecordStore) InsertUsageBatch(ctx context.Context, records []RequestRecord) error {
	s.mu.Lock()
	s.insertCalls++
	call := s.insertCalls
	err := s.insertErrors[call]
	s.mu.Unlock()

	batch := append([]RequestRecord(nil), records...)
	s.batchCalls <- batch
	s.insertStarted <- struct{}{}
	if s.insertBlock != nil {
		select {
		case <-s.insertBlock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (s *fakeRecordStore) DeleteUsageBefore(_ context.Context, cutoff time.Time) (int64, error) {
	s.mu.Lock()
	s.cleanupCount++
	s.mu.Unlock()
	s.cleanupCalls <- cutoff
	return 0, nil
}

func (s *fakeRecordStore) cleanupCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cleanupCount
}
