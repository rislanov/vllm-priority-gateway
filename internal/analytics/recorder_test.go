package analytics

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strconv"
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

	reserveAndCompleteResponse(t, recorder, recordEvent("request-1"))
	reserveAndCompleteResponse(t, recorder, recordEvent("request-2"))

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

	reserveAndCompleteResponse(t, recorder, recordEvent("timed-request"))
	awaitSignal(t, dequeued, "recorder dequeue")
	flush <- time.Unix(1_800_000_000, 0)

	batch := awaitBatch(t, store)
	if got := requestIDs(batch); !reflect.DeepEqual(got, []string{"timed-request"}) {
		t.Fatalf("timed batch request IDs = %v", got)
	}
}

func TestRecorderAppliesBackpressureBeforeCompletionWhenQueueIsFull(t *testing.T) {
	releaseInsert := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseInsert) })
	store := newFakeRecordStore()
	store.insertBlock = releaseInsert
	recorder := newRecorder(store, 0, nil, recorderSettings{
		queueCapacity: 1,
		batchSize:     1,
	})
	t.Cleanup(func() {
		release()
		closeRecorder(t, recorder)
	})

	reserveAndCompleteResponse(t, recorder, recordEvent("request-in-writer"))
	awaitSignal(t, store.insertStarted, "writer start")
	reserveAndCompleteResponse(t, recorder, recordEvent("request-in-queue"))

	returned := make(chan struct{})
	go func() {
		reserveAndCompleteResponse(t, recorder, recordEvent("request-blocked"))
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("reservation returned while the recorder queue was saturated")
	case <-time.After(20 * time.Millisecond):
	}

	release()
	awaitSignal(t, returned, "blocked reservation return")
}

func TestRecorderReservationBackpressureHonorsCancellationWithoutStagingRow(t *testing.T) {
	releaseInsert := make(chan struct{})
	store := newFakeRecordStore()
	store.insertBlock = releaseInsert
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 1, batchSize: 1})
	t.Cleanup(func() {
		close(releaseInsert)
		closeRecorder(t, recorder)
	})

	reserveAndCompleteResponse(t, recorder, recordEvent("request-in-writer"))
	awaitSignal(t, store.insertStarted, "writer start")
	reserveAndCompleteResponse(t, recorder, recordEvent("request-in-queue"))

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	returned := make(chan bool, 1)
	go func() {
		close(started)
		_, reserved := recorder.ReserveResponseComplete(ctx, "request-cancelled")
		returned <- reserved
	}()
	awaitSignal(t, started, "cancelled reservation start")
	select {
	case reserved := <-returned:
		t.Fatalf("saturated reservation returned before cancellation: reserved=%t", reserved)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case reserved := <-returned:
		if reserved {
			t.Fatal("canceled saturated reservation succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled saturated reservation did not return")
	}
	recorder.mu.Lock()
	_, staged := recorder.pending["request-cancelled"]
	_, held := recorder.reserved["request-cancelled"]
	recorder.mu.Unlock()
	if staged || held {
		t.Fatalf("canceled reservation retained state: staged=%t reserved=%t", staged, held)
	}
}

func TestRecorderRejectsAlreadyCanceledReservationWhenCapacityIsAvailable(t *testing.T) {
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 1, batchSize: 1})
	t.Cleanup(func() { closeRecorder(t, recorder) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for index := 0; index < 128; index++ {
		requestID := "already-canceled-" + strconv.Itoa(index)
		rollback, reserved := recorder.ReserveResponseComplete(ctx, requestID)
		if reserved {
			rollback()
			t.Fatalf("already-canceled reservation %d acquired available capacity", index)
		}
	}
}

func TestRecorderRejectsDuplicateReservationWithoutStealingOriginal(t *testing.T) {
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 2, batchSize: 1})
	t.Cleanup(func() { closeRecorder(t, recorder) })

	_, reserved := recorder.ReserveResponseComplete(context.Background(), "duplicate-request")
	if !reserved {
		t.Fatal("first reservation was refused")
	}
	duplicateRollback, duplicateReserved := recorder.ReserveResponseComplete(context.Background(), "duplicate-request")
	if duplicateReserved {
		if duplicateRollback != nil {
			duplicateRollback()
		}
		t.Fatal("duplicate request ID acquired a second reservation")
	}

	recorder.Complete(recordEvent("duplicate-request"))
	recorder.ResponseComplete("duplicate-request")
	batch := awaitBatch(t, store)
	if got := requestIDs(batch); !reflect.DeepEqual(got, []string{"duplicate-request"}) {
		t.Fatalf("persisted request IDs = %v", got)
	}
}

func TestRecorderStaleRollbackCannotReleaseReusedRequestID(t *testing.T) {
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 1, batchSize: 1})
	t.Cleanup(func() { closeRecorder(t, recorder) })

	staleRollback, reserved := recorder.ReserveResponseComplete(context.Background(), "reused-request")
	if !reserved {
		t.Fatal("first reservation was refused")
	}
	recorder.Complete(recordEvent("reused-request"))
	recorder.ResponseComplete("reused-request")
	_ = awaitBatch(t, store)

	_, reserved = recorder.ReserveResponseComplete(context.Background(), "reused-request")
	if !reserved {
		t.Fatal("reused request ID reservation was refused after prior completion")
	}
	staleRollback()
	recorder.Complete(recordEvent("reused-request"))
	recorder.ResponseComplete("reused-request")
	batch := awaitBatch(t, store)
	if got := requestIDs(batch); !reflect.DeepEqual(got, []string{"reused-request"}) {
		t.Fatalf("persisted reused request IDs = %v", got)
	}
}

func TestRecorderReservedTerminalHandoffUsesItsPreacquiredQueueSlot(t *testing.T) {
	writerStarted := make(chan struct{})
	releaseWriter := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseWriter) })
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{
		queueCapacity: 2,
		batchSize:     2,
		onStart: func() {
			close(writerStarted)
			<-releaseWriter
		},
	})
	t.Cleanup(func() {
		release()
		closeRecorder(t, recorder)
	})
	awaitSignal(t, writerStarted, "paused writer start")

	targetRollback, reserved := recorder.ReserveResponseComplete(context.Background(), "target-request")
	if !reserved {
		t.Fatal("target reservation was refused")
	}
	t.Cleanup(targetRollback)
	if _, reserved := recorder.ReserveResponseComplete(context.Background(), "queued-request"); !reserved {
		t.Fatal("queued reservation was refused")
	}
	recorder.Complete(recordEvent("queued-request"))
	recorder.ResponseComplete("queued-request")
	if got := len(recorder.permits); got != 2 {
		t.Fatalf("held recorder permits after terminal enqueue = %d, want 2 until writer dequeue", got)
	}

	recorder.Complete(recordEvent("target-request"))
	returned := make(chan struct{})
	go func() {
		recorder.ResponseComplete("target-request")
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("reserved terminal handoff waited for queue capacity")
	}
	if got := len(recorder.events); got != 2 {
		t.Fatalf("queued terminal records = %d, want full two-slot queue", got)
	}
}

func TestRecorderCloseWaitsForReservedTerminalHandoffWithoutClosingQueue(t *testing.T) {
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 1, batchSize: 1})
	_, reserved := recorder.ReserveResponseComplete(context.Background(), "shutdown-request")
	if !reserved {
		t.Fatal("reservation was refused")
	}
	recorder.Complete(recordEvent("shutdown-request"))
	blockedStarted := make(chan struct{})
	blockedReturned := make(chan bool, 1)
	go func() {
		close(blockedStarted)
		_, blockedReturnedValue := recorder.ReserveResponseComplete(context.Background(), "blocked-at-shutdown")
		blockedReturned <- blockedReturnedValue
	}()
	awaitSignal(t, blockedStarted, "blocked shutdown reservation start")
	select {
	case reservedAtShutdown := <-blockedReturned:
		t.Fatalf("saturated shutdown reservation returned before Close: reserved=%t", reservedAtShutdown)
	case <-time.After(20 * time.Millisecond):
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	closeReturned := make(chan error, 1)
	go func() { closeReturned <- recorder.Close(ctx) }()
	select {
	case err := <-closeReturned:
		t.Fatalf("Close() returned before the reserved terminal handoff: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case reservedAtShutdown := <-blockedReturned:
		if reservedAtShutdown {
			t.Fatal("reservation blocked at shutdown unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("reservation blocked on capacity did not wake when Close stopped intake")
	}

	recorder.ResponseComplete("shutdown-request")
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not finish after the reserved terminal handoff")
	}
	batch := awaitBatch(t, store)
	if got := requestIDs(batch); !reflect.DeepEqual(got, []string{"shutdown-request"}) {
		t.Fatalf("shutdown batch request IDs = %v", got)
	}
}

func TestRecorderCloseDrainsAndFlushesPendingRows(t *testing.T) {
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 8, batchSize: 10})
	for _, id := range []string{"request-1", "request-2", "request-3"} {
		reserveAndCompleteResponse(t, recorder, recordEvent(id))
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
		reserveAndCompleteResponse(t, recorder, event)
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
	actualNow := startedAt
	var clockMu sync.Mutex
	now := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return actualNow
	}
	setNow := func(value time.Time) {
		clockMu.Lock()
		actualNow = value
		clockMu.Unlock()
	}
	cleanupTicks := make(chan time.Time)
	tickHandled := make(chan struct{}, 4)
	store := newFakeRecordStore()
	recorder := newRecorder(store, 24*time.Hour, nil, recorderSettings{
		queueCapacity:    1,
		batchSize:        1,
		cleanup:          cleanupTicks,
		now:              now,
		afterCleanupTick: func() { tickHandled <- struct{}{} },
	})
	t.Cleanup(func() { closeRecorder(t, recorder) })
	_ = awaitCleanup(t, store)

	setNow(startedAt.Add(30 * time.Minute))
	cleanupTicks <- startedAt.Add(2 * time.Hour)
	awaitSignal(t, tickHandled, "sub-hour cleanup tick")
	if calls := store.cleanupCallCount(); calls != 1 {
		t.Fatalf("cleanup calls after 30 minutes = %d, want startup only", calls)
	}

	setNow(startedAt.Add(time.Hour))
	cleanupTicks <- startedAt.Add(30 * time.Minute)
	cutoff := awaitCleanup(t, store)
	awaitSignal(t, tickHandled, "hourly cleanup tick")
	if want := startedAt.Add(time.Hour - 24*time.Hour); !cutoff.Equal(want) {
		t.Fatalf("hourly cleanup cutoff = %s, want %s", cutoff, want)
	}

	setNow(startedAt.Add(90 * time.Minute))
	cleanupTicks <- startedAt.Add(3 * time.Hour)
	awaitSignal(t, tickHandled, "stale duplicate cleanup tick")
	if calls := store.cleanupCallCount(); calls != 2 {
		t.Fatalf("cleanup calls within an actual hour = %d, want 2 total", calls)
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
	reserveAndCompleteResponse(t, recorder, missingClient)
	missingModel := recordEvent("missing-model")
	missingModel.ModelPoolID = 0
	reserveAndCompleteResponse(t, recorder, missingModel)
	valid := recordEvent("unmetered")
	valid.Usage = nil
	reserveAndCompleteResponse(t, recorder, valid)

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
		go func(requestID string) {
			defer callers.Done()
			<-start
			event := recordEvent(requestID)
			if _, reserved := recorder.ReserveResponseComplete(context.Background(), event.RequestID); !reserved {
				return
			}
			recorder.Complete(event)
			recorder.ResponseComplete(event.RequestID)
		}("concurrent-request-" + strconv.Itoa(i))
	}
	close(start)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := recorder.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	callers.Wait()
}

func TestRecorderKeepsOnePendingRecordAndEnqueuesResponseOnce(t *testing.T) {
	store := newFakeRecordStore()
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 2, batchSize: 1})
	t.Cleanup(func() { closeRecorder(t, recorder) })
	event := recordEvent("same-request")
	if _, reserved := recorder.ReserveResponseComplete(context.Background(), event.RequestID); !reserved {
		t.Fatal("reservation was refused")
	}

	recorder.Complete(event)
	recorder.Complete(event)
	recorder.mu.Lock()
	pending := len(recorder.pending)
	recorder.mu.Unlock()
	if pending != 1 {
		t.Fatalf("pending records for duplicate Complete = %d, want 1", pending)
	}

	recorder.ResponseComplete(event.RequestID)
	recorder.ResponseComplete(event.RequestID)
	batch := awaitBatch(t, store)
	if got := requestIDs(batch); !reflect.DeepEqual(got, []string{"same-request"}) {
		t.Fatalf("enqueued request IDs = %v", got)
	}
	select {
	case duplicate := <-store.batchCalls:
		t.Fatalf("duplicate response completion enqueued %+v", duplicate)
	default:
	}
}

func TestRecorderCloseReturnsAtDeadlineWhileNonCooperativeStoreFinishesLater(t *testing.T) {
	releaseInsert := make(chan struct{})
	store := newFakeRecordStore()
	store.insertBlock = releaseInsert
	store.ignoreInsertContext = true
	recorder := newRecorder(store, 0, nil, recorderSettings{queueCapacity: 1, batchSize: 1})
	reserveAndCompleteResponse(t, recorder, recordEvent("blocked-store"))
	awaitSignal(t, store.insertStarted, "non-cooperative insert start")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	returned := make(chan error, 1)
	go func() { returned <- recorder.Close(ctx) }()
	select {
	case err := <-returned:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close() error = %v, want deadline exceeded", err)
		}
	case <-time.After(200 * time.Millisecond):
		close(releaseInsert)
		t.Fatal("Close() waited for a non-cooperative store beyond its deadline")
	}
	select {
	case <-recorder.Done():
		t.Fatal("recorder worker finished while store remained blocked")
	default:
	}
	close(releaseInsert)
	awaitSignal(t, recorder.Done(), "recorder worker completion")
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

func completeResponse(recorder *Recorder, event gateway.RequestEvent) {
	recorder.Complete(event)
	recorder.ResponseComplete(event.RequestID)
}

func reserveAndCompleteResponse(t *testing.T, recorder *Recorder, event gateway.RequestEvent) {
	t.Helper()
	_, reserved := recorder.ReserveResponseComplete(context.Background(), event.RequestID)
	if !reserved {
		t.Fatalf("ReserveResponseComplete(%q) refused", event.RequestID)
	}
	completeResponse(recorder, event)
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
	mu                  sync.Mutex
	insertCalls         int
	insertErrors        map[int]error
	insertBlock         <-chan struct{}
	ignoreInsertContext bool
	insertStarted       chan struct{}
	batchCalls          chan []RequestRecord
	cleanupCalls        chan time.Time
	cleanupCount        int
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
		if s.ignoreInsertContext {
			<-s.insertBlock
			return err
		}
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
