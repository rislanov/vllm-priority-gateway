package analytics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
)

const (
	recorderQueueCapacity  = 1024
	recorderBatchSize      = 64
	recorderFlushInterval  = 250 * time.Millisecond
	retentionCheckInterval = time.Hour
)

// RecordStore is the recorder's narrow durable-storage dependency.
type RecordStore interface {
	InsertUsageBatch(context.Context, []RequestRecord) error
	DeleteUsageBefore(context.Context, time.Time) (int64, error)
}

// Recorder persists completed request metadata from a bounded asynchronous
// queue. It implements gateway.Observer; inflight events are intentionally no-ops.
type Recorder struct {
	store     RecordStore
	retention time.Duration
	onFailure func()
	logger    *slog.Logger
	settings  recorderSettings

	ctx    context.Context
	cancel context.CancelFunc
	events chan RequestRecord
	done   chan struct{}

	mu        sync.Mutex
	accepting bool
	pending   map[string]RequestRecord
	senders   sync.WaitGroup
	closeOnce sync.Once
	lastErr   error
}

type recorderSettings struct {
	queueCapacity    int
	batchSize        int
	flush            <-chan time.Time
	cleanup          <-chan time.Time
	now              func() time.Time
	logger           *slog.Logger
	stopTimers       func()
	beforeEnqueue    func()
	afterDequeue     func()
	onStart          func()
	afterCleanupTick func()
}

// NewRecorder starts one writer for request metadata and retention cleanup.
func NewRecorder(store RecordStore, retention time.Duration, onFailure func(), logger *slog.Logger) *Recorder {
	flushTicker := time.NewTicker(recorderFlushInterval)
	var cleanupTicker *time.Ticker
	var cleanup <-chan time.Time
	if retention > 0 {
		cleanupTicker = time.NewTicker(retentionCheckInterval)
		cleanup = cleanupTicker.C
	}
	stopTimers := func() {
		flushTicker.Stop()
		if cleanupTicker != nil {
			cleanupTicker.Stop()
		}
	}
	return newRecorder(store, retention, onFailure, recorderSettings{
		queueCapacity: recorderQueueCapacity,
		batchSize:     recorderBatchSize,
		flush:         flushTicker.C,
		cleanup:       cleanup,
		now:           time.Now,
		logger:        logger,
		stopTimers:    stopTimers,
	})
}

func newRecorder(store RecordStore, retention time.Duration, onFailure func(), settings recorderSettings) *Recorder {
	if settings.queueCapacity <= 0 {
		settings.queueCapacity = recorderQueueCapacity
	}
	if settings.batchSize <= 0 {
		settings.batchSize = recorderBatchSize
	}
	if settings.now == nil {
		settings.now = time.Now
	}
	if settings.logger == nil {
		settings.logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	recorder := &Recorder{
		store: store, retention: retention, onFailure: onFailure, logger: settings.logger, settings: settings,
		ctx: ctx, cancel: cancel, events: make(chan RequestRecord, settings.queueCapacity), done: make(chan struct{}),
		accepting: true, pending: make(map[string]RequestRecord),
	}
	go recorder.run()
	return recorder
}

func (*Recorder) ClientInflight(gateway.InflightEvent, int)  {}
func (*Recorder) BackendInflight(gateway.InflightEvent, int) {}

// Complete stages one resolved request without blocking the response path.
func (r *Recorder) Complete(event gateway.RequestEvent) {
	if event.RequestID == "" || event.ClientID <= 0 || event.ModelPoolID <= 0 {
		return
	}
	record := requestRecord(event)

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepting {
		return
	}
	r.pending[event.RequestID] = record
}

// ResponseComplete moves a staged record into the bounded worker queue. Any
// resulting backpressure occurs only after the downstream response is written.
func (r *Recorder) ResponseComplete(requestID string) {
	r.mu.Lock()
	record, exists := r.pending[requestID]
	if exists {
		delete(r.pending, requestID)
	}
	if !exists || !r.accepting {
		r.mu.Unlock()
		return
	}
	r.senders.Add(1)
	r.mu.Unlock()
	defer r.senders.Done()

	if r.settings.beforeEnqueue != nil {
		r.settings.beforeEnqueue()
	}
	r.events <- record
}

// Close stops intake, drains accepted sends, and waits for the final batch.
// When ctx expires it cancels in-progress storage calls and returns ctx.Err().
func (r *Recorder) Close(ctx context.Context) error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.accepting = false
		clear(r.pending)
		r.mu.Unlock()
		go func() {
			r.senders.Wait()
			close(r.events)
		}()
	})

	select {
	case <-r.done:
		return r.lastErr
	case <-ctx.Done():
		r.cancel()
		return ctx.Err()
	}
}

// Done is closed after the writer exits and can no longer access its store.
func (r *Recorder) Done() <-chan struct{} {
	return r.done
}

func (r *Recorder) run() {
	defer close(r.done)
	defer r.cancel()
	if r.settings.stopTimers != nil {
		defer r.settings.stopTimers()
	}

	lastCleanup := time.Time{}
	if r.retention > 0 {
		lastCleanup = r.settings.now()
		r.cleanup(lastCleanup)
	}
	if r.settings.onStart != nil {
		r.settings.onStart()
	}

	batch := make([]RequestRecord, 0, r.settings.batchSize)
	for {
		select {
		case record, ok := <-r.events:
			if !ok {
				r.flush(batch)
				return
			}
			batch = append(batch, record)
			if r.settings.afterDequeue != nil {
				r.settings.afterDequeue()
			}
			if len(batch) >= r.settings.batchSize {
				r.flush(batch)
				batch = batch[:0]
			}
		case <-r.settings.flush:
			if len(batch) > 0 {
				r.flush(batch)
				batch = batch[:0]
			}
		case <-r.settings.cleanup:
			at := r.settings.now()
			if r.retention > 0 && (lastCleanup.IsZero() || at.Sub(lastCleanup) >= retentionCheckInterval) {
				lastCleanup = at
				r.cleanup(at)
			}
			if r.settings.afterCleanupTick != nil {
				r.settings.afterCleanupTick()
			}
		}
	}
}

func (r *Recorder) flush(records []RequestRecord) {
	if len(records) == 0 {
		return
	}
	batch := append([]RequestRecord(nil), records...)
	if err := r.store.InsertUsageBatch(r.ctx, batch); err != nil {
		r.lastErr = err
		if r.onFailure != nil {
			for range batch {
				r.onFailure()
			}
		}
		r.logger.Error("usage batch persistence failed",
			"count", len(batch),
			"firstRequestId", batch[0].RequestID,
			"lastRequestId", batch[len(batch)-1].RequestID,
			"error", err,
		)
	}
}

func (r *Recorder) cleanup(at time.Time) {
	cutoff := at.Add(-r.retention).UTC()
	if _, err := r.store.DeleteUsageBefore(r.ctx, cutoff); err != nil {
		r.logger.Error("usage retention cleanup failed", "cutoff", cutoff, "error", err)
	}
}

func requestRecord(event gateway.RequestEvent) RequestRecord {
	record := RequestRecord{
		OccurredAt:      event.OccurredAt.UTC(),
		RequestID:       event.RequestID,
		ParentRequestID: event.ParentRequestID,
		ClientID:        event.ClientID,
		ClientName:      event.Client,
		ModelPoolID:     event.ModelPoolID,
		ModelName:       event.Model,
		BackendName:     event.Backend,
		HTTPStatus:      event.Status,
		DurationMS:      event.Duration.Milliseconds(),
		RetryCount:      event.RetryCount,
		Disconnected:    event.Disconnect,
	}
	if event.TTFT > 0 {
		ttft := event.TTFT.Milliseconds()
		record.TTFTMS = &ttft
	}
	if event.Usage != nil {
		record.UsageAvailable = true
		input, output := event.Usage.InputTokens, event.Usage.OutputTokens
		record.InputTokens, record.OutputTokens = &input, &output
		if event.Usage.CacheReadTokens != nil {
			cacheRead := *event.Usage.CacheReadTokens
			record.CacheReadTokens = &cacheRead
		}
	}
	return record
}
