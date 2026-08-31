package proxy_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
)

func TestForwardFlushesStreamingBytesWithoutFullBuffering(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{Tokens: []string{"one", "two"}, TokenDelay: 150 * time.Millisecond})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	writer := newObservingWriter()
	finished := make(chan proxy.Result, 1)
	go func() {
		finished <- proxy.New(server.Client()).Forward(context.Background(), writer, proxy.Request{
			Method: http.MethodPost, Path: "/v1/chat/completions",
			Body: []byte(`{"model":"upstream","stream":true}`), RequestID: "request", Priority: -10,
			Target: proxy.Target{Backend: backend(1, server.URL)},
		})
	}()
	select {
	case <-writer.firstWrite:
		if !strings.Contains(writer.String(), "one") {
			t.Fatalf("first bytes = %q", writer.String())
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("first SSE bytes were buffered")
	}
	result := <-finished
	if result.Err != nil {
		t.Fatalf("Forward() error = %v", result.Err)
	}
	if got := writer.String(); !strings.Contains(got, "one") || !strings.Contains(got, "two") || !strings.HasSuffix(got, "data: [DONE]\n\n") {
		t.Fatalf("stream = %q", got)
	}
	if writer.FlushCount() < 2 || result.BytesSent != int64(len(writer.String())) {
		t.Fatalf("flushes=%d result=%+v", writer.FlushCount(), result)
	}
}

func TestForwardInspectsUsageWithoutChangingStreamingBehavior(t *testing.T) {
	firstChunk := `{"choices":[{"message":{"content":"one"}}],`
	secondChunk := `"usage":{"prompt_tokens":13,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":8}}}`
	releaseFinalChunk := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(firstChunk))
		writer.(http.Flusher).Flush()
		<-releaseFinalChunk
		_, _ = writer.Write([]byte(secondChunk))
		writer.(http.Flusher).Flush()
	}))
	defer func() {
		select {
		case <-releaseFinalChunk:
		default:
			close(releaseFinalChunk)
		}
		upstream.Close()
	}()

	downstream := newObservingWriter()
	finished := make(chan proxy.Result, 1)
	go func() {
		finished <- proxy.New(upstream.Client()).Forward(context.Background(), downstream, proxy.Request{
			Method: http.MethodPost, Path: "/v1/chat/completions", Body: []byte(`{"model":"upstream"}`),
			Target: proxy.Target{Backend: backend(1, upstream.URL)},
		})
	}()

	select {
	case <-downstream.firstWrite:
	case <-time.After(time.Second):
		t.Fatal("first response chunk was not forwarded")
	}
	if got := downstream.String(); got != firstChunk {
		t.Fatalf("body before completion = %q, want %q", got, firstChunk)
	}
	if downstream.FlushCount() != 1 {
		t.Fatalf("flushes before completion = %d, want 1", downstream.FlushCount())
	}
	select {
	case result := <-finished:
		t.Fatalf("Forward returned before response completion: %+v", result)
	default:
	}

	close(releaseFinalChunk)
	result := <-finished
	wantBody := firstChunk + secondChunk
	if got := downstream.String(); got != wantBody {
		t.Fatalf("body = %q, want byte-identical %q", got, wantBody)
	}
	if downstream.FlushCount() != 2 {
		t.Fatalf("flushes = %d, want 2", downstream.FlushCount())
	}
	if result.Err != nil || result.BytesSent != int64(len(wantBody)) {
		t.Fatalf("result = %+v", result)
	}
	if result.Usage == nil || result.Usage.InputTokens != 13 || result.Usage.OutputTokens != 5 ||
		result.Usage.CacheReadTokens == nil || *result.Usage.CacheReadTokens != 8 || result.UsageParseFailure != "" {
		t.Fatalf("usage=%+v parse_failure=%q", result.Usage, result.UsageParseFailure)
	}
}

func TestForwardInspectsMalformedUsageWithoutAffectingDelivery(t *testing.T) {
	body := `{"usage":{"prompt_tokens":3,"completion_tokens":2},"bad":}`
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusAccepted)
		_, _ = writer.Write([]byte(body))
		writer.(http.Flusher).Flush()
	}))
	defer upstream.Close()

	downstream := newObservingWriter()
	result := proxy.New(upstream.Client()).Forward(context.Background(), downstream, proxy.Request{
		Method: http.MethodPost, Path: "/v1/chat/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{Backend: backend(1, upstream.URL)},
	})
	if got := downstream.String(); got != body {
		t.Fatalf("body = %q, want byte-identical %q", got, body)
	}
	if downstream.status != http.StatusAccepted || result.Status != http.StatusAccepted {
		t.Fatalf("downstream status=%d result=%+v", downstream.status, result)
	}
	if downstream.FlushCount() != 1 || result.BytesSent != int64(len(body)) {
		t.Fatalf("flushes=%d bytes=%d, want 1 and %d", downstream.FlushCount(), result.BytesSent, len(body))
	}
	if result.Err != nil || result.Usage != nil || result.UsageParseFailure != "json" {
		t.Fatalf("result=%+v", result)
	}
}

func TestForwardCancellationStopsUpstream(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{TTFT: 10 * time.Second})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan proxy.Result, 1)
	go func() {
		finished <- proxy.New(server.Client()).Forward(ctx, newObservingWriter(), proxy.Request{
			Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
			Target: proxy.Target{Backend: backend(1, server.URL)},
		})
	}()
	waitFor(t, time.Second, func() bool { return fake.Snapshot().ActiveRequests == 1 })
	cancel()
	result := <-finished
	if !result.Cancelled || !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("result = %+v", result)
	}
	waitFor(t, time.Second, func() bool { return fake.Snapshot().CancelledRequests == 1 })
}

func TestForwardRetriesOneAlternateBeforeFirstByte(t *testing.T) {
	failing := fakevllm.New()
	failing.SetState(fakevllm.State{ResetMode: fakevllm.ResetBeforeHeaders})
	failingServer := httptest.NewServer(failing.Handler())
	defer failingServer.Close()
	success := fakevllm.New()
	success.SetState(fakevllm.State{HTTPBody: `{"backend":"second"}`})
	successServer := httptest.NewServer(success.Handler())
	defer successServer.Close()
	var selections atomic.Int64
	writer := newObservingWriter()
	result := proxy.New(failingServer.Client()).Forward(context.Background(), writer, proxy.Request{
		Method: http.MethodPost, Path: "/v1/responses", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{Backend: backend(1, failingServer.URL)},
		SelectAlternate: func(exclude map[int64]struct{}) (proxy.Target, error) {
			selections.Add(1)
			if _, ok := exclude[1]; !ok {
				t.Fatal("failed backend was not excluded")
			}
			return proxy.Target{Backend: backend(2, successServer.URL)}, nil
		},
	})
	if result.Err != nil || result.RetryCount != 1 || result.BackendID != 2 || selections.Load() != 1 {
		t.Fatalf("result=%+v selections=%d", result, selections.Load())
	}
	if writer.String() != `{"backend":"second"}` {
		t.Fatalf("body = %q", writer.String())
	}
}

func TestForwardDoesNotRetryAfterEmptyFirstRead(t *testing.T) {
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       &emptyThenFailBody{},
		}, nil
	})}
	var selections atomic.Int64
	var outcomes []domain.InferenceOutcome
	result := proxy.New(client).Forward(context.Background(), newObservingWriter(), proxy.Request{
		Method: http.MethodPost, Path: "/v1/responses", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{
			Backend:  backend(1, "http://upstream.invalid"),
			Complete: func(outcome domain.InferenceOutcome) { outcomes = append(outcomes, outcome) },
		},
		SelectAlternate: func(map[int64]struct{}) (proxy.Target, error) {
			selections.Add(1)
			return proxy.Target{}, errors.New("unexpected alternate selection")
		},
	})

	if !errors.Is(result.Err, errUpstreamRead) || result.RetryCount != 0 || result.ResponseStarted || result.BytesSent != 0 {
		t.Fatalf("result = %+v", result)
	}
	if selections.Load() != 0 {
		t.Fatalf("alternate selections = %d, want 0", selections.Load())
	}
	if len(outcomes) != 1 || outcomes[0] != domain.InferenceFailure {
		t.Fatalf("outcomes = %v, want [failure]", outcomes)
	}
}

func TestForwardCompletesTransportFailureBeforeSelectingAlternate(t *testing.T) {
	failing := fakevllm.New()
	failing.SetState(fakevllm.State{ResetMode: fakevllm.ResetBeforeHeaders})
	failingServer := httptest.NewServer(failing.Handler())
	defer failingServer.Close()
	success := fakevllm.New()
	success.SetState(fakevllm.State{HTTPBody: `{"backend":"second"}`})
	successServer := httptest.NewServer(success.Handler())
	defer successServer.Close()

	var events []string
	result := proxy.New(failingServer.Client()).Forward(context.Background(), newObservingWriter(), proxy.Request{
		Method: http.MethodPost, Path: "/v1/responses", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{
			Backend: backend(1, failingServer.URL),
			Complete: func(outcome domain.InferenceOutcome) {
				events = append(events, "attempt-a:"+string(outcome))
			},
		},
		SelectAlternate: func(map[int64]struct{}) (proxy.Target, error) {
			events = append(events, "select-alternate")
			return proxy.Target{
				Backend: backend(2, successServer.URL),
				Complete: func(outcome domain.InferenceOutcome) {
					events = append(events, "attempt-b:"+string(outcome))
				},
			}, nil
		},
	})
	if result.Err != nil {
		t.Fatalf("Forward() error = %v", result.Err)
	}
	want := []string{"attempt-a:failure", "select-alternate", "attempt-b:success"}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestForwardClassifiesUpstream5xxAsFailureWithoutRetry(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{HTTPStatus: http.StatusServiceUnavailable, HTTPBody: `{"error":"busy"}`})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()

	var outcomes []domain.InferenceOutcome
	var selections atomic.Int64
	writer := newObservingWriter()
	result := proxy.New(server.Client()).Forward(context.Background(), writer, proxy.Request{
		Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{
			Backend:  backend(1, server.URL),
			Complete: func(outcome domain.InferenceOutcome) { outcomes = append(outcomes, outcome) },
		},
		SelectAlternate: func(map[int64]struct{}) (proxy.Target, error) {
			selections.Add(1)
			return proxy.Target{}, nil
		},
	})
	if result.Err != nil || result.Status != http.StatusServiceUnavailable || result.RetryCount != 0 {
		t.Fatalf("result = %+v", result)
	}
	if selections.Load() != 0 || len(outcomes) != 1 || outcomes[0] != domain.InferenceFailure {
		t.Fatalf("selections=%d outcomes=%v", selections.Load(), outcomes)
	}
	if writer.String() != `{"error":"busy"}` {
		t.Fatalf("body = %q", writer.String())
	}
}

func TestForwardClassifies5xxWriteFailureAsFailureWithoutRetry(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{HTTPStatus: http.StatusServiceUnavailable, HTTPBody: `{"error":"busy"}`})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()

	var outcomes []domain.InferenceOutcome
	var selections atomic.Int64
	result := proxy.New(server.Client()).Forward(context.Background(), &writeFailWriter{header: make(http.Header)}, proxy.Request{
		Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{
			Backend:  backend(1, server.URL),
			Complete: func(outcome domain.InferenceOutcome) { outcomes = append(outcomes, outcome) },
		},
		SelectAlternate: func(map[int64]struct{}) (proxy.Target, error) {
			selections.Add(1)
			return proxy.Target{}, nil
		},
	})
	if !errors.Is(result.Err, errDownstreamWrite) || result.Status != http.StatusServiceUnavailable || !result.ResponseStarted || result.RetryCount != 0 {
		t.Fatalf("result = %+v", result)
	}
	if selections.Load() != 0 || len(outcomes) != 1 || outcomes[0] != domain.InferenceFailure {
		t.Fatalf("selections=%d outcomes=%v", selections.Load(), outcomes)
	}
}

func TestForwardClassifies5xxCancellationAfterHeadersAsFailureWithoutRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     make(http.Header),
			Body:       contextReadBody{ctx: request.Context()},
		}, nil
	})}
	writer := newHeaderSignalWriter()
	outcomes := make(chan domain.InferenceOutcome, 1)
	var selections atomic.Int64
	finished := make(chan proxy.Result, 1)
	go func() {
		finished <- proxy.New(client).Forward(ctx, writer, proxy.Request{
			Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
			Target: proxy.Target{
				Backend:  backend(1, "http://backend.invalid"),
				Complete: func(outcome domain.InferenceOutcome) { outcomes <- outcome },
			},
			SelectAlternate: func(map[int64]struct{}) (proxy.Target, error) {
				selections.Add(1)
				return proxy.Target{}, nil
			},
		})
	}()
	select {
	case <-writer.headerWritten:
	case <-time.After(time.Second):
		t.Fatal("upstream 503 headers were not committed")
	}
	cancel()
	result := <-finished
	if !result.Cancelled || !errors.Is(result.Err, context.Canceled) || result.Status != http.StatusServiceUnavailable || result.RetryCount != 0 {
		t.Fatalf("result = %+v", result)
	}
	if selections.Load() != 0 {
		t.Fatalf("alternate selections = %d, want 0", selections.Load())
	}
	if outcome := <-outcomes; outcome != domain.InferenceFailure {
		t.Fatalf("outcome = %q, want failure", outcome)
	}
}

func TestForwardClassifiesBodyReadFailureBeforeDownstreamWriteFailureAsFailure(t *testing.T) {
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       partialReadFailBody{},
		}, nil
	})}
	var outcomes []domain.InferenceOutcome
	result := proxy.New(client).Forward(context.Background(), &writeFailWriter{header: make(http.Header)}, proxy.Request{
		Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{
			Backend:  backend(1, "http://backend.invalid"),
			Complete: func(outcome domain.InferenceOutcome) { outcomes = append(outcomes, outcome) },
		},
	})
	if !errors.Is(result.Err, errDownstreamWrite) || !result.ResponseStarted || result.RetryCount != 0 {
		t.Fatalf("result = %+v", result)
	}
	if len(outcomes) != 1 || outcomes[0] != domain.InferenceFailure {
		t.Fatalf("outcomes = %v, want [failure]", outcomes)
	}
}

func TestForwardPreservesProvenBodyReadFailureWhenWriteCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       partialReadFailBody{},
		}, nil
	})}
	var outcomes []domain.InferenceOutcome
	var selections atomic.Int64
	result := proxy.New(client).Forward(ctx, &cancelWriteFailWriter{header: make(http.Header), cancel: cancel}, proxy.Request{
		Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{
			Backend:  backend(1, "http://backend.invalid"),
			Complete: func(outcome domain.InferenceOutcome) { outcomes = append(outcomes, outcome) },
		},
		SelectAlternate: func(map[int64]struct{}) (proxy.Target, error) {
			selections.Add(1)
			return proxy.Target{}, nil
		},
	})
	if !errors.Is(result.Err, errDownstreamWrite) || !result.Cancelled || result.Status != http.StatusOK || !result.ResponseStarted || result.BytesSent != 0 || result.RetryCount != 0 {
		t.Fatalf("result = %+v", result)
	}
	if selections.Load() != 0 || len(outcomes) != 1 || outcomes[0] != domain.InferenceFailure {
		t.Fatalf("selections=%d outcomes=%v", selections.Load(), outcomes)
	}
}

func TestForwardPreservesLateReadFailureWhenSuccessfulWriteCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       &chunkThenFailBody{},
		}, nil
	})}
	var outcomes []domain.InferenceOutcome
	var selections atomic.Int64
	result := proxy.New(client).Forward(ctx, &cancelWriteWriter{header: make(http.Header), cancel: cancel}, proxy.Request{
		Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{
			Backend:  backend(1, "http://backend.invalid"),
			Complete: func(outcome domain.InferenceOutcome) { outcomes = append(outcomes, outcome) },
		},
		SelectAlternate: func(map[int64]struct{}) (proxy.Target, error) {
			selections.Add(1)
			return proxy.Target{}, nil
		},
	})
	if !errors.Is(result.Err, errUpstreamRead) || !result.Cancelled || result.Status != http.StatusOK || !result.ResponseStarted || result.BytesSent != int64(len("partial")) || result.RetryCount != 0 {
		t.Fatalf("result = %+v", result)
	}
	if selections.Load() != 0 || len(outcomes) != 1 || outcomes[0] != domain.InferenceNeutral {
		t.Fatalf("selections=%d outcomes=%v", selections.Load(), outcomes)
	}
}

func TestForwardClassifiesCompleted4xxAsSuccess(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{HTTPStatus: http.StatusTooManyRequests, HTTPBody: `{"error":"rate limited"}`})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()

	var outcomes []domain.InferenceOutcome
	result := proxy.New(server.Client()).Forward(context.Background(), newObservingWriter(), proxy.Request{
		Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{
			Backend:  backend(1, server.URL),
			Complete: func(outcome domain.InferenceOutcome) { outcomes = append(outcomes, outcome) },
		},
	})
	if result.Err != nil || result.Status != http.StatusTooManyRequests {
		t.Fatalf("result = %+v", result)
	}
	if len(outcomes) != 1 || outcomes[0] != domain.InferenceSuccess {
		t.Fatalf("outcomes = %v, want [success]", outcomes)
	}
}

func TestForwardClassifiesUpstreamBodyReadFailureAsFailure(t *testing.T) {
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       readFailBody{},
		}, nil
	})}
	var outcomes []domain.InferenceOutcome
	result := proxy.New(client).Forward(context.Background(), newObservingWriter(), proxy.Request{
		Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{
			Backend:  backend(1, "http://backend.invalid"),
			Complete: func(outcome domain.InferenceOutcome) { outcomes = append(outcomes, outcome) },
		},
	})
	if !errors.Is(result.Err, errUpstreamRead) || result.ResponseStarted {
		t.Fatalf("result = %+v", result)
	}
	if len(outcomes) != 1 || outcomes[0] != domain.InferenceFailure {
		t.Fatalf("outcomes = %v, want [failure]", outcomes)
	}
}

func TestForwardClassifiesCancellationAndDownstreamWriteFailureAsNeutral(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		fake := fakevllm.New()
		fake.SetState(fakevllm.State{TTFT: 10 * time.Second})
		server := httptest.NewServer(fake.Handler())
		defer server.Close()
		ctx, cancel := context.WithCancel(context.Background())
		outcomes := make(chan domain.InferenceOutcome, 1)
		finished := make(chan proxy.Result, 1)
		go func() {
			finished <- proxy.New(server.Client()).Forward(ctx, newObservingWriter(), proxy.Request{
				Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
				Target: proxy.Target{
					Backend:  backend(1, server.URL),
					Complete: func(outcome domain.InferenceOutcome) { outcomes <- outcome },
				},
			})
		}()
		waitFor(t, time.Second, func() bool { return fake.Snapshot().ActiveRequests == 1 })
		cancel()
		result := <-finished
		if !result.Cancelled || !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("result = %+v", result)
		}
		if outcome := <-outcomes; outcome != domain.InferenceNeutral {
			t.Fatalf("outcome = %q, want neutral", outcome)
		}
	})

	t.Run("downstream write failure", func(t *testing.T) {
		fake := fakevllm.New()
		fake.SetState(fakevllm.State{HTTPBody: `{"ok":true}`})
		server := httptest.NewServer(fake.Handler())
		defer server.Close()
		var outcomes []domain.InferenceOutcome
		result := proxy.New(server.Client()).Forward(context.Background(), &writeFailWriter{header: make(http.Header)}, proxy.Request{
			Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
			Target: proxy.Target{
				Backend:  backend(1, server.URL),
				Complete: func(outcome domain.InferenceOutcome) { outcomes = append(outcomes, outcome) },
			},
		})
		if !errors.Is(result.Err, errDownstreamWrite) {
			t.Fatalf("result = %+v", result)
		}
		if len(outcomes) != 1 || outcomes[0] != domain.InferenceNeutral {
			t.Fatalf("outcomes = %v, want [neutral]", outcomes)
		}
	})
}

func TestForwardCompletesEverySelectedTargetExactlyOnce(t *testing.T) {
	failing := fakevllm.New()
	failing.SetState(fakevllm.State{ResetMode: fakevllm.ResetBeforeHeaders})
	failingServer := httptest.NewServer(failing.Handler())
	defer failingServer.Close()
	success := fakevllm.New()
	successServer := httptest.NewServer(success.Handler())
	defer successServer.Close()

	var first, second atomic.Int64
	result := proxy.New(failingServer.Client()).Forward(context.Background(), newObservingWriter(), proxy.Request{
		Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{Backend: backend(1, failingServer.URL), Complete: func(domain.InferenceOutcome) { first.Add(1) }},
		SelectAlternate: func(map[int64]struct{}) (proxy.Target, error) {
			return proxy.Target{Backend: backend(2, successServer.URL), Complete: func(domain.InferenceOutcome) { second.Add(1) }}, nil
		},
	})
	if result.Err != nil {
		t.Fatalf("Forward() error = %v", result.Err)
	}
	if first.Load() != 1 || second.Load() != 1 {
		t.Fatalf("completion counts = first:%d second:%d, want 1 each", first.Load(), second.Load())
	}
}

func TestForwardDoesNotRetryAfterStreamStarts(t *testing.T) {
	failing := fakevllm.New()
	failing.SetState(fakevllm.State{Tokens: []string{"one", "two"}, ResetMode: fakevllm.ResetAfterChunks, ResetAfterChunks: 1})
	server := httptest.NewServer(failing.Handler())
	defer server.Close()
	var selections atomic.Int64
	writer := newObservingWriter()
	result := proxy.New(server.Client()).Forward(context.Background(), writer, proxy.Request{
		Method: http.MethodPost, Path: "/v1/chat/completions", Body: []byte(`{"model":"upstream","stream":true}`),
		Target: proxy.Target{Backend: backend(1, server.URL)},
		SelectAlternate: func(map[int64]struct{}) (proxy.Target, error) {
			selections.Add(1)
			return proxy.Target{}, errors.New("must not be called")
		},
	})
	if result.Err == nil || result.RetryCount != 0 || selections.Load() != 0 || !strings.Contains(writer.String(), "one") {
		t.Fatalf("result=%+v selections=%d body=%q", result, selections.Load(), writer.String())
	}
}

func TestForwardDoesNotRetryHTTPErrorResponse(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{HTTPStatus: http.StatusServiceUnavailable, HTTPBody: `{"error":"busy"}`})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	var selections atomic.Int64
	writer := newObservingWriter()
	result := proxy.New(server.Client()).Forward(context.Background(), writer, proxy.Request{
		Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{Backend: backend(1, server.URL)},
		SelectAlternate: func(map[int64]struct{}) (proxy.Target, error) {
			selections.Add(1)
			return proxy.Target{}, nil
		},
	})
	if result.Err != nil || result.Status != http.StatusServiceUnavailable || result.RetryCount != 0 || selections.Load() != 0 {
		t.Fatalf("result=%+v selections=%d", result, selections.Load())
	}
	if writer.String() != `{"error":"busy"}` {
		t.Fatalf("body = %q", writer.String())
	}
}

func TestForwardDoesNotFollowUpstreamRedirects(t *testing.T) {
	var redirected atomic.Int64
	destination := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		redirected.Add(1)
		if authorization := request.Header.Get("Authorization"); authorization != "" {
			t.Errorf("upstream credential crossed redirect boundary: %q", authorization)
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, destination.URL+"/captured", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	writer := newObservingWriter()
	result := proxy.New(origin.Client()).Forward(context.Background(), writer, proxy.Request{
		Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{Backend: backend(1, origin.URL), UpstreamAPIKey: "configured-secret"},
	})
	if result.Err != nil || result.Status != http.StatusTemporaryRedirect || redirected.Load() != 0 {
		t.Fatalf("result=%+v redirected=%d", result, redirected.Load())
	}
}

func TestForwardCommitsUpstreamErrorStatusBeforeReadingBody(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{HTTPStatus: http.StatusServiceUnavailable, ResetMode: fakevllm.ResetBeforeBody})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()

	writer := newObservingWriter()
	result := proxy.New(server.Client()).Forward(context.Background(), writer, proxy.Request{
		Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{"model":"upstream"}`),
		Target: proxy.Target{Backend: backend(1, server.URL)},
	})
	if result.Status != http.StatusServiceUnavailable || writer.status != http.StatusServiceUnavailable {
		t.Fatalf("result=%+v downstream_status=%d", result, writer.status)
	}
}

func backend(id int64, baseURL string) domain.Backend {
	return domain.Backend{ID: id, Name: "backend", BaseURL: baseURL, Enabled: true, CapacityHint: 1, RunningSoftLimit: 1}
}

type observingWriter struct {
	mu         sync.Mutex
	header     http.Header
	status     int
	body       bytes.Buffer
	flushes    int
	firstWrite chan struct{}
	once       sync.Once
}

var errDownstreamWrite = errors.New("downstream write failed")

var errUpstreamRead = errors.New("upstream read failed")

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type readFailBody struct{}

func (readFailBody) Read([]byte) (int, error) { return 0, errUpstreamRead }

func (readFailBody) Close() error { return nil }

type emptyThenFailBody struct {
	emptyReadDone bool
}

func (b *emptyThenFailBody) Read([]byte) (int, error) {
	if !b.emptyReadDone {
		b.emptyReadDone = true
		return 0, nil
	}
	return 0, errUpstreamRead
}

func (*emptyThenFailBody) Close() error { return nil }

type partialReadFailBody struct{}

func (partialReadFailBody) Read(destination []byte) (int, error) {
	return copy(destination, "partial"), errUpstreamRead
}

func (partialReadFailBody) Close() error { return nil }

type chunkThenFailBody struct {
	chunkRead bool
}

func (b *chunkThenFailBody) Read(destination []byte) (int, error) {
	if !b.chunkRead {
		b.chunkRead = true
		return copy(destination, "partial"), nil
	}
	return 0, errUpstreamRead
}

func (*chunkThenFailBody) Close() error { return nil }

type contextReadBody struct {
	ctx context.Context
}

func (b contextReadBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (contextReadBody) Close() error { return nil }

type headerSignalWriter struct {
	header        http.Header
	status        int
	headerWritten chan struct{}
	once          sync.Once
}

func newHeaderSignalWriter() *headerSignalWriter {
	return &headerSignalWriter{header: make(http.Header), headerWritten: make(chan struct{})}
}

func (w *headerSignalWriter) Header() http.Header { return w.header }

func (w *headerSignalWriter) WriteHeader(status int) {
	w.status = status
	w.once.Do(func() { close(w.headerWritten) })
}

func (*headerSignalWriter) Write(data []byte) (int, error) { return len(data), nil }

type writeFailWriter struct {
	header http.Header
	status int
}

func (w *writeFailWriter) Header() http.Header { return w.header }

func (w *writeFailWriter) WriteHeader(status int) { w.status = status }

func (w *writeFailWriter) Write([]byte) (int, error) { return 0, errDownstreamWrite }

type cancelWriteFailWriter struct {
	header http.Header
	status int
	cancel context.CancelFunc
}

func (w *cancelWriteFailWriter) Header() http.Header { return w.header }

func (w *cancelWriteFailWriter) WriteHeader(status int) { w.status = status }

func (w *cancelWriteFailWriter) Write([]byte) (int, error) {
	w.cancel()
	return 0, errDownstreamWrite
}

type cancelWriteWriter struct {
	header http.Header
	status int
	cancel context.CancelFunc
}

func (w *cancelWriteWriter) Header() http.Header { return w.header }

func (w *cancelWriteWriter) WriteHeader(status int) { w.status = status }

func (w *cancelWriteWriter) Write(data []byte) (int, error) {
	w.cancel()
	return len(data), nil
}

func newObservingWriter() *observingWriter {
	return &observingWriter{header: make(http.Header), firstWrite: make(chan struct{})}
}

func (w *observingWriter) Header() http.Header { return w.header }

func (w *observingWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = status
	}
}

func (w *observingWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.once.Do(func() { close(w.firstWrite) })
	return w.body.Write(data)
}

func (w *observingWriter) Flush() {
	w.mu.Lock()
	w.flushes++
	w.mu.Unlock()
}

func (w *observingWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.String()
}

func (w *observingWriter) FlushCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushes
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
