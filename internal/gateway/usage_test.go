package gateway_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
)

func TestStreamingUsageIsForcedOnlyForSupportedStreamingEndpoints(t *testing.T) {
	tests := []struct {
		name                string
		path                string
		body                string
		wantIncludeUsage    bool
		wantUnrelatedOption any
	}{
		{
			name: "chat completions adds omitted stream options", path: "/v1/chat/completions",
			body: `{"model":"public-model","stream":true,"top_level":"keep"}`, wantIncludeUsage: true,
		},
		{
			name: "completions overrides false and preserves unrelated option", path: "/v1/completions",
			body:             `{"model":"public-model","stream":true,"stream_options":{"include_usage":false,"trace":"keep"},"top_level":"keep"}`,
			wantIncludeUsage: true, wantUnrelatedOption: "keep",
		},
		{
			name: "chat completions replaces non object stream options", path: "/v1/chat/completions",
			body: `{"model":"public-model","stream":true,"stream_options":["malformed"],"top_level":"keep"}`, wantIncludeUsage: true,
		},
		{
			name: "completions replaces null stream options", path: "/v1/completions",
			body: `{"model":"public-model","stream":true,"stream_options":null,"top_level":"keep"}`, wantIncludeUsage: true,
		},
		{
			name: "non streaming completions does not gain include usage", path: "/v1/completions",
			body:                `{"model":"public-model","stream":false,"stream_options":{"trace":"keep"},"top_level":"keep"}`,
			wantUnrelatedOption: "keep",
		},
		{
			name: "responses endpoint does not gain stream options", path: "/v1/responses",
			body: `{"model":"public-model","stream":true,"top_level":"keep"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			forwarder := &usageCaptureForwarder{result: proxy.Result{Status: http.StatusOK}}
			service, rawKey := newUsageTestService(t, forwarder, nil, nil, nil)

			_, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), gateway.ForwardRequest{
				Method: http.MethodPost, Path: test.path, Headers: make(http.Header), Body: []byte(test.body), APIKey: rawKey,
			})
			if apiErr != nil {
				t.Fatalf("Forward() API error = %+v", apiErr)
			}

			var got map[string]any
			if err := json.Unmarshal(forwarder.Request().Body, &got); err != nil {
				t.Fatalf("decode upstream body: %v", err)
			}
			if got["top_level"] != "keep" {
				t.Fatalf("top_level = %#v, want preserved value", got["top_level"])
			}
			options, hasOptions := got["stream_options"].(map[string]any)
			if test.wantIncludeUsage {
				if !hasOptions || options["include_usage"] != true {
					t.Fatalf("stream_options = %#v, want include_usage=true", got["stream_options"])
				}
			} else if hasOptions {
				if _, exists := options["include_usage"]; exists {
					t.Fatalf("stream_options = %#v, want no include_usage", options)
				}
			} else if _, exists := got["stream_options"]; exists {
				t.Fatalf("stream_options = %#v, want absent or unchanged object", got["stream_options"])
			}
			if test.wantUnrelatedOption != nil && options["trace"] != test.wantUnrelatedOption {
				t.Fatalf("stream_options.trace = %#v, want %#v", options["trace"], test.wantUnrelatedOption)
			}
		})
	}
}

func TestRequestEventUsageContainsStableLedgerAndProxyOutcome(t *testing.T) {
	started := time.Date(2026, time.August, 27, 17, 10, 0, 0, time.FixedZone("CEST", 2*60*60))
	completed := started.Add(3 * time.Second)
	clock := &mutableClock{current: started}
	cacheRead := int64(8)
	observer := &usageRecordingObserver{}
	forwarder := &retryProbeForwarder{
		beforeAlternate: func() {
			time.Sleep(time.Millisecond)
			clock.Set(completed)
		},
		result: proxy.Result{
			Status: http.StatusCreated, FirstByte: 17 * time.Millisecond, RetryCount: 1, Cancelled: true,
			Usage: &domain.TokenUsage{InputTokens: 13, OutputTokens: 5, CacheReadTokens: &cacheRead},
		},
	}
	service, rawKey := newUsageTestService(t, forwarder, observer, clock, []domain.Backend{
		{ID: 20, ModelPoolID: 10, Name: "gpu-20", BaseURL: "http://gpu-20.invalid", Enabled: true, CapacityHint: 1, RunningSoftLimit: 8},
		{ID: 21, ModelPoolID: 10, Name: "gpu-21", BaseURL: "http://gpu-21.invalid", Enabled: true, CapacityHint: 1, RunningSoftLimit: 8},
	})

	_, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), gateway.ForwardRequest{
		Method: http.MethodPost, Path: "/v1/chat/completions", Headers: make(http.Header),
		Body: []byte(`{"model":"public-model","stream":true}`), APIKey: rawKey,
		RequestID: "request-id", ParentRequestID: "parent-id",
	})
	if apiErr != nil {
		t.Fatalf("Forward() API error = %+v", apiErr)
	}

	event := observer.SingleEvent(t)
	if !event.OccurredAt.Equal(completed.UTC()) || event.OccurredAt.Location() != time.UTC {
		t.Fatalf("OccurredAt = %s (%s), want %s UTC", event.OccurredAt, event.OccurredAt.Location(), completed.UTC())
	}
	if event.ClientID != 1 || event.ModelPoolID != 10 {
		t.Fatalf("ledger IDs = client %d pool %d, want 1 and 10", event.ClientID, event.ModelPoolID)
	}
	if event.Client != "public-client" || event.Model != "public-model" || event.Backend != "gpu-21" {
		t.Fatalf("snapshot names = client %q model %q backend %q", event.Client, event.Model, event.Backend)
	}
	if event.RequestID != "request-id" || event.ParentRequestID != "parent-id" {
		t.Fatalf("request IDs = %q/%q", event.RequestID, event.ParentRequestID)
	}
	if event.Status != http.StatusCreated || event.Duration <= 0 || event.TTFT != 17*time.Millisecond || !event.Disconnect || event.RetryCount != 1 {
		t.Fatalf("proxy outcome = status %d duration %s ttft %s disconnect %t retries %d",
			event.Status, event.Duration, event.TTFT, event.Disconnect, event.RetryCount)
	}
	if event.Usage == nil || event.Usage.InputTokens != 13 || event.Usage.OutputTokens != 5 ||
		event.Usage.CacheReadTokens == nil || *event.Usage.CacheReadTokens != 8 || event.UsageParseFailure != "" {
		t.Fatalf("usage = %+v parse failure = %q", event.Usage, event.UsageParseFailure)
	}
}

func TestRequestEventUsageContainsParseFailureFormat(t *testing.T) {
	observer := &usageRecordingObserver{}
	forwarder := &usageCaptureForwarder{result: proxy.Result{Status: http.StatusOK, UsageParseFailure: "shape"}}
	service, rawKey := newUsageTestService(t, forwarder, observer, nil, nil)

	_, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), gateway.ForwardRequest{
		Method: http.MethodPost, Path: "/v1/completions", Headers: make(http.Header),
		Body: []byte(`{"model":"public-model"}`), APIKey: rawKey,
	})
	if apiErr != nil {
		t.Fatalf("Forward() API error = %+v", apiErr)
	}
	event := observer.SingleEvent(t)
	if event.Usage != nil || event.UsageParseFailure != "shape" {
		t.Fatalf("usage = %+v parse failure = %q, want nil/shape", event.Usage, event.UsageParseFailure)
	}
}

func TestRequestEventUsagePreModelFailuresDoNotHaveBothLedgerIdentities(t *testing.T) {
	tests := []struct {
		name       string
		apiKey     string
		body       string
		wantStatus int
		wantClient int64
	}{
		{name: "authentication", apiKey: "invalid", body: `{"model":"public-model"}`, wantStatus: http.StatusUnauthorized},
		{name: "body validation", body: `not-json`, wantStatus: http.StatusBadRequest, wantClient: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observer := &usageRecordingObserver{}
			service, rawKey := newUsageTestService(t, &usageCaptureForwarder{}, observer, nil, nil)
			if test.apiKey != "" {
				rawKey = test.apiKey
			}
			_, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), gateway.ForwardRequest{
				Method: http.MethodPost, Path: "/v1/completions", Headers: make(http.Header), Body: []byte(test.body), APIKey: rawKey,
			})
			if apiErr == nil || apiErr.HTTPStatus != test.wantStatus {
				t.Fatalf("Forward() API error = %+v, want status %d", apiErr, test.wantStatus)
			}
			event := observer.SingleEvent(t)
			if event.ClientID != test.wantClient || event.ModelPoolID != 0 {
				t.Fatalf("ledger IDs = client %d pool %d, want client %d and no pool", event.ClientID, event.ModelPoolID, test.wantClient)
			}
		})
	}
}

func TestRequestEventUsageCompletePublicUsesCompletionClock(t *testing.T) {
	completed := time.Date(2026, time.August, 27, 19, 25, 0, 0, time.FixedZone("CEST", 2*60*60))
	observer := &usageRecordingObserver{}
	service := gateway.New(gateway.Dependencies{
		Observer: observer,
		Now:      func() time.Time { return completed },
	})

	service.CompletePublic(gateway.RequestEvent{
		OccurredAt: completed.Add(-time.Hour),
		Status:     http.StatusNotFound,
		Reason:     "unsupported_endpoint",
	})

	event := observer.SingleEvent(t)
	if !event.OccurredAt.Equal(completed.UTC()) || event.OccurredAt.Location() != time.UTC {
		t.Fatalf("OccurredAt = %s (%s), want authoritative completion time %s UTC",
			event.OccurredAt, event.OccurredAt.Location(), completed.UTC())
	}
}

type usageCaptureForwarder struct {
	mu      sync.Mutex
	request proxy.Request
	result  proxy.Result
}

func (f *usageCaptureForwarder) Forward(_ context.Context, _ http.ResponseWriter, request proxy.Request) proxy.Result {
	f.mu.Lock()
	f.request = request
	f.mu.Unlock()
	return f.result
}

func (f *usageCaptureForwarder) Request() proxy.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.request
}

type usageRecordingObserver struct {
	mu     sync.Mutex
	events []gateway.RequestEvent
}

func (*usageRecordingObserver) ClientInflight(gateway.InflightEvent, int)  {}
func (*usageRecordingObserver) BackendInflight(gateway.InflightEvent, int) {}
func (o *usageRecordingObserver) Complete(event gateway.RequestEvent) {
	o.mu.Lock()
	o.events = append(o.events, event)
	o.mu.Unlock()
}

func (o *usageRecordingObserver) SingleEvent(t *testing.T) gateway.RequestEvent {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.events) != 1 {
		t.Fatalf("completed events = %d, want 1", len(o.events))
	}
	return o.events[0]
}

func newUsageTestService(
	t *testing.T,
	forwarder gateway.Forwarder,
	observer gateway.Observer,
	clock *mutableClock,
	backends []domain.Backend,
) (*gateway.Service, string) {
	t.Helper()
	secret := []byte(strings.Repeat("s", 32))
	rawKey := "llmgw_abcdefghijklmnopqrstuvwxyz012345"
	client := domain.Client{
		ID: 1, Name: "public-client", Enabled: true, PriorityClass: domain.PriorityHigh, VLLMPriority: -20, MaxConcurrency: 2,
	}
	pool := domain.ModelPool{ID: 10, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: true}
	key := domain.APIKey{ID: 2, ClientID: client.ID, Prefix: rawKey[:12], SecretHash: apikey.Digest(secret, rawKey)}
	if len(backends) == 0 {
		backends = []domain.Backend{{
			ID: 20, ModelPoolID: pool.ID, Name: "gpu-20", BaseURL: "http://gpu-20.invalid",
			Enabled: true, CapacityHint: 1, RunningSoftLimit: 8,
		}}
	}
	values := make(map[int64]domain.BackendRuntime, len(backends))
	for index, backend := range backends {
		values[backend.ID] = domain.BackendRuntime{
			BackendID: backend.ID, Healthy: true, MetricsFresh: true, Pressure: float64(index+1) / 10,
		}
	}
	provider := &mutableSnapshotProvider{}
	provider.Set(testSnapshot(client, key, pool, backends))
	if clock == nil {
		clock = &mutableClock{current: time.Date(2026, time.August, 27, 15, 0, 0, 0, time.UTC)}
	}
	return gateway.New(gateway.Dependencies{
		Registry: provider, HMACSecret: secret, Limiter: admission.NewLimiter(), Runtime: &recordingRuntime{values: values},
		Router: routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0)), Forwarder: forwarder,
		Observer: observer, Now: clock.Now,
	}), rawKey
}
