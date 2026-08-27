package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/observability"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

var testNow = time.Unix(1_700_000_000, 0).UTC()

func TestPublicAuthenticationCasesUseSameUnauthorizedEnvelope(t *testing.T) {
	raw, key := testKey(t)
	cases := []struct {
		name   string
		raw    string
		client domain.Client
		key    domain.APIKey
	}{
		{name: "unknown", raw: "llmgw_unknown_unknown_unknown_unknown_unknown", client: enabledClient(), key: key},
		{name: "revoked", raw: raw, client: enabledClient(), key: withRevoked(key)},
		{name: "expired", raw: raw, client: enabledClient(), key: withExpiry(key, testNow.Add(-time.Second))},
		{name: "disabled client", raw: raw, client: disabledClient(), key: key},
	}
	var firstBody string
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			handler, _ := newFixture(t, fixtureOptions{client: tt.client, key: tt.key})
			request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
			request.Header.Set("Authorization", "Bearer "+tt.raw)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d", response.Code)
			}
			if firstBody == "" {
				firstBody = response.Body.String()
			} else if response.Body.String() != firstBody {
				t.Fatalf("unauthorized body differs: %s != %s", response.Body.String(), firstBody)
			}
			assertErrorCode(t, response.Body.Bytes(), "invalid_api_key")
		})
	}
}

func TestInvalidAPIKeyIsRejectedBeforeReadingRequestBody(t *testing.T) {
	_, key := testKey(t)
	handler, _ := newFixture(t, fixtureOptions{client: enabledClient(), key: key})
	body := &readTrackingBody{}
	request := httptest.NewRequest(http.MethodPost, "/v1/completions", nil)
	request.Body = body
	request.ContentLength = -1
	request.Header.Set("Authorization", "Bearer llmgw_unknown_unknown_unknown_unknown_unknown")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	if body.reads != 0 {
		t.Fatalf("invalid API key caused %d body reads", body.reads)
	}
}

func TestModelsListsOnlyExplicitEnabledAccess(t *testing.T) {
	raw, key := testKey(t)
	handler, _ := newFixture(t, fixtureOptions{client: enabledClient(), key: key, extraPools: true})
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer "+raw)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data) != 1 || body.Data[0].ID != "public-model" {
		t.Fatalf("models = %+v", body.Data)
	}
}

func TestForwardRewritesModelAndClientControlledPriority(t *testing.T) {
	raw, key := testKey(t)
	forwarder := &capturingForwarder{}
	handler, _ := newFixture(t, fixtureOptions{client: enabledClient(), key: key, forwarder: forwarder})
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"public-model","priority":-999,"input":"hello"}`))
	request.Header.Set("Authorization", "Bearer "+raw)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Vllm-Priority", "-999")
	request.Header.Set("X-Request-Id", "parent-request")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	captured := forwarder.Request()
	if captured.Priority != -10 || captured.Headers.Get("X-Vllm-Priority") != "" {
		t.Fatalf("captured priority = %+v", captured)
	}
	var upstream map[string]json.RawMessage
	if err := json.Unmarshal(captured.Body, &upstream); err != nil {
		t.Fatal(err)
	}
	var model string
	if err := json.Unmarshal(upstream["model"], &model); err != nil {
		t.Fatal(err)
	}
	if model != "upstream-model" {
		t.Fatalf("upstream model = %q", model)
	}
	if _, exists := upstream["priority"]; exists {
		t.Fatal("client JSON priority reached upstream")
	}
	if response.Header().Get("X-Request-Id") != "fixed-gateway-id" {
		t.Fatalf("response request ID = %q", response.Header().Get("X-Request-Id"))
	}
}

func TestSessionAffinityPrefersRendezvousBackendAndStripsHeader(t *testing.T) {
	raw, key := testKey(t)
	forwarder := &capturingForwarder{}
	handler, _ := newFixture(t, fixtureOptions{
		client: enabledClient(), key: key, forwarder: forwarder, sessionAffinityBackends: true,
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"public-model"}`))
	request.Header.Set("Authorization", "Bearer "+raw)
	request.Header.Set(gateway.SessionAffinityHeader, "alpha")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	captured := forwarder.Request()
	if captured.Target.Backend.ID != 22 {
		t.Fatalf("backend = %d, want rendezvous backend 22", captured.Target.Backend.ID)
	}
	if got := captured.Headers.Get(gateway.SessionAffinityHeader); got != "" {
		t.Fatalf("session affinity header reached upstream: %q", got)
	}
}

func TestRequestWithoutSessionAffinityUsesLeastPressure(t *testing.T) {
	raw, key := testKey(t)
	for _, sessionID := range []string{"", "   \t"} {
		t.Run(strconv.Quote(sessionID), func(t *testing.T) {
			forwarder := &capturingForwarder{}
			handler, _ := newFixture(t, fixtureOptions{
				client: enabledClient(), key: key, forwarder: forwarder, sessionAffinityBackends: true,
			})
			request := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"public-model"}`))
			request.Header.Set("Authorization", "Bearer "+raw)
			if sessionID != "" {
				request.Header.Set(gateway.SessionAffinityHeader, sessionID)
			}
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
			}
			if got := forwarder.Request().Target.Backend.ID; got != 20 {
				t.Fatalf("backend = %d, want least-pressure backend 20", got)
			}
		})
	}
}

func TestSessionAffinityKeyIncludesClientAndPoolIdentity(t *testing.T) {
	tests := []struct {
		name          string
		clientID      int64
		poolID        int64
		wantBackendID int64
	}{
		{name: "client one pool ten", clientID: 1, poolID: 10, wantBackendID: 22},
		{name: "client two pool ten", clientID: 2, poolID: 10, wantBackendID: 21},
		{name: "client one pool eleven", clientID: 1, poolID: 11, wantBackendID: 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, key := testKey(t)
			client := enabledClient()
			client.ID = tt.clientID
			key.ClientID = tt.clientID
			forwarder := &capturingForwarder{}
			handler, _ := newFixture(t, fixtureOptions{
				client: client, key: key, poolID: tt.poolID, forwarder: forwarder, sessionAffinityBackends: true,
			})
			request := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"public-model"}`))
			request.Header.Set("Authorization", "Bearer "+raw)
			request.Header.Set(gateway.SessionAffinityHeader, "session-1")
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
			}
			if got := forwarder.Request().Target.Backend.ID; got != tt.wantBackendID {
				t.Fatalf("backend = %d, want %d", got, tt.wantBackendID)
			}
		})
	}
}

func TestSessionAffinityRejectsOversizedIdentifier(t *testing.T) {
	raw, key := testKey(t)
	forwarder := &capturingForwarder{}
	handler, _ := newFixture(t, fixtureOptions{client: enabledClient(), key: key, forwarder: forwarder})
	request := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"public-model"}`))
	request.Header.Set("Authorization", "Bearer "+raw)
	request.Header.Set(gateway.SessionAffinityHeader, strings.Repeat("s", gateway.MaxSessionAffinityIDBytes+1))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response.Body.Bytes(), "invalid_request_error")
	if forwarder.Calls() != 0 {
		t.Fatalf("oversized session identifier was forwarded %d time(s)", forwarder.Calls())
	}
}

func TestConcurrencyLeaseSpansWholeForwardLifecycle(t *testing.T) {
	raw, key := testKey(t)
	client := enabledClient()
	client.MaxConcurrency = 1
	forwarder := newBlockingForwarder()
	handler, _ := newFixture(t, fixtureOptions{client: client, key: key, forwarder: forwarder})
	server := httptest.NewServer(handler)
	defer server.Close()
	firstDone := make(chan *http.Response, 1)
	go func() {
		firstDone <- post(t, server.URL, raw)
	}()
	<-forwarder.started
	second := post(t, server.URL, raw)
	secondBody, _ := io.ReadAll(second.Body)
	second.Body.Close()
	if second.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second status = %d body=%s", second.StatusCode, secondBody)
	}
	close(forwarder.release)
	first := <-firstDone
	first.Body.Close()
	third := post(t, server.URL, raw)
	third.Body.Close()
	if third.StatusCode != http.StatusOK {
		t.Fatalf("third status = %d", third.StatusCode)
	}
}

func TestPublicControlledErrors(t *testing.T) {
	raw, key := testKey(t)
	cases := []struct {
		name       string
		method     string
		path       string
		body       string
		options    fixtureOptions
		wantStatus int
		wantCode   string
	}{
		{name: "malformed JSON", method: http.MethodPost, path: "/v1/completions", body: `{`, options: fixtureOptions{client: enabledClient(), key: key}, wantStatus: 400, wantCode: "invalid_request_error"},
		{name: "model name too long", method: http.MethodPost, path: "/v1/completions", body: `{"model":"` + strings.Repeat("x", 257) + `"}`, options: fixtureOptions{client: enabledClient(), key: key}, wantStatus: 400, wantCode: "invalid_request_error"},
		{name: "forbidden model", method: http.MethodPost, path: "/v1/completions", body: `{"model":"forbidden"}`, options: fixtureOptions{client: enabledClient(), key: key}, wantStatus: 403, wantCode: "model_not_allowed"},
		{name: "unsupported endpoint", method: http.MethodPost, path: "/v1/embeddings", body: `{}`, options: fixtureOptions{client: enabledClient(), key: key}, wantStatus: 404, wantCode: "unsupported_endpoint"},
		{name: "no backend", method: http.MethodPost, path: "/v1/completions", body: `{"model":"public-model"}`, options: fixtureOptions{client: enabledClient(), key: key, unhealthy: true}, wantStatus: 503, wantCode: "backend_unavailable"},
		{name: "overloaded", method: http.MethodPost, path: "/v1/completions", body: `{"model":"public-model"}`, options: fixtureOptions{client: backgroundClient(), key: key, poolState: domain.PoolSaturated}, wantStatus: 429, wantCode: "gateway_overloaded"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			handler, _ := newFixture(t, tt.options)
			request := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			request.Header.Set("Authorization", "Bearer "+raw)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
			}
			assertErrorCode(t, response.Body.Bytes(), tt.wantCode)
			if tt.wantStatus == http.StatusTooManyRequests || tt.wantStatus == http.StatusServiceUnavailable {
				if response.Header().Get("Retry-After") != "2" {
					t.Fatalf("Retry-After = %q", response.Header().Get("Retry-After"))
				}
			}
		})
	}
}

func TestConfiguredRetryAfterIsUsedForAdmissionErrors(t *testing.T) {
	raw, key := testKey(t)
	handler, _ := newFixture(t, fixtureOptions{
		client: backgroundClient(), key: key, poolState: domain.PoolSaturated,
		retryAfter: 6500 * time.Millisecond,
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"public-model"}`))
	request.Header.Set("Authorization", "Bearer "+raw)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "7" {
		t.Fatalf("status=%d Retry-After=%q", response.Code, response.Header().Get("Retry-After"))
	}
}

func TestResolvedRejectionWritesResponseBeforeRecorderBackpressure(t *testing.T) {
	raw, key := testKey(t)
	store := &blockingAnalyticsStore{insertStarted: make(chan struct{}, 1), releaseInsert: make(chan struct{})}
	recorder := analytics.NewRecorder(store, 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	release := sync.OnceFunc(func() { close(store.releaseInsert) })
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = recorder.Close(ctx)
	})
	observer := observability.Multi(recorder)
	responseObserver, ok := observer.(gateway.ResponseCompleteObserver)
	if !ok {
		t.Fatal("Multi did not expose response-complete capability")
	}

	for index := 0; index < 64; index++ {
		id := "writer-" + strconv.Itoa(index)
		observer.Complete(resolvedEvent(id))
		responseObserver.ResponseComplete(id)
	}
	select {
	case <-store.insertStarted:
	case <-time.After(time.Second):
		t.Fatal("recorder writer did not reach blocked store")
	}
	for index := 0; index < 1024; index++ {
		id := "queued-" + strconv.Itoa(index)
		observer.Complete(resolvedEvent(id))
		responseObserver.ResponseComplete(id)
	}

	handler, _ := newFixture(t, fixtureOptions{
		client: backgroundClient(), key: key, poolState: domain.PoolSaturated, observer: observer,
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"public-model"}`))
	request.Header.Set("Authorization", "Bearer "+raw)
	response := &responseSignalWriter{ResponseRecorder: httptest.NewRecorder(), wroteBody: make(chan struct{})}
	handlerDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(handlerDone)
	}()

	select {
	case <-response.wroteBody:
	case <-time.After(200 * time.Millisecond):
		release()
		t.Fatal("resolved rejection response was blocked by recorder queue saturation")
	}
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response.Body.Bytes(), "gateway_overloaded")
	select {
	case <-handlerDone:
		t.Fatal("handler returned before post-response recorder backpressure was released")
	default:
	}

	release()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler did not finish after recorder queue was released")
	}
}

func TestServiceObserverTracksRejectionAndInflightLifecycle(t *testing.T) {
	raw, key := testKey(t)
	observer := &recordingObserver{}
	handler, _ := newFixture(t, fixtureOptions{
		client: backgroundClient(), key: key, poolState: domain.PoolSaturated, observer: observer,
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"public-model"}`))
	request.Header.Set("Authorization", "Bearer "+raw)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	events := observer.Events()
	if len(events) != 1 || events[0].Status != http.StatusTooManyRequests || events[0].Reason != "gateway_overloaded" {
		t.Fatalf("rejection events = %+v", events)
	}

	observer = &recordingObserver{}
	handler, _ = newFixture(t, fixtureOptions{client: enabledClient(), key: key, observer: observer})
	request = httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"public-model"}`))
	request.Header.Set("Authorization", "Bearer "+raw)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if got := observer.ClientDeltas(); !equalInts(got, []int{1, -1}) {
		t.Fatalf("client inflight deltas = %v", got)
	}
	if got := observer.BackendDeltas(); !equalInts(got, []int{1, -1}) {
		t.Fatalf("backend inflight deltas = %v", got)
	}
	events = observer.Events()
	if len(events) != 1 || events[0].Status != http.StatusOK || events[0].Client != "client" || events[0].Backend != "gpu" {
		t.Fatalf("completion events = %+v", events)
	}
}

func TestServiceObserverDoesNotRecordAttackerControlledModel(t *testing.T) {
	raw, key := testKey(t)
	observer := &recordingObserver{}
	handler, _ := newFixture(t, fixtureOptions{client: enabledClient(), key: key, observer: observer})

	for _, model := range []string{"attacker-model-one", strings.Repeat("x", 128)} {
		request := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":`+strconv.Quote(model)+`}`))
		request.Header.Set("Authorization", "Bearer "+raw)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
		}
	}
	for _, event := range observer.Events() {
		if event.Model != "" {
			t.Fatalf("attacker-controlled model reached observability: %q", event.Model)
		}
	}
}

func TestKnownModelPolicyDenialsAreDurablyRecordedButUnknownModelsAreNot(t *testing.T) {
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	recorder := analytics.NewRecorder(database, 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = recorder.Close(ctx)
	})

	raw, key := testKey(t)
	nextID := 0
	handler, _ := newFixture(t, fixtureOptions{
		client: enabledClient(), key: key, extraPools: true,
		observer: observability.Multi(recorder),
		generateID: func() (string, error) {
			nextID++
			return "policy-request-" + strconv.Itoa(nextID), nil
		},
	})
	for _, model := range []string{"disabled-model", "not-allowed", "attacker-controlled-model"} {
		request := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":`+strconv.Quote(model)+`}`))
		request.Header.Set("Authorization", "Bearer "+raw)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("model %q status = %d body=%s", model, response.Code, response.Body.String())
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := recorder.Close(ctx); err != nil {
		t.Fatalf("Recorder.Close() error = %v", err)
	}
	page, err := database.UsageRequests(context.Background(), analytics.Filter{
		From: testNow.Add(-time.Millisecond), To: testNow.Add(time.Millisecond),
	}, 100, 0)
	if err != nil {
		t.Fatalf("UsageRequests() error = %v", err)
	}
	if page.Total != 2 || len(page.Requests) != 2 {
		t.Fatalf("durable policy-denial rows = %+v, want exactly two known models", page)
	}
	wantModels := map[int64]string{11: "disabled-model", 12: "not-allowed"}
	for _, record := range page.Requests {
		if record.ClientID != 1 || record.HTTPStatus != http.StatusForbidden || record.UsageAvailable ||
			wantModels[record.ModelPoolID] != record.ModelName {
			t.Fatalf("policy-denial row = %+v, want resolved 403 unavailable metadata", record)
		}
		delete(wantModels, record.ModelPoolID)
	}
	if len(wantModels) != 0 {
		t.Fatalf("missing known policy-denial models: %+v", wantModels)
	}
}

func TestPublicObserverCoversNonForwardedOutcomes(t *testing.T) {
	raw, key := testKey(t)
	observer := &recordingObserver{}
	handler, _ := newFixture(t, fixtureOptions{client: enabledClient(), key: key, observer: observer})

	requests := []*http.Request{
		httptest.NewRequest(http.MethodGet, "/v1/models", nil),
		httptest.NewRequest(http.MethodPost, "/v1/embeddings", strings.NewReader(`{}`)),
		httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(strings.Repeat("x", 1024*1024+1))),
		httptest.NewRequest(http.MethodGet, "/v1/models", nil),
	}
	requests[1].Header.Set("Authorization", "Bearer "+raw)
	requests[1].Header.Set("X-Request-Id", "unsupported-parent")
	requests[2].Header.Set("Authorization", "Bearer "+raw)
	requests[3].Header.Set("Authorization", "Bearer "+raw)
	for _, request := range requests {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
	}
	events := observer.Events()
	if len(events) != 4 {
		t.Fatalf("events = %+v", events)
	}
	wantStatuses := []int{http.StatusUnauthorized, http.StatusNotFound, http.StatusBadRequest, http.StatusOK}
	for index, want := range wantStatuses {
		if events[index].Status != want {
			t.Fatalf("event %d = %+v, want status %d", index, events[index], want)
		}
	}
	if events[3].Client != "client" || events[3].PriorityClass != domain.PriorityHigh {
		t.Fatalf("models event lacks client policy: %+v", events[3])
	}
	if events[1].ParentRequestID != "unsupported-parent" {
		t.Fatalf("unsupported event parent request ID = %q", events[1].ParentRequestID)
	}
}

type readTrackingBody struct {
	reads int
}

func (b *readTrackingBody) Read([]byte) (int, error) {
	b.reads++
	return 0, errors.New("request body must not be read before authentication")
}

func (*readTrackingBody) Close() error { return nil }

func TestServicePreservesCommittedUpstreamStatusOnBodyFailure(t *testing.T) {
	raw, key := testKey(t)
	observer := &recordingObserver{}
	handler, _ := newFixture(t, fixtureOptions{
		client: enabledClient(), key: key, observer: observer, forwarder: committedErrorForwarder{},
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"public-model"}`))
	request.Header.Set("Authorization", "Bearer "+raw)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "upstream_error") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	events := observer.Events()
	if len(events) != 1 || events[0].Status != http.StatusServiceUnavailable {
		t.Fatalf("events = %+v", events)
	}
}

type fixtureOptions struct {
	client                  domain.Client
	key                     domain.APIKey
	forwarder               gateway.Forwarder
	poolState               domain.PoolState
	unhealthy               bool
	extraPools              bool
	poolID                  int64
	sessionAffinityBackends bool
	observer                gateway.Observer
	retryAfter              time.Duration
	generateID              httpapi.IDGenerator
}

func newFixture(t *testing.T, options fixtureOptions) (http.Handler, *runtimeStub) {
	t.Helper()
	poolID := options.poolID
	if poolID == 0 {
		poolID = 10
	}
	pool := domain.ModelPool{ID: poolID, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: true}
	backend := domain.Backend{ID: 20, ModelPoolID: pool.ID, Name: "gpu", BaseURL: "http://backend.invalid", Enabled: true, CapacityHint: 1, RunningSoftLimit: 8}
	data := registry.Data{
		Revision: 1, Clients: []domain.Client{options.client}, Keys: []domain.APIKey{options.key},
		Pools: []domain.ModelPool{pool}, Access: []domain.ClientModelAccess{{ClientID: options.client.ID, ModelPoolID: pool.ID, Enabled: true}},
		Backends: []domain.Backend{backend},
	}
	backendRuntimes := map[int64]domain.BackendRuntime{
		backend.ID: {
			BackendID: backend.ID, Healthy: !options.unhealthy, MetricsFresh: !options.unhealthy,
			State: domain.BackendHealthy, Pressure: .3,
		},
	}
	if options.sessionAffinityBackends {
		data.Backends[0].Name = "gpu-20"
		backendRuntimes[20] = domain.BackendRuntime{BackendID: 20, Healthy: true, MetricsFresh: true, State: domain.BackendHealthy, Pressure: .2}
		for _, id := range []int64{21, 22} {
			value := backend
			value.ID = id
			value.Name = "gpu-" + strconv.FormatInt(id, 10)
			data.Backends = append(data.Backends, value)
			pressure := .2
			if id == 22 {
				pressure = .8
			}
			backendRuntimes[id] = domain.BackendRuntime{BackendID: id, Healthy: true, MetricsFresh: true, State: domain.BackendHealthy, Pressure: pressure}
		}
	}
	if options.extraPools {
		data.Pools = append(data.Pools,
			domain.ModelPool{ID: 11, PublicModelName: "disabled-model", UpstreamModelName: "disabled", Enabled: false},
			domain.ModelPool{ID: 12, PublicModelName: "not-allowed", UpstreamModelName: "other", Enabled: true},
		)
	}
	loader := staticLoader{data: data}
	reg := registry.New(loader)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	state := options.poolState
	if state == "" {
		state = domain.PoolNormal
	}
	runtime := &runtimeStub{pool: domain.PoolRuntime{PoolID: pool.ID, State: state, AvailableBackends: len(data.Backends)}, runtimes: backendRuntimes}
	forwarder := options.forwarder
	if forwarder == nil {
		forwarder = &capturingForwarder{}
	}
	service := gateway.New(gateway.Dependencies{
		Registry: reg, HMACSecret: []byte(strings.Repeat("s", 32)),
		Limiter: admission.NewLimiter(), Runtime: runtime,
		Router: routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0)), Forwarder: forwarder,
		Observer:   options.observer,
		RetryAfter: options.retryAfter,
		Now:        func() time.Time { return testNow }, LookupEnv: func(string) (string, bool) { return "", false },
	})
	generateID := options.generateID
	if generateID == nil {
		generateID = func() (string, error) { return "fixed-gateway-id", nil }
	}
	handler := httpapi.NewPublicHandler(service, 1024*1024, generateID)
	return handler, runtime
}

type staticLoader struct{ data registry.Data }

func (l staticLoader) LoadSnapshot(context.Context) (registry.Data, error) { return l.data, nil }

type runtimeStub struct {
	mu       sync.Mutex
	pool     domain.PoolRuntime
	runtimes map[int64]domain.BackendRuntime
}

func (r *runtimeStub) PoolSnapshot(int64, time.Time) domain.PoolRuntime { return r.pool }
func (r *runtimeStub) Snapshot(backendID int64, _ time.Time) domain.BackendRuntime {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runtimes[backendID]
}
func (r *runtimeStub) IncrementInflight(backendID int64) (func(), bool) {
	r.mu.Lock()
	runtime, exists := r.runtimes[backendID]
	if !exists {
		r.mu.Unlock()
		return nil, false
	}
	runtime.GatewayInflight++
	r.runtimes[backendID] = runtime
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			value := r.runtimes[backendID]
			value.GatewayInflight--
			r.runtimes[backendID] = value
			r.mu.Unlock()
		})
	}, true
}

type capturingForwarder struct {
	mu      sync.Mutex
	request proxy.Request
}

type committedErrorForwarder struct{}

func (committedErrorForwarder) Forward(_ context.Context, writer http.ResponseWriter, request proxy.Request) proxy.Result {
	writer.WriteHeader(http.StatusServiceUnavailable)
	return proxy.Result{
		BackendID: request.Target.Backend.ID, Status: http.StatusServiceUnavailable,
		ResponseStarted: true, Err: io.ErrUnexpectedEOF,
	}
}

func (f *capturingForwarder) Forward(_ context.Context, writer http.ResponseWriter, request proxy.Request) proxy.Result {
	f.mu.Lock()
	f.request = request
	f.mu.Unlock()
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	written, _ := writer.Write([]byte(`{"ok":true}`))
	return proxy.Result{BackendID: request.Target.Backend.ID, Status: http.StatusOK, BytesSent: int64(written)}
}

func (f *capturingForwarder) Request() proxy.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.request
}

func (f *capturingForwarder) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.request.Method == "" {
		return 0
	}
	return 1
}

type blockingForwarder struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type recordingObserver struct {
	mu            sync.Mutex
	events        []gateway.RequestEvent
	clientDeltas  []int
	backendDeltas []int
}

type responseSignalWriter struct {
	*httptest.ResponseRecorder
	once      sync.Once
	wroteBody chan struct{}
}

func (w *responseSignalWriter) Write(value []byte) (int, error) {
	written, err := w.ResponseRecorder.Write(value)
	w.once.Do(func() { close(w.wroteBody) })
	return written, err
}

type blockingAnalyticsStore struct {
	insertStarted chan struct{}
	releaseInsert chan struct{}
}

func (s *blockingAnalyticsStore) InsertUsageBatch(context.Context, []analytics.RequestRecord) error {
	select {
	case s.insertStarted <- struct{}{}:
	default:
	}
	<-s.releaseInsert
	return nil
}

func (*blockingAnalyticsStore) DeleteUsageBefore(context.Context, time.Time) (int64, error) {
	return 0, nil
}

func resolvedEvent(requestID string) gateway.RequestEvent {
	return gateway.RequestEvent{
		OccurredAt: time.Unix(1_800_000_000, 0).UTC(), RequestID: requestID,
		ClientID: 1, ModelPoolID: 10, Client: "client", Model: "public-model", Status: http.StatusOK,
	}
}

func (o *recordingObserver) ClientInflight(_ gateway.InflightEvent, delta int) {
	o.mu.Lock()
	o.clientDeltas = append(o.clientDeltas, delta)
	o.mu.Unlock()
}

func (o *recordingObserver) BackendInflight(_ gateway.InflightEvent, delta int) {
	o.mu.Lock()
	o.backendDeltas = append(o.backendDeltas, delta)
	o.mu.Unlock()
}

func (o *recordingObserver) Complete(event gateway.RequestEvent) {
	o.mu.Lock()
	o.events = append(o.events, event)
	o.mu.Unlock()
}

func (o *recordingObserver) Events() []gateway.RequestEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]gateway.RequestEvent(nil), o.events...)
}

func (o *recordingObserver) ClientDeltas() []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]int(nil), o.clientDeltas...)
}

func (o *recordingObserver) BackendDeltas() []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]int(nil), o.backendDeltas...)
}

func equalInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func newBlockingForwarder() *blockingForwarder {
	return &blockingForwarder{started: make(chan struct{}), release: make(chan struct{})}
}

func (f *blockingForwarder) Forward(ctx context.Context, writer http.ResponseWriter, request proxy.Request) proxy.Result {
	f.once.Do(func() { close(f.started) })
	select {
	case <-f.release:
		writer.WriteHeader(http.StatusOK)
		written, _ := writer.Write([]byte(`{"ok":true}`))
		return proxy.Result{BackendID: request.Target.Backend.ID, Status: http.StatusOK, BytesSent: int64(written)}
	case <-ctx.Done():
		return proxy.Result{BackendID: request.Target.Backend.ID, Cancelled: true, Err: ctx.Err()}
	}
}

func testKey(t *testing.T) (string, domain.APIKey) {
	t.Helper()
	plaintext, err := apikey.Generate(bytes.NewReader(bytes.Repeat([]byte{0x42}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte(strings.Repeat("s", 32))
	return plaintext.Value, domain.APIKey{
		ID: 2, ClientID: 1, Prefix: plaintext.Prefix, SecretHash: apikey.Digest(secret, plaintext.Value), CreatedAt: testNow,
	}
}

func enabledClient() domain.Client {
	return domain.Client{ID: 1, Name: "client", Enabled: true, PriorityClass: domain.PriorityHigh, VLLMPriority: -10, MaxConcurrency: 2}
}

func disabledClient() domain.Client { value := enabledClient(); value.Enabled = false; return value }
func backgroundClient() domain.Client {
	value := enabledClient()
	value.PriorityClass = domain.PriorityBackground
	value.VLLMPriority = 100
	return value
}
func withRevoked(key domain.APIKey) domain.APIKey {
	value := testNow
	key.RevokedAt = &value
	return key
}
func withExpiry(key domain.APIKey, value time.Time) domain.APIKey { key.ExpiresAt = &value; return key }

func post(t *testing.T, url, rawKey string) *http.Response {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, url+"/v1/completions", strings.NewReader(`{"model":"public-model"}`))
	request.Header.Set("Authorization", "Bearer "+rawKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func assertErrorCode(t *testing.T, body []byte, want string) {
	t.Helper()
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("invalid error JSON %s: %v", body, err)
	}
	if envelope.Error.Code != want {
		t.Fatalf("error code = %q, want %q; body=%s", envelope.Error.Code, want, body)
	}
}
