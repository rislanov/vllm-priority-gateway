package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
)

const (
	SessionAffinityHeader     = "X-LLM-Session-Id"
	MaxSessionAffinityIDBytes = 256
)

type SnapshotProvider interface {
	Snapshot() *registry.Snapshot
}

type Runtime interface {
	PoolSnapshot(poolID int64, at time.Time) domain.PoolRuntime
	AcquirePool(poolID int64, maximum int) (release func(), ok bool)
	Snapshot(backendID int64, at time.Time) domain.BackendRuntime
	AcquireBackend(backendID int64, at time.Time) (complete func(domain.InferenceOutcome), ok bool)
}

type Forwarder interface {
	Forward(context.Context, http.ResponseWriter, proxy.Request) proxy.Result
}

type UsageRecorder interface {
	Record(keyID int64, usedAt time.Time)
}

type Dependencies struct {
	Registry   SnapshotProvider
	HMACSecret []byte
	Limiter    *admission.Limiter
	Runtime    Runtime
	Router     *routing.Router
	Forwarder  Forwarder
	Usage      UsageRecorder
	Observer   Observer
	Now        func() time.Time
	LookupEnv  func(string) (string, bool)
	RetryAfter time.Duration
}

type Service struct {
	registry   SnapshotProvider
	hmacSecret []byte
	limiter    *admission.Limiter
	runtime    Runtime
	router     *routing.Router
	forwarder  Forwarder
	usage      UsageRecorder
	observer   Observer
	now        func() time.Time
	lookupEnv  func(string) (string, bool)
	retryAfter time.Duration
}

func New(dependencies Dependencies) *Service {
	now := dependencies.Now
	if now == nil {
		now = time.Now
	}
	lookupEnv := dependencies.LookupEnv
	if lookupEnv == nil {
		lookupEnv = func(string) (string, bool) { return "", false }
	}
	limiter := dependencies.Limiter
	if limiter == nil {
		limiter = admission.NewLimiter()
	}
	retryAfter := dependencies.RetryAfter
	if retryAfter <= 0 {
		retryAfter = 2 * time.Second
	}
	return &Service{
		registry: dependencies.Registry, hmacSecret: append([]byte(nil), dependencies.HMACSecret...),
		limiter: limiter, runtime: dependencies.Runtime, router: dependencies.Router,
		forwarder: dependencies.Forwarder, usage: dependencies.Usage, observer: dependencies.Observer,
		now: now, lookupEnv: lookupEnv, retryAfter: retryAfter,
	}
}

type APIError struct {
	HTTPStatus int
	Message    string
	Type       string
	Code       string
	RetryAfter time.Duration
}

type ForwardRequest struct {
	Method          string
	Path            string
	Headers         http.Header
	Body            []byte
	APIKey          string
	RequestID       string
	ParentRequestID string
}

type InferenceReadiness struct {
	Status              string `json:"status"`
	Revision            int64  `json:"revision"`
	PoolAvailability    int    `json:"poolAvailability"`
	BackendAvailability int    `json:"backendAvailability"`
}

func (s *Service) InferenceReadiness() InferenceReadiness {
	snapshot := s.registry.Snapshot()
	readiness := InferenceReadiness{Status: "unavailable", Revision: snapshot.Revision}
	at := s.now().UTC()
	for _, pool := range snapshot.PoolsByID {
		if !pool.Enabled {
			continue
		}
		poolAvailable := false
		for _, backend := range snapshot.BackendsByPool[pool.ID] {
			if !backend.Enabled || backend.Draining {
				continue
			}
			runtime := s.runtime.Snapshot(backend.ID, at)
			_, secretAvailable := s.upstreamSecret(backend)
			if !runtime.Healthy || !runtime.MetricsFresh || !runtime.CircuitAvailable || !secretAvailable {
				continue
			}
			readiness.BackendAvailability++
			poolAvailable = true
		}
		if poolAvailable {
			readiness.PoolAvailability++
		}
	}
	if readiness.PoolAvailability > 0 || readiness.BackendAvailability > 0 {
		readiness.Status = "ready"
	}
	return readiness
}

func (s *Service) Models(_ context.Context, rawKey string) ([]domain.ModelPool, domain.Client, *APIError) {
	client, _, authErr := s.authenticate(rawKey)
	if authErr != nil {
		return nil, domain.Client{}, authErr
	}
	snapshot := s.registry.Snapshot()
	access := snapshot.Access[client.ID]
	models := make([]domain.ModelPool, 0, len(access))
	for poolID, allowed := range access {
		pool, exists := snapshot.PoolsByID[poolID]
		if allowed && exists && pool.Enabled {
			models = append(models, pool)
		}
	}
	sort.Slice(models, func(i, j int) bool { return models[i].PublicModelName < models[j].PublicModelName })
	return models, client, nil
}

// ValidateAPIKey authenticates a key without recording usage. Public HTTP
// handlers use it before reading a potentially large request body; Forward
// authenticates again after the read so revocations during upload fail closed.
func (s *Service) ValidateAPIKey(rawKey string) (domain.Client, *APIError) {
	client, _, apiErr := s.validateAPIKey(rawKey)
	return client, apiErr
}

// CompletePublic records public outcomes that do not enter the forwarding
// lifecycle, such as model listing and request-envelope rejection.
func (s *Service) CompletePublic(event RequestEvent) {
	if s.observer != nil {
		s.observer.Complete(event)
	}
}

func (s *Service) Forward(ctx context.Context, writer http.ResponseWriter, request ForwardRequest) (result proxy.Result, apiErr *APIError) {
	started := time.Now()
	event := RequestEvent{RequestID: request.RequestID, ParentRequestID: request.ParentRequestID}
	defer func() {
		event.Duration = time.Since(started)
		event.TTFT = result.FirstByte
		event.Disconnect = result.Cancelled
		event.RetryCount = result.RetryCount
		if apiErr != nil {
			event.Status = apiErr.HTTPStatus
			event.Reason = apiErr.Code
		} else {
			event.Status = result.Status
		}
		if s.observer != nil {
			s.observer.Complete(event)
		}
	}()

	client, _, authErr := s.authenticate(request.APIKey)
	if authErr != nil {
		return proxy.Result{}, authErr
	}
	sessionID := strings.TrimSpace(request.Headers.Get(SessionAffinityHeader))
	if len(sessionID) > MaxSessionAffinityIDBytes {
		return proxy.Result{}, invalidRequest("X-LLM-Session-Id must not exceed 256 bytes")
	}
	event.Client = client.Name
	event.PriorityClass = client.PriorityClass
	event.VLLMPriority = client.VLLMPriority
	payload, publicModel, parseErr := rewritePayload(request.Body)
	if parseErr != nil {
		return proxy.Result{}, invalidRequest(parseErr.Error())
	}
	snapshot := s.registry.Snapshot()
	pool, exists := snapshot.PoolsByName[publicModel]
	if !exists || !pool.Enabled || !snapshot.Access[client.ID][pool.ID] {
		return proxy.Result{}, modelNotAllowed()
	}
	event.Model = pool.PublicModelName
	affinityKey := ""
	if sessionID != "" {
		affinityKey = strconv.FormatInt(client.ID, 10) + "\x00" + strconv.FormatInt(pool.ID, 10) + "\x00" + sessionID
	}
	payload, err := replaceModel(payload, pool.UpstreamModelName)
	if err != nil {
		return proxy.Result{}, invalidRequest("Failed to encode the upstream model")
	}
	now := s.now().UTC()
	poolRuntime := s.runtime.PoolSnapshot(pool.ID, now)
	event.PoolState = poolRuntime.State
	if poolRuntime.State == domain.PoolUnavailable {
		return proxy.Result{}, backendUnavailable(s.retryAfter)
	}
	if pool.MaxWaiting > 0 && poolRuntime.TotalWaiting >= float64(pool.MaxWaiting) {
		return proxy.Result{}, overloaded(s.retryAfter)
	}
	releasePool, ok := s.runtime.AcquirePool(pool.ID, pool.MaxGatewayInflight)
	if !ok {
		return proxy.Result{}, overloaded(s.retryAfter)
	}
	defer releasePool()
	limit := admission.EffectiveLimit(client.PriorityClass, poolRuntime.State, client.MaxConcurrency)
	lease, ok := s.limiter.Acquire(client.ID, limit)
	if !ok {
		return proxy.Result{}, overloaded(s.retryAfter)
	}
	defer lease.Release()
	inflight := InflightEvent{Client: client.Name, Model: publicModel, PriorityClass: client.PriorityClass}
	if s.observer != nil {
		s.observer.ClientInflight(inflight, 1)
		defer s.observer.ClientInflight(inflight, -1)
	}

	selectTarget := func(exclude map[int64]struct{}) (proxy.Target, error) {
		excluded := make(map[int64]struct{}, len(exclude)+1)
		for backendID := range exclude {
			excluded[backendID] = struct{}{}
		}
		for {
			currentSnapshot := s.registry.Snapshot()
			currentPool, poolExists := currentSnapshot.PoolsByID[pool.ID]
			currentClient, clientExists := currentSnapshot.Clients[client.ID]
			if !poolExists || !currentPool.Enabled || currentPool.PublicModelName != pool.PublicModelName ||
				currentPool.UpstreamModelName != pool.UpstreamModelName || !clientExists || !currentClient.Enabled ||
				!currentSnapshot.Access[client.ID][pool.ID] {
				return proxy.Target{}, routing.ErrNoBackend
			}
			selectionTime := s.now().UTC()
			candidates := make([]routing.Candidate, 0, len(currentSnapshot.BackendsByPool[pool.ID]))
			for _, backend := range currentSnapshot.BackendsByPool[pool.ID] {
				runtime := s.runtime.Snapshot(backend.ID, selectionTime)
				_, secretOK := s.upstreamSecret(backend)
				candidates = append(candidates, routing.Candidate{
					Backend: backend, Pressure: runtime.Pressure, GatewayInflight: runtime.GatewayInflight,
					Eligible: runtime.Healthy && runtime.MetricsFresh && runtime.CircuitAvailable && secretOK,
				})
			}
			candidate, err := s.router.SelectWithSessionAffinity(candidates, excluded, affinityKey)
			if err != nil {
				return proxy.Target{}, err
			}
			completeBackend, acquired := s.runtime.AcquireBackend(candidate.Backend.ID, selectionTime)
			if !acquired {
				excluded[candidate.Backend.ID] = struct{}{}
				continue
			}
			secret, _ := s.upstreamSecret(candidate.Backend)
			backendInflight := inflight
			backendInflight.Backend = candidate.Backend.Name
			event.Backend = candidate.Backend.Name
			event.BackendPressure = candidate.Pressure
			if s.observer != nil {
				s.observer.BackendInflight(backendInflight, 1)
			}
			var once sync.Once
			return proxy.Target{
				Backend: candidate.Backend, UpstreamAPIKey: secret,
				Complete: func(outcome domain.InferenceOutcome) {
					once.Do(func() {
						completeBackend(outcome)
						if s.observer != nil {
							s.observer.BackendInflight(backendInflight, -1)
						}
					})
				},
			}, nil
		}
	}

	target, err := selectTarget(nil)
	if err != nil {
		return proxy.Result{}, backendUnavailable(s.retryAfter)
	}

	headers := request.Headers.Clone()
	headers.Del("X-Vllm-Priority")
	headers.Del(SessionAffinityHeader)
	headers.Del("Authorization")
	proxyRequest := proxy.Request{
		Method: request.Method, Path: request.Path, Headers: headers, Body: payload,
		RequestID: request.RequestID, Priority: client.VLLMPriority, Target: target,
	}
	proxyRequest.SelectAlternate = func(exclude map[int64]struct{}) (proxy.Target, error) {
		return selectTarget(exclude)
	}
	result = s.forwarder.Forward(ctx, writer, proxyRequest)
	if result.Err != nil && !result.ResponseStarted {
		if result.Cancelled || errors.Is(result.Err, context.Canceled) {
			return result, nil
		}
		return result, upstreamError()
	}
	return result, nil
}

func (s *Service) authenticate(raw string) (domain.Client, domain.APIKey, *APIError) {
	client, matched, apiErr := s.validateAPIKey(raw)
	if apiErr != nil {
		return domain.Client{}, domain.APIKey{}, apiErr
	}
	if s.usage != nil {
		s.usage.Record(matched.ID, s.now().UTC())
	}
	return client, matched, nil
}

func (s *Service) validateAPIKey(raw string) (domain.Client, domain.APIKey, *APIError) {
	if len(raw) < 12 || !strings.HasPrefix(raw, "llmgw_") {
		return domain.Client{}, domain.APIKey{}, invalidAPIKey()
	}
	snapshot := s.registry.Snapshot()
	candidates := snapshot.KeyCandidates[raw[:12]]
	var matched domain.APIKey
	found := false
	for _, candidate := range candidates {
		matches := apikey.Verify(s.hmacSecret, raw, candidate.SecretHash)
		if matches {
			matched = candidate
			found = true
		}
	}
	now := s.now().UTC()
	if !found || matched.RevokedAt != nil || (matched.ExpiresAt != nil && !matched.ExpiresAt.After(now)) {
		return domain.Client{}, domain.APIKey{}, invalidAPIKey()
	}
	client, exists := snapshot.Clients[matched.ClientID]
	if !exists || !client.Enabled {
		return domain.Client{}, domain.APIKey{}, invalidAPIKey()
	}
	return client, matched, nil
}

func (s *Service) upstreamSecret(backend domain.Backend) (string, bool) {
	if backend.UpstreamAPIKeyEnv == "" {
		return "", true
	}
	secret, ok := s.lookupEnv(backend.UpstreamAPIKeyEnv)
	return secret, ok && secret != ""
}

func rewritePayload(body []byte) ([]byte, string, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, "", errors.New("request body must be a JSON object")
	}
	if payload == nil {
		return nil, "", errors.New("request body must be a JSON object")
	}
	encodedModel, exists := payload["model"]
	if !exists {
		return nil, "", errors.New("model is required")
	}
	var model string
	if err := json.Unmarshal(encodedModel, &model); err != nil || strings.TrimSpace(model) == "" {
		return nil, "", errors.New("model must be a non-empty string")
	}
	if len(model) > domain.MaxPublicModelNameBytes {
		return nil, "", errors.New("model must not exceed 256 bytes")
	}
	delete(payload, "priority")
	returnBody, err := json.Marshal(payload)
	if err != nil {
		return nil, "", errors.New("encode upstream request")
	}
	return returnBody, model, nil
}

func replaceModel(body []byte, model string) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	encodedModel, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	payload["model"] = encodedModel
	return json.Marshal(payload)
}

func invalidAPIKey() *APIError {
	return &APIError{HTTPStatus: http.StatusUnauthorized, Message: "Invalid API key", Type: "authentication_error", Code: "invalid_api_key"}
}

func invalidRequest(message string) *APIError {
	return &APIError{HTTPStatus: http.StatusBadRequest, Message: message, Type: "invalid_request_error", Code: "invalid_request_error"}
}

func modelNotAllowed() *APIError {
	return &APIError{HTTPStatus: http.StatusForbidden, Message: "The requested model is not available to this client", Type: "invalid_request_error", Code: "model_not_allowed"}
}

func overloaded(retryAfter time.Duration) *APIError {
	return &APIError{HTTPStatus: http.StatusTooManyRequests, Message: "Inference cluster is currently overloaded", Type: "rate_limit_error", Code: "gateway_overloaded", RetryAfter: retryAfter}
}

func backendUnavailable(retryAfter time.Duration) *APIError {
	return &APIError{HTTPStatus: http.StatusServiceUnavailable, Message: "No healthy inference backend is currently available", Type: "server_error", Code: "backend_unavailable", RetryAfter: retryAfter}
}

func upstreamError() *APIError {
	return &APIError{HTTPStatus: http.StatusBadGateway, Message: "The inference backend could not complete the request", Type: "server_error", Code: "upstream_error"}
}
