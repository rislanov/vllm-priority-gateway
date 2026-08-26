package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
)

type SnapshotProvider interface {
	Snapshot() *registry.Snapshot
}

type Runtime interface {
	PoolSnapshot(poolID int64, at time.Time) domain.PoolRuntime
	Snapshot(backendID int64, at time.Time) domain.BackendRuntime
	IncrementInflight(backendID int64) (release func(), ok bool)
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
	Now        func() time.Time
	LookupEnv  func(string) (string, bool)
}

type Service struct {
	registry   SnapshotProvider
	hmacSecret []byte
	limiter    *admission.Limiter
	runtime    Runtime
	router     *routing.Router
	forwarder  Forwarder
	usage      UsageRecorder
	now        func() time.Time
	lookupEnv  func(string) (string, bool)
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
	return &Service{
		registry: dependencies.Registry, hmacSecret: append([]byte(nil), dependencies.HMACSecret...),
		limiter: limiter, runtime: dependencies.Runtime, router: dependencies.Router,
		forwarder: dependencies.Forwarder, usage: dependencies.Usage, now: now, lookupEnv: lookupEnv,
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

func (s *Service) Models(_ context.Context, rawKey string) ([]domain.ModelPool, *APIError) {
	client, _, authErr := s.authenticate(rawKey)
	if authErr != nil {
		return nil, authErr
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
	return models, nil
}

func (s *Service) Forward(ctx context.Context, writer http.ResponseWriter, request ForwardRequest) (proxy.Result, *APIError) {
	client, _, authErr := s.authenticate(request.APIKey)
	if authErr != nil {
		return proxy.Result{}, authErr
	}
	payload, publicModel, parseErr := rewritePayload(request.Body)
	if parseErr != nil {
		return proxy.Result{}, invalidRequest(parseErr.Error())
	}
	snapshot := s.registry.Snapshot()
	pool, exists := snapshot.PoolsByName[publicModel]
	if !exists || !pool.Enabled || !snapshot.Access[client.ID][pool.ID] {
		return proxy.Result{}, modelNotAllowed()
	}
	payload, err := replaceModel(payload, pool.UpstreamModelName)
	if err != nil {
		return proxy.Result{}, invalidRequest("Failed to encode the upstream model")
	}
	now := s.now().UTC()
	poolRuntime := s.runtime.PoolSnapshot(pool.ID, now)
	if poolRuntime.State == domain.PoolUnavailable {
		return proxy.Result{}, backendUnavailable()
	}
	limit := admission.EffectiveLimit(client.PriorityClass, poolRuntime.State, client.MaxConcurrency)
	lease, ok := s.limiter.Acquire(client.ID, limit)
	if !ok {
		return proxy.Result{}, overloaded()
	}
	defer lease.Release()

	selectTarget := func(exclude map[int64]struct{}) (proxy.Target, error) {
		candidates := make([]routing.Candidate, 0, len(snapshot.BackendsByPool[pool.ID]))
		for _, backend := range snapshot.BackendsByPool[pool.ID] {
			runtime := s.runtime.Snapshot(backend.ID, now)
			_, secretOK := s.upstreamSecret(backend)
			candidates = append(candidates, routing.Candidate{
				Backend: backend, Pressure: runtime.Pressure, GatewayInflight: runtime.GatewayInflight,
				Eligible: runtime.Healthy && runtime.MetricsFresh && secretOK,
			})
		}
		candidate, err := s.router.Select(candidates, exclude)
		if err != nil {
			return proxy.Target{}, err
		}
		secret, _ := s.upstreamSecret(candidate.Backend)
		return proxy.Target{Backend: candidate.Backend, UpstreamAPIKey: secret}, nil
	}

	target, err := selectTarget(nil)
	if err != nil {
		return proxy.Result{}, backendUnavailable()
	}
	currentRelease, ok := s.runtime.IncrementInflight(target.Backend.ID)
	if !ok {
		return proxy.Result{}, backendUnavailable()
	}
	defer func() {
		if currentRelease != nil {
			currentRelease()
		}
	}()

	headers := request.Headers.Clone()
	headers.Del("X-Vllm-Priority")
	headers.Del("Authorization")
	proxyRequest := proxy.Request{
		Method: request.Method, Path: request.Path, Headers: headers, Body: payload,
		RequestID: request.RequestID, Priority: client.VLLMPriority, Target: target,
	}
	proxyRequest.SelectAlternate = func(exclude map[int64]struct{}) (proxy.Target, error) {
		currentRelease()
		currentRelease = nil
		alternate, selectErr := selectTarget(exclude)
		if selectErr != nil {
			return proxy.Target{}, selectErr
		}
		release, incremented := s.runtime.IncrementInflight(alternate.Backend.ID)
		if !incremented {
			return proxy.Target{}, routing.ErrNoBackend
		}
		currentRelease = release
		return alternate, nil
	}
	result := s.forwarder.Forward(ctx, writer, proxyRequest)
	if result.Err != nil && result.BytesSent == 0 {
		if result.Cancelled || errors.Is(result.Err, context.Canceled) {
			return result, nil
		}
		return result, upstreamError()
	}
	return result, nil
}

func (s *Service) authenticate(raw string) (domain.Client, domain.APIKey, *APIError) {
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
	if s.usage != nil {
		s.usage.Record(matched.ID, now)
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

func overloaded() *APIError {
	return &APIError{HTTPStatus: http.StatusTooManyRequests, Message: "Inference cluster is currently overloaded", Type: "rate_limit_error", Code: "gateway_overloaded", RetryAfter: 2 * time.Second}
}

func backendUnavailable() *APIError {
	return &APIError{HTTPStatus: http.StatusServiceUnavailable, Message: "No healthy inference backend is currently available", Type: "server_error", Code: "backend_unavailable", RetryAfter: 2 * time.Second}
}

func upstreamError() *APIError {
	return &APIError{HTTPStatus: http.StatusBadGateway, Message: "The inference backend could not complete the request", Type: "server_error", Code: "upstream_error"}
}
