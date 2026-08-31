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
	AcquireBackend(expected domain.Backend, at time.Time) (complete func(domain.InferenceOutcome), ok bool)
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
	HTTPStatus     int
	Message        string
	Type           string
	Code           string
	RetryAfter     time.Duration
	DecisionReason DecisionReason
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
	event.OccurredAt = s.now().UTC()
	if s.observer != nil {
		s.observer.Complete(event)
	}
}

// ResponseComplete signals observer peers for the exact reserved lifecycle
// after the handler or upstream proxy finishes writing and before the public
// handler returns to net/http.
func (s *Service) ResponseComplete(reservation ResponseCompleteReservation) {
	if reservation == nil {
		return
	}
	if observer, ok := s.observer.(ResponseCompleteObserver); ok {
		observer.ResponseComplete(reservation)
	}
}

func (s *Service) acquirePool(ctx context.Context, clientID int64, original domain.ModelPool) (domain.PoolRuntime, func(), *APIError) {
	for {
		before := s.registry.Snapshot()
		pool, valid := currentAdmissionPool(before, clientID, original)
		if !valid {
			return domain.PoolRuntime{PoolID: original.ID, State: domain.PoolUnavailable}, nil, backendUnavailable(s.retryAfter, DecisionPoolUnavailable)
		}
		runtime := s.runtime.PoolSnapshot(pool.ID, s.now().UTC())
		if runtime.State == domain.PoolUnavailable {
			return runtime, nil, backendUnavailable(s.retryAfter, DecisionPoolUnavailable)
		}
		if pool.MaxWaiting > 0 && runtime.TotalWaiting >= float64(pool.MaxWaiting) {
			return runtime, nil, overloaded(s.retryAfter, DecisionPoolWaitingLimit)
		}
		release, ok := s.runtime.AcquirePool(pool.ID, pool.MaxGatewayInflight)
		if !ok {
			return runtime, nil, overloaded(s.retryAfter, DecisionPoolInflightLimit)
		}

		after := s.registry.Snapshot()
		validatedPool, stillValid := currentAdmissionPool(after, clientID, original)
		if !stillValid {
			release()
			return domain.PoolRuntime{PoolID: original.ID, State: domain.PoolUnavailable}, nil, backendUnavailable(s.retryAfter, DecisionPoolUnavailable)
		}
		if validatedPool.MaxGatewayInflight != pool.MaxGatewayInflight || validatedPool.MaxWaiting != pool.MaxWaiting {
			release()
			if ctx.Err() != nil {
				return runtime, nil, backendUnavailable(s.retryAfter, DecisionPoolUnavailable)
			}
			continue
		}
		return runtime, release, nil
	}
}

func currentAdmissionPool(snapshot *registry.Snapshot, clientID int64, original domain.ModelPool) (domain.ModelPool, bool) {
	pool, poolExists := snapshot.PoolsByID[original.ID]
	client, clientExists := snapshot.Clients[clientID]
	valid := poolExists && pool.Enabled && pool.PublicModelName == original.PublicModelName &&
		pool.UpstreamModelName == original.UpstreamModelName && clientExists && client.Enabled &&
		snapshot.Access[clientID][pool.ID]
	return pool, valid
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

func forceStreamingUsage(body []byte, path string) ([]byte, error) {
	if path != "/v1/chat/completions" && path != "/v1/completions" {
		return body, nil
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	var stream bool
	if encodedStream, exists := payload["stream"]; !exists || json.Unmarshal(encodedStream, &stream) != nil || !stream {
		return body, nil
	}
	options := make(map[string]json.RawMessage)
	if encodedOptions, exists := payload["stream_options"]; exists {
		var existing map[string]json.RawMessage
		if json.Unmarshal(encodedOptions, &existing) == nil && existing != nil {
			options = existing
		}
	}
	options["include_usage"] = json.RawMessage("true")
	encodedOptions, err := json.Marshal(options)
	if err != nil {
		return nil, err
	}
	payload["stream_options"] = encodedOptions
	return json.Marshal(payload)
}

func invalidAPIKey() *APIError {
	return &APIError{HTTPStatus: http.StatusUnauthorized, Message: "Invalid API key", Type: "authentication_error", Code: "invalid_api_key", DecisionReason: DecisionInvalidAPIKey}
}

func invalidRequest(message string) *APIError {
	return &APIError{HTTPStatus: http.StatusBadRequest, Message: message, Type: "invalid_request_error", Code: "invalid_request_error", DecisionReason: DecisionInvalidRequest}
}

func modelNotAllowed() *APIError {
	return &APIError{HTTPStatus: http.StatusForbidden, Message: "The requested model is not available to this client", Type: "invalid_request_error", Code: "model_not_allowed", DecisionReason: DecisionModelNotAllowed}
}

func overloaded(retryAfter time.Duration, reason DecisionReason) *APIError {
	return &APIError{HTTPStatus: http.StatusTooManyRequests, Message: "Inference cluster is currently overloaded", Type: "rate_limit_error", Code: "gateway_overloaded", RetryAfter: retryAfter, DecisionReason: reason}
}

func backendUnavailable(retryAfter time.Duration, reason DecisionReason) *APIError {
	return &APIError{HTTPStatus: http.StatusServiceUnavailable, Message: "No healthy inference backend is currently available", Type: "server_error", Code: "backend_unavailable", RetryAfter: retryAfter, DecisionReason: reason}
}

func gatewayUnavailable(retryAfter time.Duration) *APIError {
	return &APIError{
		HTTPStatus:     http.StatusServiceUnavailable,
		Message:        "The gateway is temporarily unable to accept requests",
		Type:           "server_error",
		Code:           "gateway_unavailable",
		RetryAfter:     retryAfter,
		DecisionReason: DecisionGatewayBackpressure,
	}
}

func upstreamError() *APIError {
	return &APIError{HTTPStatus: http.StatusBadGateway, Message: "The inference backend could not complete the request", Type: "server_error", Code: "upstream_error", DecisionReason: DecisionUpstreamFailure}
}
