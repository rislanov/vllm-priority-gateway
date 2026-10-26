package gateway

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"net/http"
	"sort"
	"time"
)

type TokenizationForwarder interface {
	ForwardTokenization(context.Context, http.ResponseWriter, proxy.TokenizationRequest) proxy.Result
}
type TokenizationEvent struct {
	RequestID, Client, Model, Backend, Reason string
	Status                                    int
	Duration                                  time.Duration
	Cancelled                                 bool
}
type TokenizationObserver interface{ TokenizationComplete(TokenizationEvent) }

func (s *Service) ObserveTokenization(event TokenizationEvent) {
	if s.tokenizationObserver != nil {
		s.tokenizationObserver.TokenizationComplete(event)
	}
}

func (s *Service) tokenizationEligible(b domain.Backend, now time.Time) bool {
	if !b.Enabled || b.Draining || s.runtime == nil {
		return false
	}
	if _, ok := s.upstreamSecret(b); !ok {
		return false
	}
	r := s.runtime.Snapshot(b.ID, now)
	return r.Healthy && !r.LastHealthAt.IsZero() && !r.LastHealthAt.After(now) && s.tokenizationHealthMaxAge > 0 && now.Sub(r.LastHealthAt) <= s.tokenizationHealthMaxAge
}
func tokenizationError(status int, reason string, retry time.Duration) *APIError {
	kind := "server_error"
	if status == 429 {
		kind = "rate_limit_error"
	}
	return &APIError{HTTPStatus: status, Message: "Tokenization request could not be completed", Type: kind, Code: "tokenization_" + reason, RetryAfter: retry}
}

func (s *Service) Tokenize(parent context.Context, w http.ResponseWriter, r ForwardRequest) (result proxy.Result, apiErr *APIError) {
	started := time.Now()
	event := TokenizationEvent{RequestID: r.RequestID, Reason: "success"}
	defer func() {
		if p := recover(); p != nil {
			event.Status = 500
			event.Reason = "panic"
			event.Duration = time.Since(started)
			s.ObserveTokenization(event)
			panic(p)
		}
		event.Status = result.Status
		event.Cancelled = result.Cancelled
		event.Duration = time.Since(started)
		if apiErr != nil {
			event.Status = apiErr.HTTPStatus
			if event.Reason == "success" {
				event.Reason = "rejected"
			}
		} else if result.Err != nil {
			event.Reason = "transport_error"
			if result.Cancelled {
				event.Reason = "cancelled"
			}
			if event.Status == 0 {
				event.Status = 499
			}
		} else if result.Status >= 400 {
			event.Reason = "upstream_error"
		}
		s.ObserveTokenization(event)
	}()
	snapshot := s.registry.Snapshot()
	client, key, e := s.validateAPIKeySnapshot(r.APIKey, snapshot)
	if e != nil {
		event.Reason = "invalid_api_key"
		return result, e
	}
	event.Client = client.Name
	body, model, parseErr := rewritePayload(r.Body)
	if parseErr != nil {
		event.Reason = "invalid_request"
		return result, invalidRequest(parseErr.Error())
	}
	pool, ok := snapshot.PoolsByName[model]
	if !ok || !pool.Enabled || !snapshot.Access[client.ID][pool.ID] {
		event.Reason = "model_not_allowed"
		return result, modelNotAllowed()
	}
	event.Model = pool.PublicModelName
	if s.tokenization == nil || s.tokenizationForwarder == nil || s.tokenizationPolicy.Validate() != nil || !s.coordinationAllowed() || !s.coordinationReady() {
		event.Reason = coordination.TokenizationUnavailable
		return result, tokenizationError(503, event.Reason, s.retryAfter)
	}
	ctx, cancel := context.WithTimeout(parent, s.tokenizationPolicy.Timeout)
	defer cancel()
	candidates := make([]coordination.TokenizationCandidate, 0)
	now := s.now().UTC()
	for _, b := range snapshot.BackendsByPool[pool.ID] {
		if s.tokenizationEligible(b, now) {
			candidates = append(candidates, coordination.TokenizationCandidate{BackendID: b.ID, BackendRevision: b.Revision})
		}
	}
	if len(candidates) == 0 {
		event.Reason = "no_eligible_backend"
		return result, tokenizationError(503, event.Reason, s.retryAfter)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].BackendID < candidates[j].BackendID })
	decision, err := s.tokenization.Acquire(ctx, coordination.TokenizationAcquireRequest{OperationID: uuid.New(), ReplicaID: s.replicaID, RequestID: r.RequestID, OperationStartedAt: now, ConfigurationRevision: snapshot.Revision, APIKeyID: key.ID, ClientID: client.ID, PoolID: pool.ID, PoolRevision: pool.Revision, Candidates: candidates})
	if err != nil {
		event.Reason = coordination.TokenizationUnavailable
		if ctx.Err() != nil {
			return result, s.tokenizationContextError(ctx, err)
		}
		return result, tokenizationError(503, event.Reason, s.retryAfter)
	}
	if decision.Lease == nil {
		event.Reason = decision.Reason
		status := 503
		retry := s.retryAfter
		switch decision.Reason {
		case coordination.TokenizationClientLimit, coordination.TokenizationBackendLimit, coordination.TokenizationRateLimit:
			status = 429
		}
		if decision.RetryAt != nil {
			retry = max(time.Second, decision.RetryAt.Sub(s.now()))
		}
		return result, tokenizationError(status, event.Reason, retry)
	}
	lease := *decision.Lease
	defer func() {
		timeout := s.coordinationTimeout
		if timeout <= 0 {
			timeout = 500 * time.Millisecond
		}
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
		defer cancel()
		_ = s.tokenization.Complete(releaseCtx, lease)
	}()
	if err = ctx.Err(); err != nil {
		return result, s.tokenizationContextError(ctx, err)
	}
	current := s.registry.Snapshot()
	_, currentKey, authErr := s.validateAPIKeySnapshot(r.APIKey, current)
	if authErr != nil {
		event.Reason = "invalid_api_key"
		return result, authErr
	}
	b, exists := current.BackendsByID[lease.BackendID]
	p, poolExists := current.PoolsByID[pool.ID]
	expected, wasCandidate := snapshot.BackendsByID[lease.BackendID]
	if current.Revision != snapshot.Revision || !poolExists || p != pool || currentKey.ID != key.ID || !current.Access[client.ID][pool.ID] || !exists || !wasCandidate || b != expected || b.Revision != lease.BackendRevision || b.ModelPoolID != pool.ID || lease.ClientID != client.ID || lease.PoolID != pool.ID || !s.tokenizationEligible(b, s.now().UTC()) || !s.coordinationAllowed() || !s.coordinationReady() {
		event.Reason = coordination.TokenizationStaleConfiguration
		return result, tokenizationError(503, event.Reason, s.retryAfter)
	}
	event.Backend = b.Name
	body, err = replaceModel(body, pool.UpstreamModelName)
	if err != nil {
		return result, invalidRequest("Failed to encode upstream model")
	}
	if s.usage != nil {
		s.usage.Record(key.ID, s.now().UTC())
	}
	secret, _ := s.upstreamSecret(b)
	result = s.tokenizationForwarder.ForwardTokenization(ctx, w, proxy.TokenizationRequest{Headers: r.Headers, Body: body, RequestID: r.RequestID, Backend: b, UpstreamAPIKey: secret})
	if result.Err != nil && !result.ResponseStarted {
		return result, s.tokenizationContextError(ctx, result.Err)
	}
	return result, nil
}
func (s *Service) tokenizationContextError(ctx context.Context, err error) *APIError {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return tokenizationError(504, "timeout", 0)
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return tokenizationError(499, "cancelled", 0)
	}
	return tokenizationError(502, "upstream_error", 0)
}
