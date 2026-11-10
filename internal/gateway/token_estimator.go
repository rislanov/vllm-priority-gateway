package gateway

import (
	"context"
	"net/http"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
)

type TokenEstimate struct {
	InputTokens   []int64
	ContextLimits []int64
	Kind, Profile string
}

type EstimationMode string

const (
	EstimationNormal    EstimationMode = "normal"
	EstimationEmergency EstimationMode = "emergency"
)

type InputEstimator interface {
	Estimate(context.Context, EstimationRequest) (TokenEstimate, *APIError)
}

type EstimationRequest struct {
	Path      string
	Prepared  admission.PreparedRequestSize
	Snapshot  *registry.Snapshot
	Client    domain.Client
	Pool      domain.ModelPool
	APIKeyID  int64
	RequestID string
	Mode      EstimationMode
	apiKey    string
}

type tokenizerTransport interface {
	Tokenize(context.Context, proxy.TokenizationRequest) (*http.Response, error)
}

type tokenEstimator struct {
	timeout          time.Duration
	maxResponseBytes int64
	retryAfter       time.Duration
	transport        tokenizerTransport
	acquire          func(context.Context, EstimationRequest) (proxy.TokenizationRequest, func(), *APIError)
}

func (e tokenEstimator) Estimate(parent context.Context, req EstimationRequest) (TokenEstimate, *APIError) {
	policy, err := domain.ResolveRequestSizePolicy(req.Client, req.Pool)
	if err != nil || !policy.Enabled || len(req.Prepared.Inputs) == 0 {
		return TokenEstimate{}, e.unavailable()
	}
	ctx, cancel := context.WithTimeout(parent, e.timeout)
	defer cancel()

	result := TokenEstimate{
		InputTokens: make([]int64, len(req.Prepared.Inputs)), ContextLimits: make([]int64, len(req.Prepared.Inputs)),
		Profile: policy.EstimationProfile,
	}
	for i, input := range req.Prepared.Inputs {
		if ctx.Err() != nil {
			return TokenEstimate{}, e.unavailable()
		}
		var count, contextLimit int64
		kind := "upstream"
		switch {
		case i < len(req.Prepared.TokenIDs) && req.Prepared.TokenIDs[i] > 0:
			count = req.Prepared.TokenIDs[i]
			kind = "token_ids"
		case req.Prepared.UseFallback:
			count = int64(len(input)) + policy.FallbackOverheadTokens
			kind = domain.TokenFallbackUTF8Bytes
		default:
			var apiErr *APIError
			count, contextLimit, apiErr = e.tokenize(ctx, req, input)
			if apiErr != nil {
				if apiErr.Code != admission.RequestSizeUnsupported || policy.FallbackMode != domain.TokenFallbackUTF8Bytes {
					return TokenEstimate{}, apiErr
				}
				count = int64(len(input)) + policy.FallbackOverheadTokens
				kind = domain.TokenFallbackUTF8Bytes
			}
		}
		if count > policy.Limits.MaxInputTokens {
			return TokenEstimate{}, sizeValidationError(admission.RequestSizeInputExceeded, "Input estimate exceeds the configured maximum")
		}
		if contextLimit > 0 && req.Prepared.OutputAllowance > contextLimit-count {
			return TokenEstimate{}, sizeValidationError(admission.RequestSizeInputExceeded, "Input and output allowance exceed the model context")
		}
		result.InputTokens[i] = count
		result.ContextLimits[i] = contextLimit
		if result.Kind == "" {
			result.Kind = kind
		} else if result.Kind != kind {
			result.Kind = "mixed"
		}
	}
	if ctx.Err() != nil {
		return TokenEstimate{}, e.unavailable()
	}
	return result, nil
}

func (e tokenEstimator) tokenize(ctx context.Context, req EstimationRequest, input []byte) (int64, int64, *APIError) {
	if e.transport == nil || e.acquire == nil {
		return 0, 0, e.unavailable()
	}
	body, err := replaceModel(input, req.Pool.UpstreamModelName)
	if err != nil {
		return 0, 0, e.unavailable()
	}
	upstream, release, apiErr := e.acquire(ctx, req)
	if apiErr != nil {
		return 0, 0, apiErr
	}
	defer release()
	upstream.Body = body
	upstream.RequestID = req.RequestID
	response, err := e.transport.Tokenize(ctx, upstream)
	if err != nil {
		return 0, 0, e.unavailable()
	}
	defer response.Body.Close()
	switch {
	case response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusNotImplemented:
		return 0, 0, sizeValidationError(admission.RequestSizeUnsupported, "The upstream tokenizer does not support this request")
	case response.StatusCode == http.StatusTooManyRequests:
		return 0, 0, tokenizationError(http.StatusTooManyRequests, coordination.TokenizationBackendLimit, e.retryAfter)
	case response.StatusCode != http.StatusOK:
		return 0, 0, e.unavailable()
	}
	count, contextLimit, err := proxy.DecodeTokenEstimate(response, e.maxResponseBytes)
	if err != nil {
		return 0, 0, e.unavailable()
	}
	return count, contextLimit, nil
}

func (e tokenEstimator) unavailable() *APIError {
	return &APIError{HTTPStatus: http.StatusServiceUnavailable, Type: "server_error", Code: "token_estimation_unavailable",
		Message: "Token estimation is temporarily unavailable", RetryAfter: e.retryAfter}
}

func sizeValidationError(code, message string) *APIError {
	return &APIError{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: code,
		Message: message, DecisionReason: DecisionInvalidRequest}
}

// acquireNormalEstimate shares the existing utility budget with public tokenize.
func (s *Service) acquireNormalEstimate(ctx context.Context, req EstimationRequest) (proxy.TokenizationRequest, func(), *APIError) {
	unavailable := func(reason string) (proxy.TokenizationRequest, func(), *APIError) {
		return proxy.TokenizationRequest{}, nil, tokenizationError(http.StatusServiceUnavailable, reason, s.retryAfter)
	}
	if req.Snapshot == nil || s.tokenization == nil || !s.coordinationAllowed() || !s.coordinationReady() {
		return unavailable(coordination.TokenizationUnavailable)
	}
	now := s.now().UTC()
	candidates := make([]coordination.TokenizationCandidate, 0, len(req.Snapshot.BackendsByPool[req.Pool.ID]))
	for _, backend := range req.Snapshot.BackendsByPool[req.Pool.ID] {
		if s.tokenizationEligible(backend, now) {
			candidates = append(candidates, coordination.TokenizationCandidate{BackendID: backend.ID, BackendRevision: backend.Revision})
		}
	}
	if len(candidates) == 0 {
		return unavailable("no_eligible_backend")
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].BackendID < candidates[j].BackendID })
	decision, err := s.tokenization.Acquire(ctx, coordination.TokenizationAcquireRequest{
		OperationID: uuid.New(), ReplicaID: s.replicaID, RequestID: req.RequestID, OperationStartedAt: now,
		ConfigurationRevision: req.Snapshot.Revision, APIKeyID: req.APIKeyID,
		ClientID: req.Client.ID, PoolID: req.Pool.ID, PoolRevision: req.Pool.Revision, Candidates: candidates,
	})
	if err != nil {
		return unavailable(coordination.TokenizationUnavailable)
	}
	if decision.Lease == nil {
		switch decision.Reason {
		case coordination.TokenizationClientLimit, coordination.TokenizationBackendLimit, coordination.TokenizationRateLimit:
			retry := s.retryAfter
			if decision.RetryAt != nil {
				retry = max(time.Second, decision.RetryAt.Sub(s.now()))
			}
			return proxy.TokenizationRequest{}, nil, tokenizationError(http.StatusTooManyRequests, decision.Reason, retry)
		default:
			return unavailable(decision.Reason)
		}
	}
	lease := *decision.Lease
	release := func() {
		deadline, ok := ctx.Deadline()
		if !ok {
			return
		}
		timeout := s.coordinationTimeout
		if timeout <= 0 {
			timeout = 500 * time.Millisecond
		}
		releaseCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), minTime(deadline, time.Now().Add(timeout)))
		defer cancel()
		_ = s.tokenization.Complete(releaseCtx, lease)
	}

	current := s.registry.Snapshot()
	client, key, authErr := s.validateAPIKeySnapshot(req.apiKey, current)
	if authErr != nil {
		release()
		return proxy.TokenizationRequest{}, nil, authErr
	}
	backend, found := current.BackendsByID[lease.BackendID]
	expected, wasCandidate := req.Snapshot.BackendsByID[lease.BackendID]
	if ctx.Err() != nil || current.Revision != req.Snapshot.Revision || current.PoolsByID[req.Pool.ID] != req.Pool ||
		key.ID != req.APIKeyID || client.ID != req.Client.ID || !current.Access[client.ID][req.Pool.ID] ||
		!found || !wasCandidate || backend != expected || backend.Revision != lease.BackendRevision ||
		backend.ModelPoolID != req.Pool.ID || lease.ClientID != client.ID || lease.PoolID != req.Pool.ID ||
		!s.tokenizationEligible(backend, s.now().UTC()) || !s.coordinationAllowed() || !s.coordinationReady() {
		release()
		return unavailable(coordination.TokenizationStaleConfiguration)
	}
	secret, ok := s.upstreamSecret(backend)
	if !ok {
		release()
		return unavailable(coordination.TokenizationStaleConfiguration)
	}
	return proxy.TokenizationRequest{Backend: backend, UpstreamAPIKey: secret}, release, nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
