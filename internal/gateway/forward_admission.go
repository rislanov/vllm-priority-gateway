package gateway

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
)

func (s *Service) acquireForwardAdmission(ctx context.Context, request ForwardRequest, resolved *resolvedForwardRequest, lifecycle *forwardLifecycle, poolRuntime domain.PoolRuntime) (func(*coordination.TokenUsage), *APIError) {
	limit := admission.EffectiveLimit(resolved.client.PriorityClass, poolRuntime.State, resolved.client.MaxConcurrency)
	if s.admission == nil {
		if resolved.sizePolicy.Enabled {
			return nil, gatewayUnavailable(s.retryAfter)
		}
		lease, ok := s.limiter.Acquire(resolved.client.ID, limit)
		if !ok {
			return nil, overloaded(s.retryAfter, DecisionPriorityConcurrencyLimit)
		}
		return func(*coordination.TokenUsage) { lease.Release() }, nil
	}
	var decision coordination.AdmissionDecision
	for attempt := range 2 {
		if ctx.Err() != nil || !s.coordinationAllowed() {
			return nil, gatewayUnavailable(s.retryAfter)
		}
		client, key, authErr := s.validateAPIKeySnapshot(request.APIKey, resolved.snapshot)
		if authErr != nil {
			return nil, authErr
		}
		apiErr := s.estimateForwardReservation(ctx, request, key.ID, resolved)
		if apiErr != nil && apiErr.Code != "tokenization_"+coordination.TokenizationStaleConfiguration {
			return nil, apiErr
		}
		if apiErr == nil {
			req := coordination.AdmissionRequest{
				LeaseID: uuid.New(), OperationStartedAt: s.now().UTC(), RequestID: request.RequestID,
				ReplicaID: s.replicaID, ConfigurationRevision: resolved.snapshot.Revision, APIKeyID: key.ID,
				ClientID: client.ID, PoolID: resolved.pool.ID, ClientPolicyRevision: client.Revision,
				EffectiveClientLimit: limit, ConfiguredClientLimit: client.MaxConcurrency,
				PoolGatewayInflightLimit: resolved.pool.MaxGatewayInflight,
				PoolHighPriorityReserve:  resolved.pool.HighPriorityReserve, PriorityClass: client.PriorityClass,
				RequestsPerMinute: client.RequestsPerMinute, TokensPerMinute: client.TokensPerMinute, LeaseTTL: s.leaseTTL,
				TokenReservation: resolved.tokenReservation,
			}
			if resolved.estimationMode == EstimationEmergency || !s.coordinationReady() {
				release, reason := s.acquireEmergencyForward(ctx, request.APIKey, req, resolved)
				if reason != coordination.ReasonStaleConfiguration {
					return release, s.forwardDecisionError(reason, nil, "")
				}
				decision = coordination.AdmissionDecision{Reason: reason}
			} else {
				var err error
				decision, err = s.acquireConfiguredAdmission(ctx, request.APIKey, req)
				if err != nil || decision.Reason == coordination.ReasonCoordinationUnavailable {
					var permanent coordination.PermanentError
					if errors.As(err, &permanent) || ctx.Err() != nil || !s.coordinationAllowed() {
						return nil, gatewayUnavailable(s.retryAfter)
					}
					// The shared receipt may have committed. Use only the local
					// emergency allocation and leave an unknown receipt to TTL.
					release, reason := s.acquireEmergencyForward(ctx, request.APIKey, req, resolved)
					return release, s.forwardDecisionError(reason, nil, "")
				}
			}
			if decision.Reason != coordination.ReasonStaleConfiguration {
				break
			}
		}
		if attempt != 0 {
			return nil, gatewayUnavailable(s.retryAfter)
		}

		current := s.registry.Snapshot()
		if current.Revision == resolved.snapshot.Revision {
			reloader, ok := s.registry.(SnapshotReloader)
			if !ok || reloader.Reload(ctx) != nil {
				return nil, gatewayUnavailable(s.retryAfter)
			}
			current = s.registry.Snapshot()
		}
		client, _, authErr = s.validateAPIKeySnapshot(request.APIKey, current)
		if authErr != nil {
			return nil, authErr
		}
		pool, ok := current.PoolsByName[resolved.publicModel]
		if !ok || !pool.Enabled || !current.Access[client.ID][pool.ID] {
			return nil, modelNotAllowed()
		}
		if resolved.sizePolicy.Enabled && (pool.ID != resolved.pool.ID || pool.UpstreamModelName != resolved.pool.UpstreamModelName ||
			pool.RequestSizeAdmission.EstimationProfile != resolved.pool.RequestSizeAdmission.EstimationProfile) {
			return nil, gatewayUnavailable(s.retryAfter)
		}
		resolved.client, resolved.pool, resolved.snapshot = client, pool, current
		prepared, apiErr := s.prepareForwardPayload(request, *resolved)
		if apiErr != nil {
			return nil, apiErr
		}
		*resolved = prepared
		poolRuntime, release, poolErr := s.acquirePool(ctx, client.ID, pool)
		if poolErr != nil {
			return nil, poolErr
		}
		release()
		limit = admission.EffectiveLimit(client.PriorityClass, poolRuntime.State, client.MaxConcurrency)
		lifecycle.event.ClientID, lifecycle.event.Client = client.ID, client.Name
		lifecycle.event.PriorityClass, lifecycle.event.VLLMPriority = client.PriorityClass, client.VLLMPriority
		lifecycle.event.ModelPoolID, lifecycle.event.Model, lifecycle.event.PoolState = pool.ID, pool.PublicModelName, poolRuntime.State
	}
	if !decision.Admitted() {
		if decision.Reason == "" {
			return nil, gatewayUnavailable(s.retryAfter)
		}
		return nil, s.forwardDecisionError(decision.Reason, decision.RetryAt, decision.ConcurrencyScope)
	}
	lifecycle.event.SoftTPMExpected = resolved.client.TokensPerMinute > 0
	var handle *coordination.LeaseHandle
	if s.leases != nil {
		handle = s.leases.Track(decision.Lease.Identity())
	}
	return func(usage *coordination.TokenUsage) {
		if observer, ok := s.admission.(coordination.AdmissionPolicyObserver); ok {
			if client, exists := s.registry.Snapshot().Clients[resolved.client.ID]; exists {
				observer.ObserveClientRatePolicy(coordination.ClientRatePolicy{ClientID: client.ID, Revision: client.Revision, RequestsPerMinute: client.RequestsPerMinute, TokensPerMinute: client.TokensPerMinute})
			}
		}
		if handle != nil {
			handle.Complete(usage)
			return
		}
		_, _ = s.admission.Complete(context.WithoutCancel(ctx), []coordination.LeaseCompletion{{Lease: decision.Lease.Identity(), Usage: usage}})
	}, nil
}

func (s *Service) forwardDecisionError(reason coordination.Reason, retryAt *time.Time, scope coordination.AdmissionScope) *APIError {
	if reason == "" {
		return nil
	}
	apiErr := coordinationAPIError(reason, s.retryAfter, retryAt, s.now().UTC())
	if reason == coordination.ReasonConcurrencyExhausted && scope == coordination.AdmissionPoolScope {
		apiErr.DecisionReason = DecisionPoolInflightLimit
	}
	return apiErr
}

func (s *Service) estimateForwardReservation(ctx context.Context, request ForwardRequest, keyID int64, resolved *resolvedForwardRequest) *APIError {
	if !resolved.sizePolicy.Enabled {
		return nil
	}
	if s.emergency.HasLegacyPoolLeases(resolved.pool.ID) {
		return s.forwardDecisionError(coordination.ReasonTokenPolicyTransition, nil, "")
	}
	if s.inputEstimator == nil {
		return gatewayUnavailable(s.retryAfter)
	}
	if resolved.estimationStarted.IsZero() {
		resolved.estimationStarted = time.Now()
	}
	if resolved.estimationMode == "" && !s.coordinationReady() {
		resolved.estimationMode = EstimationEmergency
	}
	if resolved.estimationMode == EstimationEmergency && !s.emergency.CanServe(resolved.client.PriorityClass, resolved.client.ID, resolved.client.MaxConcurrency) {
		return gatewayUnavailable(s.retryAfter)
	}
	timeout := s.admissionEstimateTimeout
	if resolved.estimationMode == EstimationEmergency {
		timeout = min(timeout, 2*time.Second)
	}
	estimateCtx, cancel := context.WithDeadline(ctx, resolved.estimationStarted.Add(timeout))
	defer cancel()
	estimate, apiErr := s.inputEstimator.Estimate(estimateCtx, EstimationRequest{
		Path: request.Path, Prepared: resolved.preparedSize, Snapshot: resolved.snapshot,
		Client: resolved.client, Pool: resolved.pool, APIKeyID: keyID, RequestID: request.RequestID,
		Mode: resolved.estimationMode, apiKey: request.APIKey,
	})
	if estimate.Mode == EstimationEmergency {
		resolved.estimationMode = EstimationEmergency
	}
	if apiErr != nil {
		return apiErr
	}
	if estimateCtx.Err() != nil || estimate.Profile != resolved.sizePolicy.EstimationProfile ||
		int64(len(estimate.InputTokens)) != resolved.preparedSize.PromptCount {
		return gatewayUnavailable(s.retryAfter)
	}
	for _, count := range estimate.InputTokens {
		if count > resolved.sizePolicy.Limits.MaxInputTokens {
			return sizeValidationError(admission.RequestSizeInputExceeded, "Input estimate exceeds the configured maximum")
		}
	}
	reserved, err := admission.ReservationTokens(estimate.InputTokens, resolved.preparedSize.OutputAllowance, resolved.preparedSize.Samples)
	if err != nil {
		return sizeValidationError(admission.RequestSizeInvalid, "Token reservation exceeds the supported range")
	}
	var input int64
	for _, count := range estimate.InputTokens {
		input += max(count, 1)
	}
	resolved.tokenReservation = coordination.TokenReservation{
		Enabled: true, InputEstimate: input, OutputAllowance: resolved.preparedSize.OutputAllowance,
		PromptCount: resolved.preparedSize.PromptCount, Samples: resolved.preparedSize.Samples, ReservedTokens: reserved,
		ClientBudget: resolved.sizePolicy.ClientBudget, PoolBudget: resolved.sizePolicy.PoolBudget,
		PoolPolicyRevision: resolved.pool.Revision, EstimatorKind: estimate.Kind, EstimationProfile: estimate.Profile,
	}
	if !resolved.tokenReservation.Valid() {
		return &APIError{HTTPStatus: http.StatusServiceUnavailable, Message: "Token estimate is invalid", Type: "server_error",
			Code: "token_estimation_unavailable", RetryAfter: s.retryAfter}
	}
	return nil
}

func (s *Service) acquireEmergencyForward(ctx context.Context, rawKey string, req coordination.AdmissionRequest, resolved *resolvedForwardRequest) (func(*coordination.TokenUsage), coordination.Reason) {
	var release func()
	reason := coordination.ReasonStaleConfiguration
	s.withAdmissionSnapshot(rawKey, req, func() {
		if ctx.Err() != nil || !s.coordinationAllowed() {
			reason = coordination.ReasonCoordinationUnavailable
			return
		}
		if req.TokenReservation.Enabled {
			release, reason, _ = s.emergency.AcquireTokens(req.PriorityClass, req.ClientID, req.PoolID, req.EffectiveClientLimit,
				req.TokenReservation, resolved.sizePolicy.EmergencyPoolBudget)
		} else {
			var ok bool
			release, ok = s.emergency.AcquireForPool(req.PriorityClass, req.ClientID, req.PoolID, req.EffectiveClientLimit)
			if ok {
				reason = ""
			} else {
				reason = coordination.ReasonCoordinationUnavailable
			}
		}
	})
	s.observeEmergency(req.PriorityClass, release != nil)
	if release == nil {
		return nil, reason
	}
	resolved.estimationMode = EstimationEmergency
	return func(*coordination.TokenUsage) { release() }, ""
}

func (s *Service) acquireConfiguredAdmission(ctx context.Context, rawKey string, req coordination.AdmissionRequest) (decision coordination.AdmissionDecision, err error) {
	decision.Reason = coordination.ReasonStaleConfiguration
	s.withAdmissionSnapshot(rawKey, req, func() {
		if req.TokenReservation.Enabled && s.emergency.HasLegacyPoolLeases(req.PoolID) {
			decision.Reason = coordination.ReasonTokenPolicyTransition
			return
		}
		decision, err = s.admission.Acquire(ctx, req)
	})
	return decision, err
}

func (s *Service) withAdmissionSnapshot(rawKey string, req coordination.AdmissionRequest, apply func()) {
	acquire := func(current *registry.Snapshot) {
		client, key, authErr := s.validateAPIKeySnapshot(rawKey, current)
		pool, exists := current.PoolsByID[req.PoolID]
		if current.Revision != req.ConfigurationRevision || authErr != nil || key.ID != req.APIKeyID || client.ID != req.ClientID ||
			client.PriorityClass != req.PriorityClass || client.Revision != req.ClientPolicyRevision || !exists || !pool.Enabled ||
			!current.Access[req.ClientID][req.PoolID] || pool.MaxGatewayInflight != req.PoolGatewayInflightLimit || pool.HighPriorityReserve != req.PoolHighPriorityReserve ||
			req.TokenReservation.Enabled != pool.RequestSizeAdmission.Enabled ||
			req.TokenReservation.Enabled && req.TokenReservation.PoolPolicyRevision != pool.Revision {
			return
		}
		apply()
	}
	if guarded, ok := s.registry.(interface {
		WithSnapshot(func(*registry.Snapshot))
	}); ok {
		// Publication and both allocation paths are linearized under this read
		// lock. The tokenizer is always called before entering this guard.
		guarded.WithSnapshot(acquire)
	} else {
		acquire(s.registry.Snapshot())
	}
}

func coordinationTokenUsage(usage *domain.TokenUsage) *coordination.TokenUsage {
	if usage == nil {
		return nil
	}
	cache := int64(0)
	if usage.CacheReadTokens != nil {
		cache = *usage.CacheReadTokens
	}
	return &coordination.TokenUsage{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, CacheReadTokens: cache}
}
