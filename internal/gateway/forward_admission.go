package gateway

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func (s *Service) acquireForwardAdmission(ctx context.Context, request ForwardRequest, resolved *resolvedForwardRequest, lifecycle *forwardLifecycle, poolRuntime domain.PoolRuntime) (func(*coordination.TokenUsage), *APIError) {
	limit := admission.EffectiveLimit(resolved.client.PriorityClass, poolRuntime.State, resolved.client.MaxConcurrency)
	if s.admission == nil {
		lease, ok := s.limiter.Acquire(resolved.client.ID, limit)
		if !ok {
			return nil, overloaded(s.retryAfter, DecisionPriorityConcurrencyLimit)
		}
		return func(*coordination.TokenUsage) { lease.Release() }, nil
	}
	if !s.coordinationAllowed() {
		return nil, gatewayUnavailable(s.retryAfter)
	}
	emergency := func() (func(*coordination.TokenUsage), *APIError) {
		release, ok := s.emergency.Acquire(resolved.client.PriorityClass, resolved.client.ID, limit)
		s.observeEmergency(resolved.client.PriorityClass, ok)
		if !ok {
			return nil, gatewayUnavailable(s.retryAfter)
		}
		return func(*coordination.TokenUsage) { release() }, nil
	}
	if !s.coordinationReady() {
		return emergency()
	}
	var decision coordination.AdmissionDecision
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		client, key, authErr := s.validateAPIKeySnapshot(request.APIKey, resolved.snapshot)
		if authErr != nil {
			return nil, authErr
		}
		decision, err = s.admission.Acquire(ctx, coordination.AdmissionRequest{
			LeaseID: uuid.New(), OperationStartedAt: s.now().UTC(), RequestID: request.RequestID,
			ReplicaID: s.replicaID, ConfigurationRevision: resolved.snapshot.Revision, APIKeyID: key.ID,
			ClientID: client.ID, PoolID: resolved.pool.ID, ClientPolicyRevision: client.Revision,
			EffectiveClientLimit: limit, ConfiguredClientLimit: client.MaxConcurrency,
			PoolGatewayInflightLimit: resolved.pool.MaxGatewayInflight,
			RequestsPerMinute:        client.RequestsPerMinute, TokensPerMinute: client.TokensPerMinute, LeaseTTL: s.leaseTTL,
		})
		if decision.Reason != coordination.ReasonStaleConfiguration || attempt > 0 {
			break
		}
		reloader, ok := s.registry.(SnapshotReloader)
		if !ok || reloader.Reload(ctx) != nil {
			err = errors.New("configuration reload failed")
			break
		}
		resolved.snapshot = s.registry.Snapshot()
		client, _, authErr = s.validateAPIKeySnapshot(request.APIKey, resolved.snapshot)
		if authErr != nil {
			return nil, authErr
		}
		pool, ok := resolved.snapshot.PoolsByName[resolved.publicModel]
		if !ok {
			return nil, modelNotAllowed()
		}
		resolved.client, resolved.pool = client, pool
		prepared, apiErr := s.prepareForwardPayload(request, *resolved)
		if apiErr != nil {
			return nil, apiErr
		}
		*resolved = prepared
		var release func()
		var poolErr *APIError
		poolRuntime, release, poolErr = s.acquirePool(ctx, client.ID, pool)
		if poolErr != nil {
			return nil, poolErr
		}
		release()
		limit = admission.EffectiveLimit(client.PriorityClass, poolRuntime.State, client.MaxConcurrency)
		lifecycle.event.ClientID, lifecycle.event.Client = client.ID, client.Name
		lifecycle.event.PriorityClass, lifecycle.event.VLLMPriority = client.PriorityClass, client.VLLMPriority
		lifecycle.event.ModelPoolID, lifecycle.event.Model, lifecycle.event.PoolState = pool.ID, pool.PublicModelName, poolRuntime.State
	}
	if err != nil || decision.Reason == coordination.ReasonCoordinationUnavailable {
		var permanent coordination.PermanentError
		if errors.As(err, &permanent) {
			return nil, gatewayUnavailable(s.retryAfter)
		}
		return emergency()
	}
	if !decision.Admitted() {
		apiErr := coordinationAPIError(decision.Reason, s.retryAfter, decision.RetryAt, s.now().UTC())
		if decision.Reason == coordination.ReasonConcurrencyExhausted && decision.ConcurrencyScope == coordination.AdmissionPoolScope {
			apiErr.DecisionReason = DecisionPoolInflightLimit
		}
		return nil, apiErr
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
