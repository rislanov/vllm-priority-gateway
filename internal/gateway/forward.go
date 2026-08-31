package gateway

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
)

type ForwardRequest struct {
	Method          string
	Path            string
	Headers         http.Header
	Body            []byte
	APIKey          string
	RequestID       string
	ParentRequestID string
}

type forwardLifecycle struct {
	service             *Service
	started             time.Time
	admissionStarted    time.Time
	event               RequestEvent
	reservationRollback func()
	targetsMu           sync.Mutex
	targetCompletions   []func(domain.InferenceOutcome)
}

func (l *forwardLifecycle) finish(result proxy.Result, reservation ResponseCompleteReservation, apiErr *APIError) {
	l.event.OccurredAt = l.service.now().UTC()
	l.event.Duration = time.Since(l.started)
	l.event.TTFT = result.FirstByte
	l.event.Disconnect = result.Cancelled
	l.event.RetryCount = result.RetryCount
	l.event.Usage = result.Usage
	l.event.UsageParseFailure = result.UsageParseFailure
	if apiErr != nil {
		l.event.Status = apiErr.HTTPStatus
		l.event.Reason = apiErr.Code
		l.event.DecisionReason = apiErr.DecisionReason
		if !l.admissionStarted.IsZero() && l.event.QueueOutcome == "" {
			l.event.QueueWait = time.Since(l.admissionStarted)
			l.event.QueueOutcome = QueueRejected
		}
	} else {
		l.event.Status = result.Status
	}
	if reservation != nil {
		reservation.StageResponseComplete(l.event)
	}
	if l.service.observer != nil {
		l.service.observer.Complete(l.event)
	}
}

func (l *forwardLifecycle) rollbackOnPanic() {
	panicValue := recover()
	if panicValue == nil {
		return
	}
	l.targetsMu.Lock()
	targetCompletions := append([]func(domain.InferenceOutcome){}, l.targetCompletions...)
	l.targetsMu.Unlock()
	for _, complete := range targetCompletions {
		invokePanicCleanup(func() { complete(domain.InferenceNeutral) })
	}
	if l.reservationRollback != nil {
		invokePanicCleanup(l.reservationRollback)
	}
	panic(panicValue)
}

func (l *forwardLifecycle) registerTarget(complete func(domain.InferenceOutcome)) {
	l.targetsMu.Lock()
	l.targetCompletions = append(l.targetCompletions, complete)
	l.targetsMu.Unlock()
}

func invokePanicCleanup(cleanup func()) {
	defer func() { _ = recover() }()
	cleanup()
}

type resolvedForwardRequest struct {
	client      domain.Client
	pool        domain.ModelPool
	snapshot    *registry.Snapshot
	payload     []byte
	publicModel string
	sessionID   string
	affinityKey string
}

func (s *Service) resolveForwardRequest(request ForwardRequest, lifecycle *forwardLifecycle) (resolvedForwardRequest, *APIError) {
	client, _, authErr := s.authenticate(request.APIKey)
	if authErr != nil {
		return resolvedForwardRequest{}, authErr
	}
	lifecycle.event.ClientID = client.ID
	lifecycle.event.Client = client.Name
	lifecycle.event.PriorityClass = client.PriorityClass
	lifecycle.event.VLLMPriority = client.VLLMPriority

	sessionID := strings.TrimSpace(request.Headers.Get(SessionAffinityHeader))
	if len(sessionID) > MaxSessionAffinityIDBytes {
		return resolvedForwardRequest{}, invalidRequest("X-LLM-Session-Id must not exceed 256 bytes")
	}
	payload, publicModel, parseErr := rewritePayload(request.Body)
	if parseErr != nil {
		return resolvedForwardRequest{}, invalidRequest(parseErr.Error())
	}
	snapshot := s.registry.Snapshot()
	pool, exists := snapshot.PoolsByName[publicModel]
	if !exists {
		return resolvedForwardRequest{}, modelNotAllowed()
	}
	lifecycle.event.ModelPoolID = pool.ID
	lifecycle.event.Model = pool.PublicModelName
	return resolvedForwardRequest{
		client:      client,
		pool:        pool,
		snapshot:    snapshot,
		payload:     payload,
		publicModel: publicModel,
		sessionID:   sessionID,
	}, nil
}

func (s *Service) prepareForwardPayload(request ForwardRequest, resolved resolvedForwardRequest) (resolvedForwardRequest, *APIError) {
	if !resolved.pool.Enabled || !resolved.snapshot.Access[resolved.client.ID][resolved.pool.ID] {
		return resolved, modelNotAllowed()
	}
	if resolved.sessionID != "" {
		resolved.affinityKey = strconv.FormatInt(resolved.client.ID, 10) + "\x00" +
			strconv.FormatInt(resolved.pool.ID, 10) + "\x00" + resolved.sessionID
	}

	payload, err := forceStreamingUsage(resolved.payload, request.Path)
	if err != nil {
		return resolved, invalidRequest("Failed to encode the upstream request")
	}
	payload, err = replaceModel(payload, resolved.pool.UpstreamModelName)
	if err != nil {
		return resolved, invalidRequest("Failed to encode the upstream model")
	}
	resolved.payload = payload
	return resolved, nil
}

type targetSelector struct {
	service   *Service
	client    domain.Client
	pool      domain.ModelPool
	affinity  string
	inflight  InflightEvent
	lifecycle *forwardLifecycle
}

func (s targetSelector) selectTarget(exclude map[int64]struct{}) (proxy.Target, error) {
	excluded := make(map[int64]struct{}, len(exclude)+1)
	for backendID := range exclude {
		excluded[backendID] = struct{}{}
	}
	for {
		currentSnapshot := s.service.registry.Snapshot()
		currentPool, poolExists := currentSnapshot.PoolsByID[s.pool.ID]
		currentClient, clientExists := currentSnapshot.Clients[s.client.ID]
		if !poolExists || !currentPool.Enabled || currentPool.PublicModelName != s.pool.PublicModelName ||
			currentPool.UpstreamModelName != s.pool.UpstreamModelName || !clientExists || !currentClient.Enabled ||
			!currentSnapshot.Access[s.client.ID][s.pool.ID] {
			return proxy.Target{}, routing.ErrNoBackend
		}
		selectionTime := s.service.now().UTC()
		candidates := make([]routing.Candidate, 0, len(currentSnapshot.BackendsByPool[s.pool.ID]))
		for _, backend := range currentSnapshot.BackendsByPool[s.pool.ID] {
			runtime := s.service.runtime.Snapshot(backend.ID, selectionTime)
			_, secretOK := s.service.upstreamSecret(backend)
			candidates = append(candidates, routing.Candidate{
				Backend: backend, Pressure: runtime.Pressure, GatewayInflight: runtime.GatewayInflight,
				Eligible: runtime.Healthy && runtime.MetricsFresh && runtime.CircuitAvailable && secretOK,
			})
		}
		candidate, err := s.service.router.SelectWithSessionAffinity(candidates, excluded, s.affinity)
		if err != nil {
			return proxy.Target{}, err
		}
		backendInflight := s.inflight
		backendInflight.Backend = candidate.Backend.Name
		completeBackend, acquired := s.service.runtime.AcquireBackend(candidate.Backend, selectionTime)
		if !acquired {
			excluded[candidate.Backend.ID] = struct{}{}
			continue
		}
		var once sync.Once
		completeTarget := func(outcome domain.InferenceOutcome) {
			once.Do(func() {
				if s.service.observer != nil {
					defer s.service.observer.BackendInflight(backendInflight, -1)
				}
				completeBackend(outcome)
			})
		}
		s.lifecycle.registerTarget(completeTarget)
		secret, _ := s.service.upstreamSecret(candidate.Backend)
		s.lifecycle.event.Backend = candidate.Backend.Name
		s.lifecycle.event.BackendPressure = candidate.Pressure
		if s.lifecycle.event.QueueOutcome == "" {
			s.lifecycle.event.QueueWait = time.Since(s.lifecycle.admissionStarted)
			s.lifecycle.event.QueueOutcome = QueueSelected
		}
		if s.service.observer != nil {
			s.service.observer.BackendInflight(backendInflight, 1)
		}
		return proxy.Target{
			Backend: candidate.Backend, UpstreamAPIKey: secret,
			Complete: completeTarget,
		}, nil
	}
}

func (s *Service) Forward(
	ctx context.Context,
	writer http.ResponseWriter,
	request ForwardRequest,
) (result proxy.Result, reservation ResponseCompleteReservation, apiErr *APIError) {
	lifecycle := forwardLifecycle{
		service: s,
		started: time.Now(),
		event:   RequestEvent{RequestID: request.RequestID, ParentRequestID: request.ParentRequestID},
	}
	// Register before event finalization so panics from forwarding or deferred
	// observer delivery both release an acquired lifecycle before propagating.
	defer lifecycle.rollbackOnPanic()
	defer func() { lifecycle.finish(result, reservation, apiErr) }()

	resolved, apiErr := s.resolveForwardRequest(request, &lifecycle)
	if apiErr != nil {
		return proxy.Result{}, nil, apiErr
	}
	if reserver, ok := s.observer.(ResponseCompleteReserver); ok {
		reservedLifecycle, rollback, reserved := reserver.ReserveResponseComplete(ctx, request.RequestID)
		if !reserved || reservedLifecycle == nil {
			if rollback != nil {
				rollback()
			}
			if reservationErr := ctx.Err(); reservationErr != nil {
				return proxy.Result{Cancelled: true, Err: reservationErr}, nil, nil
			}
			return proxy.Result{}, nil, gatewayUnavailable(s.retryAfter)
		}
		reservation = reservedLifecycle
		lifecycle.reservationRollback = rollback
	}
	resolved, apiErr = s.prepareForwardPayload(request, resolved)
	if apiErr != nil {
		return proxy.Result{}, reservation, apiErr
	}

	lifecycle.admissionStarted = time.Now()
	poolRuntime, releasePool, poolErr := s.acquirePool(ctx, resolved.client.ID, resolved.pool)
	lifecycle.event.PoolState = poolRuntime.State
	if poolErr != nil {
		return proxy.Result{}, reservation, poolErr
	}
	defer releasePool()
	limit := admission.EffectiveLimit(resolved.client.PriorityClass, poolRuntime.State, resolved.client.MaxConcurrency)
	lease, ok := s.limiter.Acquire(resolved.client.ID, limit)
	if !ok {
		return proxy.Result{}, reservation, overloaded(s.retryAfter, DecisionPriorityConcurrencyLimit)
	}
	defer lease.Release()
	inflight := InflightEvent{
		Client: resolved.client.Name, Model: resolved.publicModel, PriorityClass: resolved.client.PriorityClass,
	}
	if s.observer != nil {
		s.observer.ClientInflight(inflight, 1)
		defer s.observer.ClientInflight(inflight, -1)
	}

	selector := targetSelector{
		service: s, client: resolved.client, pool: resolved.pool, affinity: resolved.affinityKey,
		inflight: inflight, lifecycle: &lifecycle,
	}
	target, err := selector.selectTarget(nil)
	if err != nil {
		return proxy.Result{}, reservation, backendUnavailable(s.retryAfter, DecisionNoEligibleBackend)
	}

	headers := request.Headers.Clone()
	headers.Del("X-Vllm-Priority")
	headers.Del(SessionAffinityHeader)
	headers.Del("Authorization")
	proxyRequest := proxy.Request{
		Method: request.Method, Path: request.Path, Headers: headers, Body: resolved.payload,
		RequestID: request.RequestID, Priority: resolved.client.VLLMPriority, Target: target,
	}
	proxyRequest.SelectAlternate = selector.selectTarget
	result = s.forwarder.Forward(ctx, writer, proxyRequest)
	if result.Err != nil && !result.ResponseStarted {
		if result.Cancelled || errors.Is(result.Err, context.Canceled) {
			return result, reservation, nil
		}
		return result, reservation, upstreamError()
	}
	return result, reservation, nil
}
