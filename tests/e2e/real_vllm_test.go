package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestProductionSmoke(t *testing.T) {
	cfg := loadE2EConfig(t, modeSmoke)
	h := newRemoteHarness(t, cfg)

	ready := h.ready()
	if ready.Status != "ready" {
		t.Fatalf("ready status = %q, want ready", ready.Status)
	}
	if ready.BackendAvailability < cfg.expectedBackends {
		t.Fatalf("backend availability = %d, want at least %d", ready.BackendAvailability, cfg.expectedBackends)
	}
	inferenceReady, inferenceStatus := h.inferenceReadiness()
	if inferenceStatus != http.StatusOK || inferenceReady.Status != "ready" {
		t.Fatalf("inference readiness = HTTP %d %+v, want HTTP 200 ready", inferenceStatus, inferenceReady)
	}
	if inferenceReady.PoolAvailability < 1 || inferenceReady.BackendAvailability < cfg.expectedBackends {
		t.Fatalf("inference availability = pools %d backends %d, want at least 1 pool and %d backends", inferenceReady.PoolAvailability, inferenceReady.BackendAvailability, cfg.expectedBackends)
	}

	status := h.adminStatus()
	if len(status.Pools) < 1 || len(status.Backends) < cfg.expectedBackends {
		t.Fatalf("admin inventory = %d pools and %d backends, want at least 1 pool and %d backends", len(status.Pools), len(status.Backends), cfg.expectedBackends)
	}
	pool := status.requirePool(t, cfg.model)
	if !pool.Enabled {
		t.Fatalf("model pool %q is disabled", cfg.model)
	}
	if pool.Runtime.AvailableBackends < cfg.expectedBackends {
		t.Fatalf("pool available backends = %d, want at least %d", pool.Runtime.AvailableBackends, cfg.expectedBackends)
	}
	status.requireHealthyFreshBackends(t, pool.ID, cfg.expectedBackends)
	t.Logf("management_ready revision=%d inference_ready revision=%d pools=%d backends=%d pool_state=%s", ready.Revision, inferenceReady.Revision, inferenceReady.PoolAvailability, inferenceReady.BackendAvailability, pool.Runtime.State)

	models := h.models(cfg.highKey)
	if !models.contains(cfg.model) {
		t.Fatalf("/v1/models does not contain public model %q: %+v", cfg.model, models.Data)
	}

	result := h.completion(context.Background(), completionRequest{
		Key: cfg.highKey, Prompt: "Production smoke check.", MaxTokens: 4, Stream: true,
	})
	result.requireCompleteStream(t)
	if result.FirstByte <= 0 || result.Duration < result.FirstByte {
		t.Fatalf("invalid streaming timing: first_byte=%s duration=%s", result.FirstByte, result.Duration)
	}
	t.Logf("streaming inference status=%d first_byte=%s duration=%s", result.Status, result.FirstByte, result.Duration)

	metrics := h.metrics()
	for _, family := range []string{
		"llmgw_requests_total", "llmgw_backend_pressure", "llmgw_backend_running_requests",
		"llmgw_backend_circuit_state", "llmgw_backend_circuit_failures", "llmgw_pool_gateway_inflight",
		"llmgw_pool_waiting_requests", "llmgw_pool_available_backends",
	} {
		if !metrics.containsFamily(family) {
			t.Fatalf("/metrics does not expose %s", family)
		}
	}
}

func TestPriorityIsolationWithRealVLLM(t *testing.T) {
	cfg := loadE2EConfig(t, modePriority)
	h := newRemoteHarness(t, cfg)
	originalStatus := h.adminStatus()
	originalPool := originalStatus.requirePool(t, cfg.model)
	mutationCleanup, err := newAdminMutationCleanup(h, originalStatus, originalPool.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.probeTimeout)
		defer cancel()
		if err := mutationCleanup.Restore(ctx); err != nil {
			t.Errorf("restore priority E2E Admin mutations: %v", err)
		}
	})

	loadCtx, cancelLoad := context.WithCancel(context.Background())
	load := h.startHighLoad(loadCtx)
	t.Cleanup(func() {
		cancelLoad()
		load.wait(10 * time.Second)
	})

	saturated := h.waitForPool(func(pool adminPool) bool {
		return (pool.Runtime.State == "saturated" || pool.Runtime.State == "emergency") &&
			pool.Runtime.GatewayInflight > 0 && pool.Runtime.TotalWaiting > 0
	}, cfg.saturationTimeout)
	if cfg.requireAllWaiting && !saturated.Runtime.AllBackendsWaiting {
		t.Fatalf("pool reached %s without all backends waiting: %+v", saturated.Runtime.State, saturated.Runtime)
	}
	t.Logf("saturation state=%s pressure=%.4f gateway_inflight=%d total_waiting=%.0f available_backends=%d all_waiting=%t", saturated.Runtime.State, saturated.Runtime.BestBackendPressure, saturated.Runtime.GatewayInflight, saturated.Runtime.TotalWaiting, saturated.Runtime.AvailableBackends, saturated.Runtime.AllBackendsWaiting)

	for _, probe := range []completionRequest{
		{Key: cfg.lowKeys[0], Prompt: "Ordinary low-priority probe.", MaxTokens: 2},
		{Key: cfg.lowKeys[1], Prompt: "Spoofed low-priority probe.", MaxTokens: 2, Priority: intPointer(-9999), ExtraHeaders: map[string]string{"X-Vllm-Priority": "-9999"}},
		{Key: cfg.lowKeys[2], Prompt: "Affinity low-priority probe.", MaxTokens: 2, ExtraHeaders: map[string]string{"X-LLM-Session-Id": "e2e-low-affinity"}},
	} {
		h.completion(context.Background(), probe).requireOverloaded(t)
	}

	high := h.completion(context.Background(), completionRequest{
		Key: cfg.highKey, Prompt: "High-priority continuity probe.", MaxTokens: 4, Stream: true,
	})
	high.requireCompleteStream(t)
	critical := h.completion(context.Background(), completionRequest{
		Key: cfg.criticalKey, Prompt: "Critical-priority continuity probe.", MaxTokens: 4, Stream: true,
	})
	critical.requireCompleteStream(t)
	t.Logf("continuity high_first_byte=%s critical_first_byte=%s", high.FirstByte, critical.FirstByte)

	limitedPool := saturated
	limitedPool.MaxGatewayInflight = 1
	if _, err := h.updatePool(context.Background(), limitedPool); err != nil {
		t.Fatalf("set priority E2E pool limit: %v", err)
	}
	h.waitForPool(func(pool adminPool) bool {
		return pool.MaxGatewayInflight == 1 && pool.Runtime.GatewayInflight >= 1
	}, cfg.saturationTimeout)
	h.completion(context.Background(), completionRequest{
		Key: cfg.criticalKey, Prompt: "Critical probe bounded by pool safety.", MaxTokens: 2,
	}).requireOverloaded(t)
	if _, err := h.updatePool(context.Background(), originalPool); err != nil {
		t.Fatalf("restore pool limit before continuity probe: %v", err)
	}
	postLimitCritical := h.completion(context.Background(), completionRequest{
		Key: cfg.criticalKey, Prompt: "Critical continuity after pool limit restoration.", MaxTokens: 4, Stream: true,
	})
	postLimitCritical.requireCompleteStream(t)
	t.Logf("pool safety rejected Critical at max_gateway_inflight=1 and continuity resumed after restoration; first_byte=%s", postLimitCritical.FirstByte)

	if cfg.drainBackendID > 0 {
		beforeDrain := h.adminStatus()
		target := beforeDrain.requireBackend(t, cfg.drainBackendID)
		if target.ModelPoolID != saturated.ID {
			t.Fatalf("drain backend %d belongs to pool %d, want selected pool %d", target.ID, target.ModelPoolID, saturated.ID)
		}
		if !target.Enabled || target.Draining {
			t.Fatalf("drain backend %d must be enabled and not already draining; enabled=%t draining=%t", target.ID, target.Enabled, target.Draining)
		}
		h.drainBackend(cfg.drainBackendID)
		h.waitForBackend(cfg.drainBackendID, func(backend adminBackend) bool { return backend.Draining }, cfg.saturationTimeout)
		h.completion(context.Background(), completionRequest{
			Key: cfg.lowKeys[0], Prompt: "Low-priority probe while one backend is draining.", MaxTokens: 2,
		}).requireOverloaded(t)
		drainHigh := h.completion(context.Background(), completionRequest{
			Key: cfg.highKey, Prompt: "High-priority probe while one backend is draining.", MaxTokens: 2, Stream: true,
		})
		drainHigh.requireCompleteStream(t)
		t.Logf("drain scenario backend_id=%d preserved High admission and rejected Low", cfg.drainBackendID)
		h.resumeBackend(cfg.drainBackendID)
		h.waitForBackend(cfg.drainBackendID, func(backend adminBackend) bool { return !backend.Draining }, cfg.saturationTimeout)
	}

	recoveryStarted := time.Now()
	cancelLoad()
	h.completion(context.Background(), completionRequest{
		Key: cfg.lowKeys[0], Prompt: "Immediate hysteresis probe.", MaxTokens: 2,
	}).requireOverloaded(t)
	load.wait(10 * time.Second)
	load.requireNoHTTPFailures(t)

	h.waitForPool(func(pool adminPool) bool { return pool.Runtime.State == "busy" }, cfg.recoveryTimeout)
	t.Logf("recovery reached busy after %s", time.Since(recoveryStarted))
	h.completion(context.Background(), completionRequest{
		Key: cfg.lowKeys[0], Prompt: "Busy-state recovery probe.", MaxTokens: 2,
	}).requireStatus(t, http.StatusOK)

	h.waitForPool(func(pool adminPool) bool { return pool.Runtime.State == "normal" }, cfg.recoveryTimeout)
	t.Logf("recovery reached normal after %s", time.Since(recoveryStarted))
	h.completion(context.Background(), completionRequest{
		Key: cfg.lowKeys[0], Prompt: "Normal-state recovery probe.", MaxTokens: 2,
	}).requireStatus(t, http.StatusOK)
}

func TestCircuitBreakerRecoveryWithRealVLLM(t *testing.T) {
	cfg := loadE2EConfig(t, modeResilience)
	h := newRemoteHarness(t, cfg)

	originalStatus := h.adminStatus()
	pool := originalStatus.requirePool(t, cfg.model)
	target := originalStatus.requireBackend(t, cfg.circuitBackendID)
	if target.ModelPoolID != pool.ID {
		t.Fatalf("circuit backend %d belongs to pool %d, want selected pool %d", target.ID, target.ModelPoolID, pool.ID)
	}
	if !target.Enabled || target.Draining {
		t.Fatalf("circuit backend %d must be enabled and not draining; enabled=%t draining=%t", target.ID, target.Enabled, target.Draining)
	}
	originalTargetURL, err := url.Parse(target.BaseURL)
	if err != nil || originalTargetURL.Host == "" {
		t.Fatalf("circuit backend %d has invalid target URL", target.ID)
	}
	mutationCleanup, err := newAdminMutationCleanup(h, originalStatus, pool.ID)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := startFaultProxy(originalTargetURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		restoreCtx, restoreCancel := context.WithTimeout(context.Background(), cfg.probeTimeout)
		if err := mutationCleanup.Restore(restoreCtx); err != nil {
			t.Errorf("restore resilience E2E Admin mutations: %v", err)
		}
		restoreCancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), cfg.probeTimeout)
		defer closeCancel()
		if err := proxy.Close(closeCtx); err != nil {
			t.Errorf("close resilience fault proxy: %v", err)
		}
	})

	isolatedPool := pool
	isolatedPool.MaxGatewayInflight = 0
	isolatedPool.MaxWaiting = 0
	if _, err := h.updatePool(context.Background(), isolatedPool); err != nil {
		t.Fatalf("disable pool limits for isolated circuit scenario: %v", err)
	}
	proxiedTarget := target
	proxiedTarget.BaseURL = proxy.URL()
	if _, err := h.updateBackend(context.Background(), proxiedTarget); err != nil {
		t.Fatalf("point circuit backend at fault proxy: %v", err)
	}
	h.waitForBackend(target.ID, func(backend adminBackend) bool {
		return backend.BaseURL == proxy.URL() && backend.Runtime.Healthy && backend.Runtime.MetricsFresh
	}, cfg.saturationTimeout)
	for _, backend := range originalStatus.Backends {
		if backend.ModelPoolID != pool.ID || backend.ID == target.ID || backend.Draining {
			continue
		}
		drained := backend
		drained.Draining = true
		if _, err := h.updateBackend(context.Background(), drained); err != nil {
			t.Fatalf("drain sibling backend %d: %v", backend.ID, err)
		}
	}
	h.waitForPool(func(pool adminPool) bool { return pool.Runtime.AvailableBackends == 1 }, cfg.saturationTimeout)

	proxy.SetFaulting(true)
	for attempt := 1; attempt <= cfg.circuitFailureCount; attempt++ {
		result := h.completion(context.Background(), completionRequest{
			Key: cfg.highKey, Prompt: "Circuit breaker failure injection.", MaxTokens: 2,
			ExtraHeaders: map[string]string{"X-Request-Id": fmt.Sprintf("e2e-circuit-failure-%d", attempt)},
		})
		result.requireStatus(t, http.StatusServiceUnavailable)
	}
	opened := h.waitForBackend(target.ID, func(backend adminBackend) bool {
		return backend.Runtime.CircuitState == "open" && backend.Runtime.Healthy && backend.Runtime.MetricsFresh
	}, cfg.saturationTimeout)
	if opened.Runtime.CircuitFailures < 1 {
		t.Fatal("open circuit did not retain any qualifying failures")
	}
	unavailable, unavailableStatus := h.inferenceReadiness()
	if unavailableStatus != http.StatusServiceUnavailable || unavailable.Status != "unavailable" || unavailable.PoolAvailability != 0 || unavailable.BackendAvailability != 0 {
		t.Fatalf("open-circuit inference readiness = HTTP %d %+v, want HTTP 503 unavailable", unavailableStatus, unavailable)
	}

	proxy.SetFaulting(false)
	halfOpen := h.waitForBackend(target.ID, func(backend adminBackend) bool {
		return backend.Runtime.CircuitState == "half_open" && backend.Runtime.CircuitAvailable && backend.Runtime.CircuitProbesInFlight == 0 && backend.Runtime.Healthy && backend.Runtime.MetricsFresh
	}, cfg.recoveryTimeout)
	h.waitForPool(func(pool adminPool) bool { return pool.Runtime.AvailableBackends == 1 }, cfg.recoveryTimeout)
	probe := h.completion(context.Background(), completionRequest{
		Key: cfg.highKey, Prompt: "Circuit breaker half-open streaming probe.", MaxTokens: 4, Stream: true,
		ExtraHeaders: map[string]string{"X-Request-Id": "e2e-circuit-recovery-probe"},
	})
	probe.requireCompleteStream(t)
	closed := h.waitForBackend(target.ID, func(backend adminBackend) bool {
		return backend.Runtime.CircuitState == "closed" && backend.Runtime.CircuitAvailable && backend.Runtime.CircuitFailures == 0
	}, cfg.recoveryTimeout)
	recovered, recoveredStatus := h.inferenceReadiness()
	if recoveredStatus != http.StatusOK || recovered.Status != "ready" || recovered.PoolAvailability < 1 || recovered.BackendAvailability < 1 {
		t.Fatalf("recovered inference readiness = HTTP %d %+v, want HTTP 200 ready", recoveredStatus, recovered)
	}
	t.Logf("circuit backend=%d opened_failures=%d retry_at=%s half_open_probes=%d closed_failures=%d recovery_first_byte=%s", target.ID, opened.Runtime.CircuitFailures, opened.Runtime.CircuitRetryAt, halfOpen.Runtime.CircuitProbesInFlight, closed.Runtime.CircuitFailures, probe.FirstByte)
}
