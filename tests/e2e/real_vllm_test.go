package e2e_test

import (
	"context"
	"net/http"
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

	status := h.adminStatus()
	pool := status.requirePool(t, cfg.model)
	if !pool.Enabled {
		t.Fatalf("model pool %q is disabled", cfg.model)
	}
	if pool.Runtime.AvailableBackends < cfg.expectedBackends {
		t.Fatalf("pool available backends = %d, want at least %d", pool.Runtime.AvailableBackends, cfg.expectedBackends)
	}
	status.requireHealthyFreshBackends(t, pool.ID, cfg.expectedBackends)
	t.Logf("ready revision=%d available_backends=%d pool_state=%s", ready.Revision, ready.BackendAvailability, pool.Runtime.State)

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
	for _, family := range []string{"llmgw_requests_total", "llmgw_backend_pressure", "llmgw_backend_running_requests"} {
		if !metrics.containsFamily(family) {
			t.Fatalf("/metrics does not expose %s", family)
		}
	}
}

func TestPriorityIsolationWithRealVLLM(t *testing.T) {
	cfg := loadE2EConfig(t, modePriority)
	h := newRemoteHarness(t, cfg)

	loadCtx, cancelLoad := context.WithCancel(context.Background())
	load := h.startHighLoad(loadCtx)
	t.Cleanup(func() {
		cancelLoad()
		load.wait(10 * time.Second)
	})

	saturated := h.waitForPool(func(pool adminPool) bool {
		return pool.Runtime.State == "saturated" || pool.Runtime.State == "emergency"
	}, cfg.saturationTimeout)
	if cfg.requireAllWaiting && !saturated.Runtime.AllBackendsWaiting {
		t.Fatalf("pool reached %s without all backends waiting: %+v", saturated.Runtime.State, saturated.Runtime)
	}
	t.Logf("saturation state=%s pressure=%.4f available_backends=%d all_waiting=%t", saturated.Runtime.State, saturated.Runtime.BestBackendPressure, saturated.Runtime.AvailableBackends, saturated.Runtime.AllBackendsWaiting)

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

	if cfg.drainBackendID > 0 {
		beforeDrain := h.adminStatus()
		target := beforeDrain.requireBackend(t, cfg.drainBackendID)
		if target.ModelPoolID != saturated.ID {
			t.Fatalf("drain backend %d belongs to pool %d, want selected pool %d", target.ID, target.ModelPoolID, saturated.ID)
		}
		if !target.Enabled || target.Draining {
			t.Fatalf("drain backend %d must be enabled and not already draining; enabled=%t draining=%t", target.ID, target.Enabled, target.Draining)
		}
		restoreNonDraining := true
		t.Cleanup(func() {
			if restoreNonDraining {
				cancelLoad()
				load.wait(10 * time.Second)
				h.resumeBackend(cfg.drainBackendID)
				h.waitForBackend(cfg.drainBackendID, func(backend adminBackend) bool { return !backend.Draining }, cfg.saturationTimeout)
			}
		})
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
		restoreNonDraining = false
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
