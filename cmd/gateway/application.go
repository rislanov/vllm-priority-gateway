package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/config"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/monitor"
	"github.com/rislanov/vllm-priority-gateway/internal/observability"
	"github.com/rislanov/vllm-priority-gateway/internal/pressure"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
	"github.com/rislanov/vllm-priority-gateway/internal/web"
)

type gatewayApplication struct {
	handler         http.Handler
	database        *store.SQLite
	requestRecorder *analytics.Recorder
	apiKeyUsage     *apiKeyUsageRecorder
	manager         *monitor.Manager
	transport       *http.Transport
	runtimeCancel   context.CancelFunc
	runtimeDone     <-chan struct{}
	closeOnce       sync.Once
	closeErr        error
}

func newGatewayApplication(
	ctx context.Context,
	cfg config.Config,
	getenv config.LookupFunc,
	stderr io.Writer,
) (application *gatewayApplication, applicationErr error) {
	database, err := store.Open(ctx, cfg.DatabasePath)
	if err != nil {
		return nil, err
	}
	application = &gatewayApplication{database: database}
	owned := application
	defer func() {
		if applicationErr == nil {
			return
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
		defer cancel()
		if owned.requestRecorder != nil {
			if err := owned.Close(closeCtx); err != nil {
				applicationErr = errors.Join(applicationErr, fmt.Errorf("drain usage recorder: %w", err))
			}
			return
		}
		if owned.apiKeyUsage != nil {
			owned.apiKeyUsage.Close()
		}
		if owned.manager != nil {
			owned.manager.Shutdown()
		}
		if owned.transport != nil {
			owned.transport.CloseIdleConnections()
		}
		applicationErr = errors.Join(applicationErr, owned.database.Close())
	}()

	registryValue := registry.New(database)
	if err := registryValue.Reload(ctx); err != nil {
		return nil, fmt.Errorf("initialize registry: %w", err)
	}
	transport := &http.Transport{
		Proxy:             http.ProxyFromEnvironment,
		DialContext:       (&net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: 256, MaxIdleConnsPerHost: 64,
		IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
	}
	application.transport = transport
	upstreamClient := &http.Client{Transport: transport}
	manager := monitor.NewManager(ctx, monitor.Options{
		HTTPClient: upstreamClient, HealthInterval: cfg.HealthInterval, HealthTimeout: cfg.HealthTimeout,
		MetricsInterval: cfg.MetricsInterval, MetricsTimeout: cfg.MetricsTimeout, StaleAfter: cfg.MetricsStaleAfter,
		UnhealthyAfter: cfg.UnhealthyAfter, RecoveryAfter: cfg.RecoveryAfter,
		Circuit: circuitbreaker.Options{
			FailureThreshold: cfg.CircuitFailureThreshold, FailureWindow: cfg.CircuitFailureWindow,
			OpenCooldown: cfg.CircuitOpenCooldown, HalfOpenMaxProbes: cfg.CircuitHalfOpenMaxProbes,
		},
		Limits:     pressure.Limits{QueueSoft: cfg.QueueSoftLimit, KVSoft: cfg.KVSoftLimit, KVHard: cfg.KVHardLimit},
		EWMAWindow: cfg.EWMAWindow, BusyThreshold: cfg.BusyThreshold, SaturatedThreshold: cfg.SaturatedThreshold,
		PoolThresholds: pressure.Thresholds{
			Busy: cfg.BusyThreshold, Saturated: cfg.SaturatedThreshold, Emergency: cfg.EmergencyThreshold,
			BusyRecovery: cfg.BusyRecoveryThreshold, SaturatedRecovery: cfg.SaturatedRecoveryThreshold,
			EmergencyRecovery: cfg.EmergencyRecoveryThreshold, EnterWindow: cfg.OverloadEnterWindow,
			RecoveryWindow: cfg.OverloadRecoveryWindow,
		},
	})
	application.manager = manager
	if err := manager.Reconcile(backends(registryValue.Snapshot())); err != nil {
		return nil, fmt.Errorf("start backend monitoring: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	metrics := observability.NewMetrics()
	apiKeyUsage := newAPIKeyUsageRecorder(ctx, projectedKeyUsageStore{destination: database, projection: registryValue})
	application.apiKeyUsage = apiKeyUsage
	requestRecorder := analytics.NewRecorder(database, cfg.AnalyticsRetention, metrics.UsagePersistenceFailure, logger)
	application.requestRecorder = requestRecorder
	service := gateway.New(gateway.Dependencies{
		Registry: registryValue, HMACSecret: cfg.APIKeyHMACSecret, Limiter: admission.NewLimiter(),
		Runtime: manager, Router: routing.NewWithSessionAffinity(
			cfg.RoutingPressureEpsilon,
			cfg.SessionAffinityMaxPressure,
			routing.NewRandomSource(time.Now().UnixNano()),
		),
		Forwarder: proxy.New(upstreamClient), Usage: apiKeyUsage,
		Observer: observability.Multi(metrics, observability.NewLogger(logger), requestRecorder), LookupEnv: getenv,
		RetryAfter: cfg.RetryAfter,
	})
	publicHandler := httpapi.NewPublicHandler(service, cfg.RequestBodyLimit, nil)
	adminService, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Analytics: database, Registry: registryValue, Runtime: manager,
		HMACSecret: cfg.APIKeyHMACSecret, Random: rand.Reader,
	})
	if err != nil {
		return nil, err
	}
	adminAPI := httpapi.NewAdminAPI(adminService)
	adminWeb, err := web.New(adminService)
	if err != nil {
		return nil, err
	}
	security, err := httpapi.NewAdminSecurity(httpapi.AdminSecurityConfig{
		Username: cfg.AdminUsername, Password: cfg.AdminPassword, Random: rand.Reader,
	})
	if err != nil {
		return nil, err
	}

	router := chi.NewRouter()
	router.Use(middleware.Recoverer)
	router.Get("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, map[string]any{"status": "alive"})
	})
	router.Get("/readyz", func(writer http.ResponseWriter, _ *http.Request) {
		view := adminService.View()
		writeJSON(writer, http.StatusOK, map[string]any{
			"status": "ready", "revision": view.Revision, "backendAvailability": availableBackends(view),
		})
	})
	inferenceReadinessHandler := httpapi.NewInferenceReadinessHandler(service)
	router.Get("/inference-readyz", inferenceReadinessHandler.ServeHTTP)
	router.Handle("/metrics", metrics.Handler())
	router.Handle("/v1", publicHandler)
	router.Handle("/v1/*", publicHandler)
	router.Handle("/admin/api", security.Wrap(adminAPI))
	router.Handle("/admin/api/*", security.Wrap(adminAPI))
	router.Handle("/admin", security.Wrap(adminWeb))
	router.Handle("/admin/*", security.Wrap(adminWeb))
	application.handler = router

	runtimeCtx, runtimeCancel := context.WithCancel(ctx)
	runtimeDone := make(chan struct{})
	application.runtimeCancel = runtimeCancel
	application.runtimeDone = runtimeDone
	go func() {
		defer close(runtimeDone)
		updateBackendMetrics(runtimeCtx, metrics, registryValue, manager, cfg.MetricsInterval)
	}()
	return application, nil
}

func (a *gatewayApplication) Handler() http.Handler {
	return a.handler
}

func (a *gatewayApplication) Close(ctx context.Context) error {
	a.closeOnce.Do(func() {
		if a.apiKeyUsage != nil {
			a.apiKeyUsage.Close()
		}
		if a.requestRecorder != nil {
			a.closeErr = closeRecorderStore(ctx, a.requestRecorder, a.database)
		} else if a.database != nil {
			a.closeErr = a.database.Close()
		}
		if a.runtimeCancel != nil {
			a.runtimeCancel()
		}
		if a.runtimeDone != nil {
			<-a.runtimeDone
		}
		if a.manager != nil {
			a.manager.Shutdown()
		}
		if a.transport != nil {
			a.transport.CloseIdleConnections()
		}
	})
	return a.closeErr
}

func backends(snapshot *registry.Snapshot) []domain.Backend {
	values := make([]domain.Backend, 0, len(snapshot.BackendsByID))
	for _, backend := range snapshot.BackendsByID {
		values = append(values, backend)
	}
	return values
}

func availableBackends(view httpapi.AdminView) int {
	count := 0
	for _, backend := range view.Backends {
		if backend.Enabled && !backend.Draining && backend.Runtime.Healthy && backend.Runtime.MetricsFresh {
			count++
		}
	}
	return count
}

type recorderStoreLifecycle interface {
	Close(context.Context) error
	Done() <-chan struct{}
}

type closeStore interface {
	Close() error
}

func closeRecorderStore(ctx context.Context, recorder recorderStoreLifecycle, destination closeStore) error {
	recorderErr := recorder.Close(ctx)
	select {
	case <-recorder.Done():
		return errors.Join(recorderErr, destination.Close())
	default:
		go func() {
			<-recorder.Done()
			_ = destination.Close()
		}()
		return recorderErr
	}
}

type keyUsageStore interface {
	TouchKeyLastUsed(context.Context, int64, time.Time) error
}

type keyUsageProjection interface {
	MarkKeyUsed(int64, time.Time) bool
}

type projectedKeyUsageStore struct {
	destination keyUsageStore
	projection  keyUsageProjection
}

func (s projectedKeyUsageStore) TouchKeyLastUsed(ctx context.Context, keyID int64, usedAt time.Time) error {
	if err := s.destination.TouchKeyLastUsed(ctx, keyID, usedAt); err != nil {
		return err
	}
	s.projection.MarkKeyUsed(keyID, usedAt)
	return nil
}

type apiKeyUsageEvent struct {
	keyID int64
	at    time.Time
}

type apiKeyUsageRecorder struct {
	cancel context.CancelFunc
	done   chan struct{}
	events chan apiKeyUsageEvent
}

func newAPIKeyUsageRecorder(parent context.Context, destination keyUsageStore) *apiKeyUsageRecorder {
	ctx, cancel := context.WithCancel(parent)
	recorder := &apiKeyUsageRecorder{cancel: cancel, done: make(chan struct{}), events: make(chan apiKeyUsageEvent, 256)}
	go func() {
		defer close(recorder.done)
		last := make(map[int64]time.Time)
		for {
			select {
			case event := <-recorder.events:
				if previous := last[event.keyID]; !previous.IsZero() && event.at.Sub(previous) < time.Minute {
					continue
				}
				if destination.TouchKeyLastUsed(ctx, event.keyID, event.at) == nil {
					last[event.keyID] = event.at
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return recorder
}

func (r *apiKeyUsageRecorder) Record(keyID int64, usedAt time.Time) {
	select {
	case r.events <- apiKeyUsageEvent{keyID: keyID, at: usedAt}:
	default:
	}
}

func (r *apiKeyUsageRecorder) Close() {
	r.cancel()
	<-r.done
}
