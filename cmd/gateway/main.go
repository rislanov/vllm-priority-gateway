package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rislanov/vllm-priority-gateway/internal/admission"
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

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.LookupEnv, nil, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv config.LookupFunc, listener net.Listener, stdout, stderr io.Writer) error {
	cfg, err := config.Load(getenv)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	database, err := store.Open(ctx, cfg.DatabasePath)
	if err != nil {
		return err
	}
	defer database.Close()

	registryValue := registry.New(database)
	if err := registryValue.Reload(ctx); err != nil {
		return fmt.Errorf("initialize registry: %w", err)
	}
	transport := &http.Transport{
		Proxy:             http.ProxyFromEnvironment,
		DialContext:       (&net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: 256, MaxIdleConnsPerHost: 64,
		IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
	}
	defer transport.CloseIdleConnections()
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
	defer manager.Shutdown()
	if err := manager.Reconcile(backends(registryValue.Snapshot())); err != nil {
		return fmt.Errorf("start backend monitoring: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	metrics := observability.NewMetrics()
	usage := newUsageRecorder(ctx, projectedKeyUsageStore{destination: database, projection: registryValue})
	defer usage.Close()
	service := gateway.New(gateway.Dependencies{
		Registry: registryValue, HMACSecret: cfg.APIKeyHMACSecret, Limiter: admission.NewLimiter(),
		Runtime: manager, Router: routing.NewWithSessionAffinity(
			cfg.RoutingPressureEpsilon,
			cfg.SessionAffinityMaxPressure,
			routing.NewRandomSource(time.Now().UnixNano()),
		),
		Forwarder: proxy.New(upstreamClient), Usage: usage,
		Observer: observability.Multi(metrics, observability.NewLogger(logger)), LookupEnv: getenv,
		RetryAfter: cfg.RetryAfter,
	})
	publicHandler := httpapi.NewPublicHandler(service, cfg.RequestBodyLimit, nil)
	adminService, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Registry: registryValue, Runtime: manager, HMACSecret: cfg.APIKeyHMACSecret, Random: rand.Reader,
	})
	if err != nil {
		return err
	}
	adminAPI := httpapi.NewAdminAPI(adminService)
	adminWeb, err := web.New(adminService)
	if err != nil {
		return err
	}
	security, err := httpapi.NewAdminSecurity(httpapi.AdminSecurityConfig{
		Username: cfg.AdminUsername, Password: cfg.AdminPassword, Random: rand.Reader,
	})
	if err != nil {
		return err
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

	if listener == nil {
		listener, err = net.Listen("tcp", cfg.ListenAddress)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", cfg.ListenAddress, err)
		}
	}
	server := &http.Server{
		Handler: router, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	go updateBackendMetrics(ctx, metrics, registryValue, manager, cfg.MetricsInterval)
	serveError := make(chan error, 1)
	go func() { serveError <- server.Serve(listener) }()
	fmt.Fprintf(stdout, "vLLM Priority Gateway listening on %s\n", listener.Addr())

	select {
	case err := <-serveError:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve gateway: %w", err)
		}
		return nil
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		return fmt.Errorf("graceful HTTP shutdown: %w", err)
	}
	err = <-serveError
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve gateway: %w", err)
	}
	return nil
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

type runtimeMetrics interface {
	PoolSnapshot(int64, time.Time) domain.PoolRuntime
	Snapshot(int64, time.Time) domain.BackendRuntime
}

func publishRuntimeMetrics(metrics *observability.Metrics, snapshot *registry.Snapshot, runtime runtimeMetrics, at time.Time) {
	for _, pool := range snapshot.PoolsByID {
		metrics.SetPool(pool.PublicModelName, runtime.PoolSnapshot(pool.ID, at))
	}
	for _, backend := range snapshot.BackendsByID {
		pool := snapshot.PoolsByID[backend.ModelPoolID]
		metrics.SetBackend(pool.PublicModelName, backend.Name, runtime.Snapshot(backend.ID, at))
	}
}

func updateBackendMetrics(ctx context.Context, metrics *observability.Metrics, registryValue *registry.Registry, manager *monitor.Manager, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case at := <-ticker.C:
			publishRuntimeMetrics(metrics, registryValue.Snapshot(), manager, at)
		case <-ctx.Done():
			return
		}
	}
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
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

type usageEvent struct {
	keyID int64
	at    time.Time
}

type usageRecorder struct {
	cancel context.CancelFunc
	done   chan struct{}
	events chan usageEvent
}

func newUsageRecorder(parent context.Context, destination keyUsageStore) *usageRecorder {
	ctx, cancel := context.WithCancel(parent)
	recorder := &usageRecorder{cancel: cancel, done: make(chan struct{}), events: make(chan usageEvent, 256)}
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

func (r *usageRecorder) Record(keyID int64, usedAt time.Time) {
	select {
	case r.events <- usageEvent{keyID: keyID, at: usedAt}:
	default:
	}
}

func (r *usageRecorder) Close() {
	r.cancel()
	<-r.done
}
