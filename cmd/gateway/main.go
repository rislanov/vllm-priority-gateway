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
	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/config"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordlocal "github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
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
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
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

func run(ctx context.Context, getenv config.LookupFunc, listener net.Listener, stdout, stderr io.Writer) (runErr error) {
	cfg, err := config.Load(getenv)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	metrics := observability.NewMetrics()
	eventLogger := observability.NewLogger(logger)
	var database interface {
		store.ConfigurationStore
		store.AnalyticsStore
		store.LifecycleStore
	}
	switch cfg.DatabaseDriver {
	case "sqlite":
		database, err = store.Open(ctx, cfg.DatabasePath)
	case "postgres":
		database, err = pgstore.Open(ctx, pgstore.Options{DatabaseURL: cfg.DatabaseURL, MigrationURL: cfg.DatabaseMigrationURL, ConfigMaxConns: cfg.PostgresConfigMaxConns, AnalyticsMaxConns: cfg.PostgresAnalyticsMaxConns, CoordinationMaxConns: cfg.PostgresCoordinationMaxConns})
	}
	if err != nil {
		return err
	}
	databaseOwnedByRun := true
	defer func() {
		if databaseOwnedByRun {
			runErr = errors.Join(runErr, database.Close())
		}
	}()
	runContext, cancelRun := context.WithCancel(ctx)
	ctx = runContext
	defer cancelRun()

	registryValue := registry.New(database)
	if err := registryValue.Reload(ctx); err != nil {
		return fmt.Errorf("initialize registry: %w", err)
	}
	revisionGuard := &registry.RevisionGuard{}
	revisionGuard.ObservePublished(registryValue.Snapshot().Revision)
	replicaID := uuid.New()
	circuitOptions := circuitbreaker.Options{
		FailureThreshold: cfg.CircuitFailureThreshold, FailureWindow: cfg.CircuitFailureWindow,
		OpenCooldown: cfg.CircuitOpenCooldown, HalfOpenMaxProbes: cfg.CircuitHalfOpenMaxProbes,
	}
	var admissionCoordinator coordination.AdmissionCoordinator
	var circuitCoordinator coordination.CircuitCoordinator
	var replicaManager *coordpostgres.ReplicaManager
	if postgresDatabase, ok := database.(*pgstore.Store); ok {
		admissionCoordinator = coordpostgres.NewAdmissionCoordinator(postgresDatabase, cfg.CoordinationTimeout)
		circuitCoordinator, err = coordpostgres.NewCircuitCoordinator(postgresDatabase, cfg.CoordinationTimeout, circuitOptions)
		if err != nil {
			return fmt.Errorf("initialize circuit coordination: %w", err)
		}
		replicaManager = coordpostgres.NewReplicaManager(postgresDatabase, replicaID, coordpostgres.ReplicaPolicy{LeaseTTL: cfg.LeaseTTL, LeaseRenewInterval: cfg.LeaseRenewInterval, Circuit: circuitOptions}, cfg.CoordinationTimeout)
	} else {
		admissionCoordinator = coordlocal.NewAdmissionCoordinator(time.Now)
		circuitCoordinator, err = coordlocal.NewCircuitCoordinator(circuitOptions, time.Now)
		if err != nil {
			return fmt.Errorf("initialize local circuit coordination: %w", err)
		}
	}
	var manager *monitor.Manager
	var leaseManager *coordination.LeaseManager
	var recoveryManager *coordination.RecoveryManager
	var reloadConfiguration func(context.Context) error
	if postgresDatabase, ok := database.(*pgstore.Store); ok {
		reloadConfiguration = func(parent context.Context) error {
			reloadCtx, cancel := context.WithTimeout(parent, cfg.ConfigPollInterval)
			defer cancel()
			captured := revisionGuard.Highest()
			revision, err := postgresDatabase.CurrentRevision(reloadCtx)
			if err != nil {
				return err
			}
			if err := revisionGuard.VerifyFresh(captured, revision); err != nil {
				return coordination.PermanentError{Err: err}
			}
			if err := registryValue.Reload(reloadCtx); err != nil {
				return err
			}
			revisionGuard.ObservePublished(registryValue.Snapshot().Revision)
			return manager.Reconcile(backends(registryValue.Snapshot()))
		}
		recoveryManager = coordination.NewRecoveryManager(coordination.RecoverySteps{
			Ping: func(recoveryCtx context.Context) error {
				pingCtx, cancel := context.WithTimeout(recoveryCtx, cfg.CoordinationTimeout)
				defer cancel()
				return postgresDatabase.ConfigPool().Ping(pingCtx)
			},
			CheckFingerprint:    replicaManager.Check,
			ReloadConfiguration: reloadConfiguration,
			ReconcileLeases: func(recoveryCtx context.Context) error {
				if err := leaseManager.Reconcile(recoveryCtx); err != nil {
					return err
				}
				return admissionCoordinator.(coordination.AdmissionRuntime).RefreshInflight(recoveryCtx)
			},
			ReplayFailures: func(recoveryCtx context.Context) error {
				return manager.ReplayCircuitFailures(recoveryCtx)
			},
			RefreshCircuits: func(recoveryCtx context.Context) error {
				return circuitCoordinator.Refresh(recoveryCtx)
			},
		}, time.Now, eventLogger)
		admissionCoordinator.(*coordpostgres.AdmissionCoordinator).SetFailureObserver(recoveryManager)
		circuitCoordinator.(*coordpostgres.CircuitCoordinator).SetFailureObserver(recoveryManager)
		replicaManager.SetFailureObserver(recoveryManager)
		if err := replicaManager.Start(ctx); err != nil {
			return fmt.Errorf("register PostgreSQL coordination replica: %w", err)
		}
		defer replicaManager.Close()
	}
	coordinationObserver := observability.CoordinationMulti(metrics, eventLogger)
	admissionCoordinator = coordination.ObserveAdmission(admissionCoordinator, coordinationObserver)
	circuitCoordinator = coordination.ObserveCircuit(circuitCoordinator, coordinationObserver)
	if postgresDatabase, ok := database.(*pgstore.Store); ok {
		go postgresDatabase.WatchCircuit(ctx, cfg.MetricsInterval, circuitCoordinator.Refresh, pgstore.WatchHooks{ReloadFailure: func(error) {
			metrics.CircuitRefreshFailure()
		}})
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
	manager = monitor.NewManager(ctx, monitor.Options{
		HTTPClient: upstreamClient, HealthInterval: cfg.HealthInterval, HealthTimeout: cfg.HealthTimeout,
		MetricsInterval: cfg.MetricsInterval, MetricsTimeout: cfg.MetricsTimeout, StaleAfter: cfg.MetricsStaleAfter,
		UnhealthyAfter: cfg.UnhealthyAfter, RecoveryAfter: cfg.RecoveryAfter,
		Circuit: circuitOptions, CircuitCoordinator: circuitCoordinator, AdmissionRuntime: admissionCoordinator.(coordination.AdmissionRuntime), Observer: metrics, ReplicaID: replicaID, ProbeTTL: cfg.LeaseTTL, ProbeRenewInterval: cfg.LeaseRenewInterval,
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

	apiKeyUsage := newAPIKeyUsageRecorder(ctx, projectedKeyUsageStore{destination: database, projection: registryValue})
	apiKeyUsageClosed := false
	closeAPIKeyUsage := func() {
		if !apiKeyUsageClosed {
			apiKeyUsageClosed = true
			apiKeyUsage.Close()
		}
	}
	defer closeAPIKeyUsage()
	requestRecorder := analytics.NewRecorder(database, cfg.AnalyticsRetention, metrics.UsagePersistenceFailure, logger)
	requestRecorderClosed := false
	closeRequestRecorder := func(closeCtx context.Context) error {
		if requestRecorderClosed {
			return nil
		}
		requestRecorderClosed = true
		cancelRun()
		manager.Shutdown()
		if replicaManager != nil {
			replicaManager.Close()
		}
		var leaseErr error
		if leaseManager != nil {
			leaseErr = leaseManager.Close()
		}
		closeAPIKeyUsage()
		return errors.Join(leaseErr, closeRecorderStore(closeCtx, requestRecorder, database))
	}
	databaseOwnedByRun = false
	defer func() {
		if requestRecorderClosed {
			return
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
		defer cancel()
		if err := closeRequestRecorder(closeCtx); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("drain usage recorder: %w", err))
		}
	}()
	leaseManager = coordination.NewLeaseManager(ctx, admissionCoordinator, coordination.LeaseManagerOptions{RenewInterval: cfg.LeaseRenewInterval, CompletionBacklog: cfg.CoordinationCompletionBacklog, ShutdownTimeout: cfg.ShutdownGracePeriod, Observer: metrics})
	if postgresDatabase, ok := database.(*pgstore.Store); ok {
		recoveryManager.MarkDegraded(nil)
		if err := recoveryManager.Recover(ctx); err != nil {
			return fmt.Errorf("initialize PostgreSQL recovery barrier: %w", err)
		}
		go postgresDatabase.WatchConfig(ctx, cfg.ConfigPollInterval, func(reloadCtx context.Context) error {
			err := reloadConfiguration(reloadCtx)
			if err == nil {
				return nil
			}
			var permanent coordination.PermanentError
			if errors.As(err, &permanent) {
				recoveryManager.MarkPermanent(err)
			} else {
				recoveryManager.MarkDegraded(err)
			}
			return err
		}, pgstore.WatchHooks{NotificationReconnect: metrics.ConfigNotificationReconnect, ReloadSuccess: metrics.ConfigPollSuccess})
	}
	coordinationAllowed := func() bool {
		return !revisionGuard.Faulted() && !admissionCoordinator.Status().Permanent && !circuitCoordinator.Status().Permanent &&
			(replicaManager == nil || replicaManager.Compatible() && !replicaManager.Status().Permanent) &&
			(recoveryManager == nil || recoveryManager.Status().State != coordination.RecoveryPermanentFault)
	}
	adminMutationsAllowed := func() bool {
		return coordinationReadiness(admissionCoordinator, circuitCoordinator, replicaManager, revisionGuard, recoveryManager) == "ready"
	}
	service := gateway.New(gateway.Dependencies{
		Registry: registryValue, HMACSecret: cfg.APIKeyHMACSecret, Admission: admissionCoordinator, Leases: leaseManager, ReplicaID: replicaID, LeaseTTL: cfg.LeaseTTL,
		Emergency:           coordination.NewEmergencyAdmission(cfg.EmergencyCriticalMaxInflight, cfg.EmergencyHighMaxInflight),
		CoordinationAllowed: coordinationAllowed,
		CoordinationReady:   adminMutationsAllowed,
		Runtime:             manager, Router: routing.NewWithSessionAffinity(
			cfg.RoutingPressureEpsilon,
			cfg.SessionAffinityMaxPressure,
			routing.NewRandomSource(time.Now().UnixNano()),
		),
		Forwarder: proxy.New(upstreamClient), Usage: apiKeyUsage,
		Observer: observability.Multi(metrics, eventLogger, requestRecorder), LookupEnv: getenv,
		RetryAfter: cfg.RetryAfter,
	})
	publicHandler := httpapi.NewPublicHandler(service, cfg.RequestBodyLimit, nil)
	adminService, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Analytics: database, Registry: registryValue, Runtime: manager,
		HMACSecret: cfg.APIKeyHMACSecret, Random: rand.Reader, MutationsAllowed: adminMutationsAllowed,
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
	router.Get("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		view := adminService.View()
		coordinationStatus := coordinationReadiness(admissionCoordinator, circuitCoordinator, replicaManager, revisionGuard, recoveryManager)
		analyticsStatus := analyticsReadiness(request.Context(), database, requestRecorder, cfg.CoordinationTimeout)
		status := "ready"
		configurationStatus := "ready"
		if coordinationStatus != "ready" {
			status = "degraded"
			configurationStatus = coordinationStatus
		}
		if analyticsStatus != "ready" {
			status = "degraded"
		}
		writeJSON(writer, http.StatusOK, map[string]any{
			"status": status, "revision": view.Revision, "backendAvailability": availableBackends(view),
			"components": map[string]string{"configuration": configurationStatus, "coordination": coordinationStatus, "analytics": analyticsStatus},
		})
	})
	router.Get("/coordination-readyz", func(writer http.ResponseWriter, _ *http.Request) {
		status := coordinationReadiness(admissionCoordinator, circuitCoordinator, replicaManager, revisionGuard, recoveryManager)
		code := http.StatusOK
		if status != "ready" {
			code = http.StatusServiceUnavailable
		}
		writeJSON(writer, code, map[string]any{"status": status, "revision": registryValue.Snapshot().Revision})
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
	go updateCoordinationMetrics(ctx, metrics, database, registryValue, revisionGuard, leaseManager, manager, admissionCoordinator, circuitCoordinator, replicaManager, recoveryManager, cfg.MetricsInterval)
	if _, postgresMode := database.(*pgstore.Store); postgresMode {
		go runCoordinationCleanup(ctx, admissionCoordinator.(coordination.CleanupBatcher), circuitCoordinator.(coordination.CleanupBatcher))
	}
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
	var shutdownErr error
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		shutdownErr = fmt.Errorf("graceful HTTP shutdown: %w", err)
	}
	err = <-serveError
	var serveErr error
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		serveErr = fmt.Errorf("serve gateway: %w", err)
	}
	var recorderErr error
	if err := closeRequestRecorder(shutdownCtx); err != nil {
		recorderErr = fmt.Errorf("drain usage recorder: %w", err)
	}
	return errors.Join(shutdownErr, serveErr, recorderErr)
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

func coordinationReadiness(admissionCoordinator coordination.AdmissionCoordinator, circuitCoordinator coordination.CircuitCoordinator, replica *coordpostgres.ReplicaManager, guard *registry.RevisionGuard, recovery *coordination.RecoveryManager) string {
	if guard != nil && guard.Faulted() {
		if recovery != nil {
			recovery.MarkPermanent(errors.New("configuration revision consistency fault"))
		}
		return "consistency_fault"
	}
	if admissionCoordinator.Status().Permanent || circuitCoordinator.Status().Permanent || replica != nil && replica.Status().Permanent {
		if recovery != nil {
			recovery.MarkPermanent(errors.New("PostgreSQL coordination consistency fault"))
		}
		return "consistency_fault"
	}
	if replica != nil && !replica.Compatible() {
		return "incompatible"
	}
	if !admissionCoordinator.Status().Available || !circuitCoordinator.Status().Available || replica != nil && !replica.Status().Available {
		if recovery != nil {
			recovery.MarkDegraded(errors.New("PostgreSQL coordination unavailable"))
		}
		return "degraded"
	}
	if recovery != nil {
		switch recovery.Status().State {
		case coordination.RecoveryPermanentFault:
			return "consistency_fault"
		case coordination.RecoveryDegraded:
			return "degraded"
		}
	}
	return "ready"
}

type analyticsPinger interface {
	PingAnalytics(context.Context) error
}

type analyticsHealth interface {
	Healthy() bool
}

func analyticsReadiness(parent context.Context, destination any, recorder analyticsHealth, timeout time.Duration) string {
	if recorder != nil && !recorder.Healthy() {
		return "degraded"
	}
	pinger, ok := destination.(analyticsPinger)
	if !ok {
		return "ready"
	}
	if timeout <= 0 {
		timeout = 50 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if err := pinger.PingAnalytics(ctx); err != nil {
		return "degraded"
	}
	return "ready"
}

type runtimeMetrics interface {
	PoolSnapshot(int64, time.Time) domain.PoolRuntime
	Snapshot(int64, time.Time) domain.BackendRuntime
}

func publishRuntimeMetrics(metrics *observability.Metrics, snapshot *registry.Snapshot, runtime runtimeMetrics, at time.Time) {
	pools := make([]observability.PoolRuntimeMetric, 0, len(snapshot.PoolsByID))
	for _, pool := range snapshot.PoolsByID {
		pools = append(pools, observability.PoolRuntimeMetric{
			Model: pool.PublicModelName, Runtime: runtime.PoolSnapshot(pool.ID, at),
		})
	}
	backends := make([]observability.BackendRuntimeMetric, 0, len(snapshot.BackendsByID))
	for _, backend := range snapshot.BackendsByID {
		pool := snapshot.PoolsByID[backend.ModelPoolID]
		backends = append(backends, observability.BackendRuntimeMetric{
			Model: pool.PublicModelName, Backend: backend.Name, Runtime: runtime.Snapshot(backend.ID, at),
		})
	}
	inflight := make([]observability.InflightRuntimeLabels, 0)
	for clientID, access := range snapshot.Access {
		client, exists := snapshot.Clients[clientID]
		if !exists || !client.Enabled {
			continue
		}
		for poolID, allowed := range access {
			pool, exists := snapshot.PoolsByID[poolID]
			if !allowed || !exists || !pool.Enabled {
				continue
			}
			inflight = append(inflight, observability.InflightRuntimeLabels{
				Client: client.Name, Model: pool.PublicModelName, PriorityClass: client.PriorityClass,
			})
		}
	}
	metrics.PublishRuntime(pools, backends, inflight)
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

func updateCoordinationMetrics(ctx context.Context, metrics *observability.Metrics, database any, registryValue *registry.Registry, guard *registry.RevisionGuard, leases *coordination.LeaseManager, manager *monitor.Manager, admissionCoordinator coordination.AdmissionCoordinator, circuit coordination.CircuitCoordinator, replica *coordpostgres.ReplicaManager, recovery *coordination.RecoveryManager, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	publish := func(now time.Time) {
		guard.ObservePublished(registryValue.Snapshot().Revision)
		if store, ok := database.(*pgstore.Store); ok && !guard.Faulted() {
			captured := guard.Highest()
			pollCtx, cancel := context.WithTimeout(ctx, interval)
			if revision, err := store.CurrentRevision(pollCtx); err == nil {
				_ = guard.VerifyFresh(captured, revision)
			}
			cancel()
		}
		if recovery != nil {
			if guard.Faulted() {
				recovery.MarkPermanent(errors.New("configuration revision consistency fault"))
			} else if admissionCoordinator.Status().Permanent || circuit.Status().Permanent || replica != nil && replica.Status().Permanent {
				recovery.MarkPermanent(errors.New("PostgreSQL coordination consistency fault"))
			} else if !admissionCoordinator.Status().Available || !circuit.Status().Available || (replica != nil && !replica.Status().Available) {
				recovery.MarkDegraded(errors.New("PostgreSQL coordination unavailable"))
			}
			if recovery.Status().State == coordination.RecoveryDegraded {
				_ = recovery.Recover(ctx)
			}
		}
		compatible := 1
		if replica != nil {
			compatible = replica.ActiveCompatibleReplicas()
		}
		status := circuit.Status()
		cacheAge := time.Duration(0)
		if !status.LastSuccess.IsZero() {
			cacheAge = now.Sub(status.LastSuccess)
		}
		activeDistributed := 0
		if _, postgresMode := database.(*pgstore.Store); postgresMode {
			if runtime, ok := admissionCoordinator.(coordination.AdmissionRuntime); ok {
				for poolID := range registryValue.Snapshot().PoolsByID {
					activeDistributed += runtime.PoolInflight(poolID)
				}
			}
		}
		metrics.SetCoordinationGauges(observability.CoordinationGauges{ActiveLeases: activeDistributed, LocalLeaseHandles: leases.ActiveCount(), PendingCompletions: leases.PendingCompletions(), CircuitReplayBacklog: manager.CircuitReplayBacklog(), CompatibleReplicas: compatible, CircuitCacheAge: cacheAge})
		if store, ok := database.(*pgstore.Store); ok {
			for workload, stats := range store.PoolStats() {
				metrics.SetPostgresPool(workload, stats.Acquired, stats.Idle, stats.Total, stats.Canceled)
			}
		}
	}
	publish(time.Now().UTC())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			publish(now.UTC())
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
