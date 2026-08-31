package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/monitor"
	"github.com/rislanov/vllm-priority-gateway/internal/observability"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
)

func serveGateway(
	ctx context.Context,
	listener net.Listener,
	handler http.Handler,
	shutdown *shutdownBudget,
	stdout io.Writer,
) error {
	server := &http.Server{
		Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	return serveGatewayWithServer(ctx, listener, server, shutdown, stdout)
}

type gatewayHTTPServer interface {
	Serve(net.Listener) error
	Shutdown(context.Context) error
	Close() error
}

func serveGatewayWithServer(
	ctx context.Context,
	listener net.Listener,
	server gatewayHTTPServer,
	shutdown *shutdownBudget,
	stdout io.Writer,
) error {
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
	var shutdownErr error
	if err := server.Shutdown(shutdown.Context()); err != nil {
		_ = server.Close()
		shutdownErr = fmt.Errorf("graceful HTTP shutdown: %w", err)
	}
	err := <-serveError
	var serveErr error
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		serveErr = fmt.Errorf("serve gateway: %w", err)
	}
	return errors.Join(shutdownErr, serveErr)
}

type shutdownBudget struct {
	grace      time.Duration
	once       sync.Once
	newContext func(time.Duration) (context.Context, context.CancelFunc)
	ctx        context.Context
	cancel     context.CancelFunc
}

func newShutdownBudget(grace time.Duration) *shutdownBudget {
	return &shutdownBudget{grace: grace}
}

func (b *shutdownBudget) Context() context.Context {
	b.once.Do(func() {
		newContext := b.newContext
		if newContext == nil {
			newContext = func(grace time.Duration) (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), grace)
			}
		}
		b.ctx, b.cancel = newContext(b.grace)
	})
	return b.ctx
}

func (b *shutdownBudget) Cancel() {
	_ = b.Context()
	b.cancel()
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
