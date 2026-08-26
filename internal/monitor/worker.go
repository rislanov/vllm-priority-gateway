package monitor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/pressure"
)

type Options struct {
	HTTPClient         *http.Client
	HealthInterval     time.Duration
	HealthTimeout      time.Duration
	MetricsInterval    time.Duration
	MetricsTimeout     time.Duration
	StaleAfter         time.Duration
	UnhealthyAfter     int
	RecoveryAfter      int
	Limits             pressure.Limits
	EWMAWindow         time.Duration
	BusyThreshold      float64
	SaturatedThreshold float64
	PoolThresholds     pressure.Thresholds
}

func (o Options) validate() error {
	if o.HealthInterval <= 0 || o.HealthTimeout <= 0 || o.MetricsInterval <= 0 || o.MetricsTimeout <= 0 || o.StaleAfter <= 0 || o.EWMAWindow <= 0 {
		return errors.New("monitor durations must be positive")
	}
	if o.UnhealthyAfter <= 0 || o.RecoveryAfter <= 0 {
		return errors.New("monitor transition counts must be positive")
	}
	if err := o.Limits.Validate(); err != nil {
		return err
	}
	if o.BusyThreshold < 0 || o.SaturatedThreshold <= o.BusyThreshold {
		return errors.New("backend pressure thresholds are out of order")
	}
	return nil
}

type Worker struct {
	backend domain.Backend
	options Options
	ewma    *pressure.EWMA

	mu      sync.Mutex
	runtime domain.BackendRuntime
}

func NewWorker(backend domain.Backend, options Options) (*Worker, error) {
	if err := backend.Validate(); err != nil {
		return nil, err
	}
	options.Limits.RunningSoft = backend.RunningSoftLimit
	if err := options.validate(); err != nil {
		return nil, err
	}
	if options.HTTPClient == nil {
		options.HTTPClient = http.DefaultClient
	}
	client := *options.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	options.HTTPClient = &client
	return &Worker{
		backend: backend,
		options: options,
		ewma:    pressure.NewEWMA(options.EWMAWindow),
		runtime: domain.BackendRuntime{BackendID: backend.ID, State: domain.BackendUnhealthy},
	}, nil
}

func (w *Worker) Backend() domain.Backend {
	return w.backend
}

func (w *Worker) Run(ctx context.Context) {
	w.PollHealth(ctx, time.Now())
	_ = w.PollMetrics(ctx, time.Now())
	healthTicker := time.NewTicker(w.options.HealthInterval)
	metricsTicker := time.NewTicker(w.options.MetricsInterval)
	defer healthTicker.Stop()
	defer metricsTicker.Stop()
	for {
		select {
		case at := <-healthTicker.C:
			w.PollHealth(ctx, at)
		case at := <-metricsTicker.C:
			_ = w.PollMetrics(ctx, at)
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) PollHealth(ctx context.Context, at time.Time) bool {
	requestCtx, cancel := context.WithTimeout(ctx, w.options.HealthTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint(w.backend.BaseURL, "/health"), nil)
	success := false
	if err == nil {
		response, requestErr := w.options.HTTPClient.Do(request)
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			_ = response.Body.Close()
			success = response.StatusCode >= 200 && response.StatusCode < 300
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.runtime.LastHealthAt = at
	if success {
		w.runtime.ConsecutiveSuccess++
		w.runtime.ConsecutiveFailure = 0
		if w.runtime.ConsecutiveSuccess >= w.options.RecoveryAfter {
			w.runtime.Healthy = true
		}
	} else {
		w.runtime.ConsecutiveFailure++
		w.runtime.ConsecutiveSuccess = 0
		if w.runtime.ConsecutiveFailure >= w.options.UnhealthyAfter {
			w.runtime.Healthy = false
		}
	}
	return success
}

func (w *Worker) PollMetrics(ctx context.Context, at time.Time) error {
	requestCtx, cancel := context.WithTimeout(ctx, w.options.MetricsTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint(w.backend.BaseURL, "/metrics"), nil)
	if err != nil {
		return fmt.Errorf("create metrics request: %w", err)
	}
	response, err := w.options.HTTPClient.Do(request)
	if err != nil {
		return fmt.Errorf("scrape metrics: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return fmt.Errorf("scrape metrics: HTTP %d", response.StatusCode)
	}
	metrics, err := ParseMetrics(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return err
	}
	raw := pressure.Calculate(pressure.Sample{
		Running: metrics.Running, Waiting: metrics.Waiting, KVUsage: metrics.KVCacheUsage,
	}, w.options.Limits)
	w.mu.Lock()
	w.runtime.Running = metrics.Running
	w.runtime.Waiting = metrics.Waiting
	w.runtime.KVCacheUsage = metrics.KVCacheUsage
	w.runtime.RawPressure = raw
	w.runtime.Pressure = w.ewma.Add(at, raw)
	w.runtime.LastMetricsAt = at
	w.mu.Unlock()
	return nil
}

func (w *Worker) Snapshot(at time.Time) domain.BackendRuntime {
	w.mu.Lock()
	snapshot := w.runtime
	w.mu.Unlock()
	age := at.Sub(snapshot.LastMetricsAt)
	snapshot.MetricsFresh = !snapshot.LastMetricsAt.IsZero() && age >= 0 && age <= w.options.StaleAfter
	snapshot.State = w.state(snapshot)
	return snapshot
}

func (w *Worker) incrementInflight(delta int) {
	w.mu.Lock()
	w.runtime.GatewayInflight += delta
	if w.runtime.GatewayInflight < 0 {
		w.runtime.GatewayInflight = 0
	}
	w.mu.Unlock()
}

func (w *Worker) state(snapshot domain.BackendRuntime) domain.BackendState {
	if !snapshot.Healthy || !snapshot.MetricsFresh {
		return domain.BackendUnhealthy
	}
	if w.backend.Draining {
		return domain.BackendDraining
	}
	if snapshot.Pressure >= w.options.SaturatedThreshold {
		return domain.BackendSaturated
	}
	if snapshot.Pressure >= w.options.BusyThreshold {
		return domain.BackendBusy
	}
	return domain.BackendHealthy
}

func endpoint(baseURL, path string) string {
	return strings.TrimRight(baseURL, "/") + path
}
