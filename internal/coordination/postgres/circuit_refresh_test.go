package postgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

type refreshSnapshotTrace struct {
	once    sync.Once
	read    chan struct{}
	release chan struct{}
}
type refreshSnapshotKey struct{}

func (tr *refreshSnapshotTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, refreshSnapshotKey{}, strings.Contains(data.SQL, "SELECT s.backend_id,s.backend_revision,s.state,s.generation,s.opened_at,s.half_open_succeeded"))
}
func (tr *refreshSnapshotTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if matched, _ := ctx.Value(refreshSnapshotKey{}).(bool); !matched {
		return
	}
	// Block only the first snapshot after every row has been consumed. Other
	// queries remain free to read and publish a newer committed generation.
	block := false
	tr.once.Do(func() { block = true })
	if block {
		close(tr.read)
		<-tr.release
	}
}

func TestRefreshCannotPublishOlderConcurrentSnapshot(t *testing.T) {
	raw := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if raw == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	store, err := pgstore.Open(ctx, pgstore.Options{DatabaseURL: raw, ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	const backendID = 900001
	_, err = store.ConfigPool().Exec(ctx, `INSERT INTO backend_circuit_state(backend_id,backend_revision,state,generation,half_open_succeeded,updated_at) VALUES($1,1,'closed',1,false,clock_timestamp()) ON CONFLICT(backend_id) DO UPDATE SET state='closed',generation=1,opened_at=NULL`, backendID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.ConfigPool().Exec(ctx, "DELETE FROM backend_circuit_state WHERE backend_id=$1", backendID)
	})
	trace := &refreshSnapshotTrace{read: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(trace.release) }) }
	defer unblock()
	cfg, err := pgstore.ParsePoolConfig(raw, 4)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = trace
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	c, err := NewCircuitCoordinator(store, 2*time.Second, circuitbreaker.Options{FailureThreshold: 3, FailureWindow: time.Minute, OpenCooldown: time.Hour, HalfOpenMaxProbes: 1})
	if err != nil {
		t.Fatal(err)
	}
	c.pool = pool
	first := make(chan error, 1)
	go func() { first <- c.Refresh(ctx) }()
	select {
	case <-trace.read:
	case <-time.After(5 * time.Second):
		t.Fatal("first snapshot did not arrive")
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, 30*time.Millisecond)
	waitErr := c.Refresh(waitCtx)
	cancelWait()
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatalf("waiting refresh ignored deadline: %v", waitErr)
	}
	_, err = store.ConfigPool().Exec(ctx, "UPDATE backend_circuit_state SET state='open',generation=2,opened_at=clock_timestamp() WHERE backend_id=$1", backendID)
	if err != nil {
		t.Fatal(err)
	}
	second := make(chan error, 1)
	go func() { second <- c.Refresh(ctx) }()
	var secondErr error
	secondFinished := false
	select {
	case secondErr = <-second:
		secondFinished = true
	case <-time.After(time.Second):
	}
	unblock()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if !secondFinished {
		secondErr = <-second
	}
	if secondErr != nil {
		t.Fatal(secondErr)
	}
	got := c.Snapshot(backendID, time.Now())
	if got.Generation != 2 || got.State != domain.CircuitOpen {
		t.Fatalf("newer open snapshot overwritten: %+v", got)
	}
}
