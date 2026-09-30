package postgres_test

import (
	"context"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	basestore "github.com/rislanov/vllm-priority-gateway/internal/store"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPostgresPriorityReserveHTTPAdmissionIsolation(t *testing.T) {
	for _, reserve := range []int{6, 0} {
		t.Run(map[int]string{6: "reserved", 0: "disabled"}[reserve], func(t *testing.T) {
			dsn := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
			s := openTestStore(t, dsn)
			ctx := context.Background()
			fs := priorityReserveFixtures(t, s, 20, reserve)
			fake := fakevllm.New()
			fake.SetState(fakevllm.State{Tokens: []string{"first", "last"}, TokenDelay: time.Minute})
			up := httptest.NewServer(fake.Handler())
			t.Cleanup(up.Close)
			f := fs[domain.PriorityHigh]
			if _, err := s.UpdateBackend(ctx, f.backend.ID, basestore.UpdateBackendParams{ModelPoolID: f.pool.ID, Name: "gpu", BaseURL: up.URL, Enabled: true, CapacityHint: 1, RunningSoftLimit: 128}); err != nil {
				t.Fatal(err)
			}
			keys := map[domain.PriorityClass]string{}
			for i, class := range []domain.PriorityClass{domain.PriorityNormal, domain.PriorityBackground, domain.PriorityHigh, domain.PriorityCritical} {
				key := "llmgw_test12" + strings.Repeat(string(rune('a'+i)), 35)
				keys[class] = key
				digest := apikey.Digest(postgresHTTPSecret, key)
				if _, err := s.ConfigPool().Exec(ctx, "UPDATE api_keys SET secret_hash=$2,prefix=$3 WHERE id=$1", fs[class].key.ID, digest[:], key[:12]); err != nil {
					t.Fatal(err)
				}
			}
			other, err := pgstore.Open(ctx, pgstore.Options{DatabaseURL: dsn, MigrationURL: dsn, ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 8})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { other.Close() })
			options := circuitbreaker.Options{FailureThreshold: 5, FailureWindow: time.Minute, OpenCooldown: time.Second, HalfOpenMaxProbes: 1}
			gateways := []*postgresHTTPGateway{newPostgresHTTPGateway(t, ctx, s, options, nil, nil, 0, postgresHTTPGatewayOptions{CoordinationTimeout: 2 * time.Second}), newPostgresHTTPGateway(t, ctx, other, options, nil, nil, 0, postgresHTTPGatewayOptions{CoordinationTimeout: 2 * time.Second})}
			for _, g := range gateways {
				waitPostgresGatewayBackend(t, g, f.backend.ID)
			}
			start := func(index int, class domain.PriorityClass, want int) func() {
				t.Helper()
				g := gateways[index%2]
				streamCtx, cancel := context.WithCancel(ctx)
				req, _ := http.NewRequestWithContext(streamCtx, http.MethodPost, g.server.URL+"/v1/completions", strings.NewReader(`{"model":"test-model","stream":true,"priority":-100}`))
				req.Header.Set("Authorization", "Bearer "+keys[class])
				req.Header.Set("X-Priority-Class", "critical")
				resp, e := g.client.Do(req)
				if e != nil {
					cancel()
					t.Fatal(e)
				}
				stop := func() { cancel(); resp.Body.Close() }
				t.Cleanup(stop)
				if resp.StatusCode != want {
					body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
					stop()
					t.Fatalf("%s status=%d want=%d body=%s", class, resp.StatusCode, want, body)
				}
				if want == 200 {
					buf := make([]byte, 1024)
					if n, e := resp.Body.Read(buf); e != nil || n == 0 {
						stop()
						t.Fatal(n, e)
					}
				} else {
					body, _ := io.ReadAll(resp.Body)
					if !strings.Contains(string(body), "gateway_overloaded") || resp.Header.Get("Retry-After") == "" {
						t.Fatal(string(body))
					}
					stop()
				}
				return stop
			}
			active := func() int {
				t.Helper()
				var count int
				if e := s.ConfigPool().QueryRow(ctx, "SELECT count(*) FROM request_leases WHERE expires_at>clock_timestamp()").Scan(&count); e != nil {
					t.Fatal(e)
				}
				return count
			}
			stops := []func(){}
			for i := 0; i < 7; i++ {
				stops = append(stops, start(i, domain.PriorityNormal, 200), start(i+1, domain.PriorityBackground, 200))
			}
			if reserve > 0 {
				start(0, domain.PriorityNormal, 429)
				start(1, domain.PriorityBackground, 429)
				for i := 0; i < 3; i++ {
					stops = append(stops, start(i, domain.PriorityHigh, 200), start(i+1, domain.PriorityCritical, 200))
				}
			} else {
				for i := 0; i < 6; i++ {
					stops = append(stops, start(i, domain.PriorityNormal, 200))
				}
			}
			if active() != 20 {
				t.Fatalf("active=%d want 20", active())
			}
			start(1, domain.PriorityCritical, 429)
			if reserve > 0 {
				stops[14]()
				waitUntil(t, time.Second, func() bool { return active() == 19 })
				start(1, domain.PriorityNormal, 429)
				stops = append(stops, start(0, domain.PriorityHigh, 200))
				stops[0]()
				waitUntil(t, time.Second, func() bool { return active() == 19 })
				stops = append(stops, start(1, domain.PriorityBackground, 200))
			}
			for _, stop := range stops {
				stop()
			}
			waitUntil(t, 2*time.Second, func() bool { return active() == 0 && fake.Snapshot().ActiveRequests == 0 })
		})
	}
}

func TestPostgresPriorityReserveCrashExpiry(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fs := priorityReserveFixtures(t, s, 2, 1)
	ctx := context.Background()
	crashed := coordpostgres.NewAdmissionCoordinator(s, time.Second)
	survivor := coordpostgres.NewAdmissionCoordinator(s, time.Second)
	r := priorityRequest(fs[domain.PriorityNormal])
	r.LeaseTTL = 150 * time.Millisecond
	d, e := crashed.Acquire(ctx, r)
	if e != nil || !d.Admitted() {
		t.Fatal(d, e)
	}
	blocked, e := survivor.Acquire(ctx, priorityRequest(fs[domain.PriorityBackground]))
	if e != nil || blocked.Reason != coordination.ReasonPoolPriorityReserve {
		t.Fatal(blocked, e)
	}
	// Simulate a dead replica: no Complete and no renewal. Physical cleanup is absent.
	waitUntil(t, time.Second, func() bool {
		var n int
		err := s.ConfigPool().QueryRow(ctx, "SELECT count(*) FROM request_leases WHERE expires_at>clock_timestamp()").Scan(&n)
		return err == nil && n == 0
	})
	next, e := survivor.Acquire(ctx, priorityRequest(fs[domain.PriorityBackground]))
	if e != nil || !next.Admitted() {
		t.Fatal(next, e)
	}
	replay, e := survivor.Acquire(ctx, r)
	if e != nil || replay.Reason != coordination.ReasonLeaseLost {
		t.Fatal(replay, e)
	}
}
