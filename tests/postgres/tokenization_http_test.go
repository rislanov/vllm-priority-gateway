package postgres_test

import (
	"context"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type tokenizationHTTPRuntime struct{ postgresGatewayRuntime }

func (tokenizationHTTPRuntime) Snapshot(id int64, _ time.Time) domain.BackendRuntime {
	return domain.BackendRuntime{BackendID: id, Healthy: true, LastHealthAt: time.Now().UTC().Add(-time.Millisecond)}
}
func TestPostgresTokenizationTwoHTTPGatewaysCancellation(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, s, 0)
	ctx := context.Background()
	secret := []byte(strings.Repeat("h", 32))
	raw := "llmgw_test12" + strings.Repeat("a", 37)
	digest := apikey.Digest(secret, raw)
	entered := make(chan struct{}, 1)
	var blocking atomic.Bool
	blocking.Store(true)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if blocking.Load() {
			entered <- struct{}{}
			<-r.Context().Done()
			return
		}
		io.WriteString(w, `{"count":1,"tokens":[1],"usage":{"prompt_tokens":100,"completion_tokens":100}}`)
	}))
	defer upstream.Close()
	if _, e := s.ConfigPool().Exec(ctx, "UPDATE api_keys SET prefix=$2,secret_hash=$3 WHERE id=$1", f.key.ID, raw[:12], digest[:]); e != nil {
		t.Fatal(e)
	}
	if _, e := s.ConfigPool().Exec(ctx, "UPDATE backends SET base_url=$2 WHERE id=$1", f.backend.ID, upstream.URL); e != nil {
		t.Fatal(e)
	}
	reg := registry.New(s)
	if e := reg.Reload(ctx); e != nil {
		t.Fatal(e)
	}
	p := coordination.DefaultTokenizationPolicy()
	p.ClientMaxInflight = 1
	servers := make([]*httptest.Server, 2)
	for i := range servers {
		service := gateway.New(gateway.Dependencies{Registry: reg, HMACSecret: secret, Runtime: tokenizationHTTPRuntime{}, Tokenization: coordpostgres.NewTokenizationCoordinator(s, p, 2*time.Second), TokenizationPolicy: p, TokenizationForwarder: proxy.New(upstream.Client()), TokenizationHealthMaxAge: time.Minute})
		servers[i] = httptest.NewServer(httpapi.NewPublicHandler(service, 1024, nil))
		defer servers[i].Close()
	}
	call := func(ctx context.Context, i int) (int, error) {
		r, _ := http.NewRequestWithContext(ctx, "POST", servers[i].URL+"/tokenize", strings.NewReader(`{"model":"test-model","prompt":"synthetic"}`))
		r.Header.Set("Authorization", "Bearer "+raw)
		response, e := servers[i].Client().Do(r)
		if e != nil {
			return 0, e
		}
		defer response.Body.Close()
		io.Copy(io.Discard, response.Body)
		return response.StatusCode, nil
	}
	first, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); call(first, 0) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream did not start")
	}
	if status, e := call(ctx, 1); e != nil || status != 429 {
		t.Fatalf("shared HTTP admission: %d %v", status, e)
	}
	cancel()
	<-done
	deadline := time.Now().Add(3 * time.Second)
	for {
		var n int
		if e := s.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM tokenization_leases").Scan(&n); e != nil {
			t.Fatal(e)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancelled lease not released")
		}
		time.Sleep(time.Millisecond)
	}
	blocking.Store(false)
	if status, e := call(ctx, 1); e != nil || status != 200 {
		t.Fatalf("capacity not recovered: %d %v", status, e)
	}
	for _, table := range []string{"request_leases", "coordination_rate_state", "usage_requests"} {
		var n int
		if e := s.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); e != nil || n != 0 {
			t.Fatalf("inference state %s changed: %d %v", table, n, e)
		}
	}
}
