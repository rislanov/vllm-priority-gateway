package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/observability"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

type reservationHTTPRuntime struct{ postgresGatewayRuntime }

func (r reservationHTTPRuntime) BackendHealth(b domain.Backend, at time.Time) (domain.BackendRuntime, bool) {
	runtime := r.Snapshot(b.ID, at)
	runtime.LastHealthAt = at.Add(-time.Millisecond)
	return runtime, true
}

func TestPostgresTokenReservationTwoHTTPGatewaysCancellation(t *testing.T) {
	db := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	ctx, stop := context.WithTimeout(context.Background(), 12*time.Second)
	defer stop()
	f := tokenReservationFixtures(t, db, 0, 0)
	secret := []byte(strings.Repeat("h", 32))
	raw := "llmgw_test12" + strings.Repeat("a", 37)
	digest := apikey.Digest(secret, raw)
	if _, err := db.ConfigPool().Exec(ctx, "UPDATE api_keys SET prefix=$2::text,secret_hash=$3::bytea WHERE id=$1::bigint", f[0].key.ID, raw[:12], digest[:]); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	var generationCalls atomic.Int32
	const promptSentinel = "sentinel-private-request-text-8900"
	const generatedSentinel = "sentinel-private-generated-text-6710"
	const tokenizerSentinel = "sentinel-private-tokenizer-text-3601"
	var block, failTokenize atomic.Bool
	block.Store(true)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			http.Error(w, "invalid request", 400)
			return
		}
		if r.URL.Path == "/tokenize" {
			if failTokenize.Load() {
				http.Error(w, tokenizerSentinel, http.StatusInternalServerError)
				return
			}
			io.WriteString(w, `{"count":100,"max_model_len":4096,"token_strs":["`+tokenizerSentinel+`"]}`)
			return
		}
		generationCalls.Add(1)
		if payload["model"] != "upstream" || payload["max_completion_tokens"] != float64(500) {
			t.Errorf("wrong forwarded payload: %#v", payload)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\""+generatedSentinel+"\"}}]}\n\n")
		w.(http.Flusher).Flush()
		if block.Load() {
			entered <- struct{}{}
			<-r.Context().Done()
			return
		}
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()
	if _, err := db.CreateBackend(ctx, store.CreateBackendParams{
		ModelPoolID: f[0].pool.ID, Name: "gpu", Enabled: true, BaseURL: upstream.URL, CapacityHint: 1, RunningSoftLimit: 1,
	}); err != nil {
		t.Fatal(err)
	}
	var gateways [2]*httptest.Server
	var coordinators [2]*coordpostgres.AdmissionCoordinator
	var logs bytes.Buffer
	metrics := observability.NewMetrics()
	logger := observability.NewLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	policy := coordination.DefaultTokenizationPolicy()
	for i := range gateways {
		reg := registry.New(db)
		if err := reg.Reload(ctx); err != nil {
			t.Fatal(err)
		}
		coordinator := coordpostgres.NewAdmissionCoordinator(db, 2*time.Second)
		coordinators[i] = coordinator
		coordinator.SetTokenReservationObserver(metrics)
		leaseManager := coordination.NewLeaseManager(ctx, coordinator, coordination.LeaseManagerOptions{RenewInterval: 20 * time.Millisecond})
		t.Cleanup(func() { _ = leaseManager.Close() })
		transport := proxy.New(upstream.Client())
		service := gateway.New(gateway.Dependencies{
			Registry: reg, Observer: observability.Multi(metrics, logger), HMACSecret: secret, Runtime: reservationHTTPRuntime{}, Forwarder: transport,
			Router: routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0)), Admission: coordinator,
			Leases: leaseManager, LeaseTTL: 300 * time.Millisecond, ReplicaID: uuid.New(),
			Tokenization: coordpostgres.NewTokenizationCoordinator(db, policy, 2*time.Second), TokenizationPolicy: policy,
			TokenizationForwarder: transport, TokenizationHealthMaxAge: time.Minute,
		})
		gateways[i] = httptest.NewServer(httpapi.NewPublicHandler(service, 1<<20, nil))
		defer gateways[i].Close()
	}
	call := func(ctx context.Context, replica int) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, gateways[replica].URL+"/v1/chat/completions", strings.NewReader(
			`{"model":"token-model","messages":[{"role":"user","content":"`+promptSentinel+`"}],"max_completion_tokens":500,"stream":true}`))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+raw)
		req.Header.Set("Content-Type", "application/json")
		return gateways[replica].Client().Do(req)
	}
	firstCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	first, err := call(firstCtx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	if first.StatusCode != 200 {
		body, _ := io.ReadAll(first.Body)
		t.Fatalf("first stream response = %d %s", first.StatusCode, body)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("stream did not start")
	}
	var initial time.Time
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT expires_at FROM request_leases WHERE reserved_tokens=600").Scan(&initial); err != nil {
		t.Fatal(err)
	}
	for time.Now().Before(initial) {
		select {
		case <-ctx.Done():
			t.Fatal("waiting past the first expiry:", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	second, err := call(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(second.Body)
	second.Body.Close()
	if second.StatusCode != 429 || !strings.Contains(string(body), "token_budget_exhausted") || second.Header.Get("Retry-After") == "" {
		t.Fatalf("renewed stream did not retain reservation across replicas: %d %s", second.StatusCode, body)
	}
	if generationCalls.Load() != 1 {
		t.Fatalf("denied request reached generation: %d calls", generationCalls.Load())
	}
	for i, coordinator := range coordinators {
		if err := coordinator.RefreshInflight(ctx); err != nil {
			t.Fatal(err)
		}
		if got := coordinator.OutstandingTokenSnapshot()[f[0].pool.ID]; got != 600 {
			t.Fatalf("replica %d shared outstanding = %d, want 600", i, got)
		}
	}
	cancel()
	first.Body.Close()
	for {
		var outstanding int
		if err := db.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM request_leases").Scan(&outstanding); err != nil {
			t.Fatal(err)
		}
		if outstanding == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("cancelled stream did not release its reservation")
		case <-time.After(10 * time.Millisecond):
		}
	}
	block.Store(false)
	third, err := call(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Body.Close()
	if third.StatusCode != 200 {
		t.Fatalf("capacity after cancellation returned %d", third.StatusCode)
	}
	generation, err := io.ReadAll(third.Body)
	if err != nil || !strings.Contains(string(generation), generatedSentinel) {
		t.Fatalf("synthetic generated content was not delivered: %v", err)
	}

	t.Run("TestTokenReservationPrivacy", func(t *testing.T) {
		failTokenize.Store(true)
		failed, err := call(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		errorBody, err := io.ReadAll(failed.Body)
		failed.Body.Close()
		if err != nil || failed.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(errorBody), "token_estimation_unavailable") {
			t.Fatalf("tokenizer failure = %d, %s, %v", failed.StatusCode, errorBody, err)
		}
		for _, server := range gateways {
			server.Close()
		}
		var receipts string
		var actualCount int
		if err := db.CoordinationPool().QueryRow(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(o))::text,''),
			count(token_actual_input)+count(token_actual_output) FROM admission_operations o`).Scan(&receipts, &actualCount); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(receipts, "ReservedTokens") || actualCount != 0 {
			t.Fatalf("receipt metadata missing or unknown usage persisted as an amount: actual columns=%d", actualCount)
		}
		scrape := httptest.NewRecorder()
		metrics.Handler().ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		for name, value := range map[string]string{
			"receipts": receipts, "logs": logs.String(), "metrics": scrape.Body.String(),
			"budget API error": string(body), "tokenizer API error": string(errorBody),
		} {
			for _, sentinel := range []string{promptSentinel, generatedSentinel, tokenizerSentinel} {
				if strings.Contains(value, sentinel) {
					t.Errorf("private content leaked to %s", name)
				}
			}
		}
		if !strings.Contains(logs.String(), `"usageKnown":false`) || strings.Contains(logs.String(), `"actualInputTokens"`) {
			t.Error("structured logs must retain unknown usage without an actual amount")
		}
		if !strings.Contains(scrape.Body.String(), `llmgw_admission_reservation_events_total{action="reconciled",mode="normal",priority_class="normal",reason="none",usage="unknown"}`) {
			t.Error("missing PostgreSQL reconciliation event")
		}
	})
}
