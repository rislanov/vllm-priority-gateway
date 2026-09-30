package integration_test

import (
	"context"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPriorityReserveAdmissionIsolationAcceptance(t *testing.T) {
	for _, reserve := range []int{6, 0} {
		t.Run(map[int]string{6: "reserved", 0: "disabled"}[reserve], func(t *testing.T) {
			h := newHarness(t)
			out := h.adminObject(http.MethodPost, "/admin/api/pools", map[string]any{"publicModelName": "reserved", "upstreamModelName": "fake-model", "enabled": true, "maxGatewayInflight": 20, "highPriorityReserve": reserve}, http.StatusCreated)
			pool := numberID(t, out["id"])
			fake, id := h.addFake(pool, "gpu", fakevllm.State{TokenDelay: time.Minute, Tokens: []string{"first", "last"}})
			h.waitBackend(id, eligible)
			keys := map[domain.PriorityClass]string{}
			for _, class := range []domain.PriorityClass{domain.PriorityNormal, domain.PriorityBackground, domain.PriorityHigh, domain.PriorityCritical} {
				_, key := h.createClient(string(class), class, 0, 100, pool)
				keys[class] = key
			}
			client := &http.Client{Transport: h.client.Transport}
			start := func(class domain.PriorityClass, want int) func() {
				t.Helper()
				ctx, cancel := context.WithCancel(context.Background())
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.server.URL+"/v1/completions", strings.NewReader(postBody("reserved", true)))
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				req.Header.Set("Authorization", "Bearer "+keys[class])
				resp, err := client.Do(req)
				if err != nil {
					cancel()
					t.Fatal(err)
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
						stop()
						t.Fatalf("overload contract: %s", body)
					}
					stop()
				}
				return stop
			}
			stops := []func(){}
			for i := 0; i < 7; i++ {
				stops = append(stops, start(domain.PriorityNormal, 200), start(domain.PriorityBackground, 200))
			}
			if reserve == 6 {
				start(domain.PriorityNormal, 429)
				start(domain.PriorityBackground, 429)
				for i := 0; i < 3; i++ {
					stops = append(stops, start(domain.PriorityHigh, 200), start(domain.PriorityCritical, 200))
				}
			} else {
				for i := 0; i < 6; i++ {
					stops = append(stops, start(domain.PriorityNormal, 200))
				}
			}
			for _, class := range []domain.PriorityClass{domain.PriorityNormal, domain.PriorityHigh, domain.PriorityCritical} {
				start(class, 429)
			}
			if reserve == 6 {
				stops[14]()
				eventually(t, time.Second, func() bool { return h.manager.PoolSnapshot(pool, time.Now()).GatewayInflight == 19 })
				start(domain.PriorityNormal, 429)
				stops = append(stops, start(domain.PriorityHigh, 200))
				stops[0]()
				eventually(t, time.Second, func() bool { return h.manager.PoolSnapshot(pool, time.Now()).GatewayInflight == 19 })
				stops = append(stops, start(domain.PriorityBackground, 200))
				resp, body := h.public(http.MethodGet, "/metrics", "", "")
				if resp.StatusCode != 200 || !strings.Contains(string(body), `reason="pool_priority_reserve"`) {
					t.Fatalf("reserve telemetry missing: %s", body)
				}
			}
			for _, stop := range stops {
				stop()
			}
			eventually(t, 2*time.Second, func() bool {
				return h.manager.PoolSnapshot(pool, time.Now()).GatewayInflight == 0 && fake.Snapshot().ActiveRequests == 0
			})
		})
	}
}
