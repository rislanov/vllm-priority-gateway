package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

// This mode needs an isolated pool and a pressure entry window longer than
// the admission burst, so pressure shedding cannot mask the reserve boundary.
func TestPriorityCapacityReserveWithRealVLLM(t *testing.T) {
	cfg := loadE2EConfig(t, modeReserve)
	h := newRemoteHarness(t, cfg)
	harnesses := []*remoteHarness{h}
	if cfg.peerURL != nil {
		peer := cfg
		peer.baseURL = cfg.peerURL
		harnesses = append(harnesses, newRemoteHarness(t, peer))
	}
	original := h.adminStatus().requirePool(t, cfg.model)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.probeTimeout)
		defer cancel()
		if _, err := h.updatePool(ctx, original); err != nil {
			t.Errorf("restore reserve pool: %v", err)
		}
	})
	for _, reserve := range []int{6, 0, 20} {
		t.Run(fmt.Sprintf("reserve_%d", reserve), func(t *testing.T) {
			pool := original
			pool.MaxGatewayInflight, pool.HighPriorityReserve, pool.MaxWaiting = 20, reserve, 0
			if _, err := h.updatePool(context.Background(), pool); err != nil {
				t.Fatal(err)
			}
			for _, replica := range harnesses {
				replica.waitForPool(func(got adminPool) bool { return samePoolConfiguration(pool, got) && got.Runtime.GatewayInflight == 0 }, cfg.probeTimeout)
			}
			var streams []*reserveStream
			stopAll := func() {
				for _, stream := range streams {
					stream.cancel()
				}
				for _, stream := range streams {
					select {
					case <-stream.done:
					case <-time.After(cfg.probeTimeout):
						t.Error("cancelled real stream did not exit")
					}
				}
			}
			defer stopAll()
			start := func(key string) *reserveStream {
				replica := harnesses[len(streams)%len(harnesses)]
				stream := startReserveStream(replica, key)
				streams = append(streams, stream)
				return stream
			}
			waitCount := func(want int) {
				deadline := time.Now().Add(10 * time.Second)
				var last []int
				for time.Now().Before(deadline) {
					last = nil
					matched := true
					for _, replica := range harnesses {
						got := replica.adminStatus().requirePool(t, cfg.model)
						last = append(last, got.Runtime.GatewayInflight)
						if got.Runtime.State != "normal" {
							t.Fatalf("pressure shedding masks reserve: state=%s", got.Runtime.State)
						}
						if got.Runtime.GatewayInflight != want {
							matched = false
						}
					}
					if matched {
						t.Logf("replicas=%d R=%d observed_global_inflight=%v", len(harnesses), reserve, last)
						return
					}
					time.Sleep(50 * time.Millisecond)
				}
				t.Fatalf("global inflight=%v, want %d", last, want)
			}
			reject := func(replica *remoteHarness, key, class, reason string) {
				labels := map[string]string{"model": cfg.model, "priority_class": class, "reason": reason}
				before, _ := replica.metrics().value("llmgw_requests_rejected_total", labels)
				spoof := -1000
				result := replica.completion(context.Background(), completionRequest{Key: key, Prompt: "Reserve boundary probe.", MaxTokens: 4, Stream: true, Priority: &spoof, ExtraHeaders: map[string]string{"X-Priority-Class": "critical"}})
				result.requireOverloaded(t)
				after, _ := replica.metrics().value("llmgw_requests_rejected_total", labels)
				if after < before+1 {
					t.Fatalf("rejection reason %s/%s did not increase: %v -> %v", class, reason, before, after)
				}
			}
			lower := 20 - reserve
			for i := 0; i < lower; i++ {
				key := cfg.normalKey
				if i%2 == 1 {
					key = cfg.lowKeys[0]
				}
				start(key)
			}
			waitCount(lower)
			if reserve > 0 {
				for _, replica := range harnesses {
					reject(replica, cfg.normalKey, "normal", "pool_priority_reserve")
					reject(replica, cfg.lowKeys[0], "background", "pool_priority_reserve")
				}
			}
			var high []*reserveStream
			for i := 0; i < reserve; i++ {
				key := cfg.highKey
				if i%2 == 1 {
					key = cfg.criticalKey
				}
				high = append(high, start(key))
			}
			waitCount(20)
			for _, replica := range harnesses {
				reject(replica, cfg.criticalKey, "critical", "pool_inflight_limit")
			}
			if reserve == 6 {
				high[0].cancel()
				waitCount(19)
				reject(harnesses[len(harnesses)-1], cfg.normalKey, "normal", "pool_priority_reserve")
				high[0] = start(cfg.highKey)
				waitCount(20)
				streams[0].cancel()
				waitCount(19)
				start(cfg.normalKey)
				waitCount(20)
				reject(harnesses[len(harnesses)-1], cfg.lowKeys[0], "background", "pool_inflight_limit")
				// Let one lease of each protected class finish on the GPU after
				// the boundary assertions; admission alone is not HTTP success.
				for _, stream := range streams {
					if stream != high[0] && stream != high[1] {
						stream.cancel()
					}
				}
				for _, stream := range []*reserveStream{high[0], high[1]} {
					select {
					case complete := <-stream.complete:
						if !complete {
							t.Fatal("protected real stream did not finish with HTTP 200 and [DONE]")
						}
					case <-time.After(cfg.probeTimeout):
						t.Fatal("protected real stream did not finish before deadline")
					}
				}
				t.Log("reserved High and Critical leases completed 900-token real GPU streams with HTTP 200 and [DONE]")
			}
			if reserve == 0 {
				// Turn the reserve on while twenty lower leases already exist.
				// Existing work continues, but new lower admissions must wait.
				pool.HighPriorityReserve = 6
				if _, err := h.updatePool(context.Background(), pool); err != nil {
					t.Fatal(err)
				}
				for _, replica := range harnesses {
					replica.waitForPool(func(got adminPool) bool { return samePoolConfiguration(got, pool) }, cfg.probeTimeout)
				}
				for i := 0; i < 6; i++ {
					streams[i].cancel()
				}
				waitCount(14)
				reject(harnesses[len(harnesses)-1], cfg.normalKey, "normal", "pool_priority_reserve")
				for i := 0; i < 6; i++ {
					key := cfg.highKey
					if i%2 == 1 {
						key = cfg.criticalKey
					}
					start(key)
				}
				waitCount(20)
				t.Log("live reserve activation: preexisting lower leases counted, high/critical filled six released slots")
			}
			stopAll()
			waitCount(0)
			for _, key := range []string{cfg.highKey, cfg.criticalKey} {
				h.completion(context.Background(), completionRequest{Key: key, Prompt: "Real reserve continuity.", MaxTokens: 4, Stream: true}).requireCompleteStream(t)
			}
			t.Logf("reserve=%d lower=%d high_critical=%d total=20 cancellation_and_continuity=PASS", reserve, lower, reserve)
		})
	}
}

type reserveStream struct {
	cancel   context.CancelFunc
	done     chan struct{}
	complete chan bool
}

func startReserveStream(h *remoteHarness, key string) *reserveStream {
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.probeTimeout)
	stream := &reserveStream{cancel: cancel, done: make(chan struct{}), complete: make(chan bool, 1)}
	body, _ := json.Marshal(map[string]any{"model": h.cfg.model, "prompt": "Repeat token repeatedly.", "max_tokens": 900, "ignore_eos": true, "stream": true, "temperature": 0})
	go func() {
		complete := false
		defer func() { stream.complete <- complete }()
		defer close(stream.done)
		defer cancel()
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint("/v1/completions"), bytes.NewReader(body))
		if err != nil {
			h.t.Errorf("create held real stream: %v", err)
			return
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		response, err := h.client.Do(request)
		if err != nil {
			if ctx.Err() == nil {
				h.t.Errorf("held real stream failed: %v", err)
			}
			return
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			h.t.Errorf("held real stream status=%d", response.StatusCode)
			return
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20))
		err = readErr
		complete = err == nil && bytes.Contains(responseBody, []byte("data: [DONE]"))
		if err != nil && ctx.Err() == nil {
			h.t.Errorf("read held real stream: %v", err)
		}
	}()
	return stream
}
