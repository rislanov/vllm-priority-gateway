package loadgen_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/loadgen"
)

func TestTrafficMixRequiresAKeyForEveryNonZeroClass(t *testing.T) {
	config := loadgen.Config{
		URL: "http://gateway.invalid", Requests: 1, Parallelism: 1,
		Keys: map[domain.PriorityClass]string{domain.PriorityHigh: "llmgw_high"},
		Mix:  map[domain.PriorityClass]int{domain.PriorityHigh: 1, domain.PriorityBackground: 1},
	}
	if err := config.Validate(); err == nil {
		t.Fatal("accepted a non-zero traffic class without a mapped key")
	}
}

func TestRunMeasuresStreamingTTFTAndCountsOverloadSeparately(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if requests == 2 {
			writer.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(writer, `{"error":{"code":"gateway_overloaded"}}`)
			return
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.WriteHeader(http.StatusOK)
		fmt.Fprint(writer, "data: first\n\n")
		writer.(http.Flusher).Flush()
		time.Sleep(40 * time.Millisecond)
		fmt.Fprint(writer, "data: [DONE]\n\n")
	}))
	defer server.Close()

	result, err := loadgen.Run(context.Background(), loadgen.Config{
		URL: server.URL, Key: "llmgw_test", Model: "qwen", Requests: 2, Parallelism: 1,
		PromptSize: 8, MaxTokens: 2, Stream: true, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 2 || result.Successes != 1 || result.Overloaded != 1 || result.Failures != 0 {
		t.Fatalf("result = %+v", result)
	}
	if result.TTFT.Count != 1 || result.Latency.Count != 1 || result.TTFT.P95 <= 0 || result.Latency.P95 < 35*time.Millisecond || result.TTFT.P95 >= result.Latency.P95 {
		t.Fatalf("TTFT=%v latency=%v", result.TTFT, result.Latency)
	}
}

func TestSmallTrafficMixDoesNotAlwaysFavorLeadingClasses(t *testing.T) {
	var mu sync.Mutex
	seen := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		seen[request.Header.Get("Authorization")]++
		mu.Unlock()
		writer.WriteHeader(http.StatusOK)
		fmt.Fprint(writer, `{}`)
	}))
	defer server.Close()
	keys := map[domain.PriorityClass]string{
		domain.PriorityCritical: "critical", domain.PriorityHigh: "high",
		domain.PriorityNormal: "normal", domain.PriorityBackground: "background",
	}
	mix := map[domain.PriorityClass]int{
		domain.PriorityCritical: 1, domain.PriorityHigh: 1,
		domain.PriorityNormal: 1, domain.PriorityBackground: 1,
	}
	for seed := uint64(1); seed <= 16; seed++ {
		if _, err := loadgen.Run(context.Background(), loadgen.Config{
			URL: server.URL, Keys: keys, Mix: mix, Model: "qwen", Requests: 2, Parallelism: 1,
			Seed: seed, HTTPClient: server.Client(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range keys {
		if seen["Bearer "+key] == 0 {
			t.Fatalf("class %s never received a remainder allocation: %v", key, seen)
		}
	}
}

func TestRunReportsSuccessfulLatencyAndOutcomesByClass(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") == "Bearer background" {
			writer.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(writer, `{}`)
			return
		}
		time.Sleep(20 * time.Millisecond)
		writer.WriteHeader(http.StatusOK)
		fmt.Fprint(writer, `{}`)
	}))
	defer server.Close()
	result, err := loadgen.Run(context.Background(), loadgen.Config{
		URL: server.URL, Model: "qwen", Requests: 4, Parallelism: 1, Seed: 7, HTTPClient: server.Client(),
		Keys: map[domain.PriorityClass]string{domain.PriorityHigh: "high", domain.PriorityBackground: "background"},
		Mix:  map[domain.PriorityClass]int{domain.PriorityHigh: 1, domain.PriorityBackground: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	high := result.ByClass[domain.PriorityHigh]
	background := result.ByClass[domain.PriorityBackground]
	if high.Total != 2 || high.Successes != 2 || high.Latency.Count != 2 {
		t.Fatalf("high = %+v", high)
	}
	if background.Total != 2 || background.Overloaded != 2 || background.Latency.Count != 0 {
		t.Fatalf("background = %+v", background)
	}
	if result.Latency.Count != 2 {
		t.Fatalf("aggregate successful latency = %+v", result.Latency)
	}
}

func TestTransportFailureMakesRunFail(t *testing.T) {
	_, err := loadgen.Run(context.Background(), loadgen.Config{
		URL: "http://127.0.0.1:1", Key: "llmgw_test", Model: "qwen", Requests: 1, Parallelism: 1,
		HTTPClient: &http.Client{Timeout: 100 * time.Millisecond},
	})
	if err == nil {
		t.Fatal("transport failure did not affect run status")
	}
}
