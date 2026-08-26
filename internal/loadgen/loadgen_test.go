package loadgen_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	if result.TTFT.P95 <= 0 || result.Latency.P95 < 35*time.Millisecond || result.TTFT.P95 >= result.Latency.P95 {
		t.Fatalf("TTFT=%v latency=%v", result.TTFT, result.Latency)
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
