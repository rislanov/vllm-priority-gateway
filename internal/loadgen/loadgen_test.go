package loadgen_test

import (
	"context"
	"fmt"
	"io"
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

func TestConfigValidateRejectsAmbiguousGatewayURLs(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "leading whitespace", url: " https://gateway.example"},
		{name: "trailing whitespace", url: "https://gateway.example "},
		{name: "userinfo", url: "https://operator:secret@gateway.example"},
		{name: "query", url: "https://gateway.example?tenant=payments"},
		{name: "fragment", url: "https://gateway.example#completions"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := loadgen.Config{
				URL: test.url, Key: "llmgw_test", Model: "qwen",
				Requests: 1, Parallelism: 1,
			}
			if err := config.Validate(); err == nil {
				t.Fatalf("Validate() accepted ambiguous gateway URL %q", test.url)
			}
		})
	}
}

func TestConfigValidateAcceptsAbsoluteGatewayURLsWithPaths(t *testing.T) {
	for _, gatewayURL := range []string{
		"http://gateway.example",
		"https://gateway.example/",
		"https://gateway.example/prefix",
		"https://gateway.example/prefix/",
	} {
		t.Run(gatewayURL, func(t *testing.T) {
			config := loadgen.Config{
				URL: gatewayURL, Key: "llmgw_test", Model: "qwen",
				Requests: 1, Parallelism: 1,
			}
			if err := config.Validate(); err != nil {
				t.Fatalf("Validate() rejected %q: %v", gatewayURL, err)
			}
		})
	}
}

func TestRunAppendsCompletionsEndpointToGatewayPath(t *testing.T) {
	requestPath := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestPath <- request.URL.RequestURI()
		writer.WriteHeader(http.StatusOK)
		fmt.Fprint(writer, `{}`)
	}))
	defer server.Close()

	_, err := loadgen.Run(context.Background(), loadgen.Config{
		URL: server.URL + "/gateway/", Key: "llmgw_test", Model: "qwen",
		Requests: 1, Parallelism: 1, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := <-requestPath; got != "/gateway/v1/completions" {
		t.Fatalf("request URI = %q, want /gateway/v1/completions", got)
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
	if result.TTFT.Count != 1 || result.Latency.Count != 1 || result.TTFT.P95 <= 0 || result.Latency.P95 < 35*time.Millisecond || result.TTFT.P95 > result.Latency.P95 {
		t.Fatalf("TTFT=%v latency=%v", result.TTFT, result.Latency)
	}
}

func TestRunMeasuresTTFTAtFirstResponseBytes(t *testing.T) {
	blockedRead := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBody := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseBody()
	body := &stagedResponseBody{blockedRead: blockedRead, release: release}
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		_ = request.Body.Close()
		responseTimer := time.NewTimer(5 * time.Millisecond)
		defer responseTimer.Stop()
		select {
		case <-responseTimer.C:
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
			Request:    request,
		}, nil
	})}
	type runOutcome struct {
		result loadgen.Result
		err    error
	}
	done := make(chan runOutcome, 1)
	go func() {
		result, err := loadgen.Run(context.Background(), loadgen.Config{
			URL: "http://gateway.invalid", Key: "llmgw_test", Model: "qwen",
			Requests: 1, Parallelism: 1, Stream: true, HTTPClient: client,
		})
		done <- runOutcome{result: result, err: err}
	}()

	readTimer := time.NewTimer(time.Second)
	defer readTimer.Stop()
	select {
	case <-blockedRead:
	case <-readTimer.C:
		t.Fatal("timed out waiting for the staged response read")
	}
	holdTimer := time.NewTimer(50 * time.Millisecond)
	select {
	case outcome := <-done:
		t.Fatalf("Run returned before the staged response was released: result=%+v err=%v", outcome.result, outcome.err)
	case <-holdTimer.C:
	}
	releaseBody()

	completionTimer := time.NewTimer(time.Second)
	defer completionTimer.Stop()
	var outcome runOutcome
	select {
	case outcome = <-done:
	case <-completionTimer.C:
		t.Fatal("timed out waiting for Run after releasing the staged response")
	}
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	if outcome.result.Successes != 1 || outcome.result.TTFT.Count != 1 || outcome.result.Latency.Count != 1 {
		t.Fatalf("result = %+v", outcome.result)
	}
	if separation := outcome.result.Latency.P95 - outcome.result.TTFT.P95; separation < 35*time.Millisecond {
		t.Fatalf("latency - TTFT = %v, want at least 35ms (TTFT=%v latency=%v)", separation, outcome.result.TTFT, outcome.result.Latency)
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type stagedResponseBody struct {
	blockedRead chan<- struct{}
	release     <-chan struct{}
	read        int
}

func (b *stagedResponseBody) Read(buffer []byte) (int, error) {
	switch b.read {
	case 0:
		b.read++
		return copy(buffer, "data: first\n\n"), nil
	case 1:
		b.read++
		close(b.blockedRead)
		<-b.release
		return copy(buffer, "data: [DONE]\n\n"), nil
	default:
		return 0, io.EOF
	}
}

func (*stagedResponseBody) Close() error { return nil }
