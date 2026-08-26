package integration_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
)

func TestStreamingCancellationAndLeaseLifetime(t *testing.T) {
	h := newHarness(t)
	poolID := h.createPool("qwen")
	fake, backendID := h.addFake(poolID, "gpu-a", fakevllm.State{
		TTFT: 20 * time.Millisecond, TokenDelay: 100 * time.Millisecond, Tokens: []string{"one", "two", "three"},
	})
	h.waitBackend(backendID, eligible)
	_, key := h.createClient("stream-client", domain.PriorityHigh, -10, 1, poolID)

	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.server.URL+"/v1/completions", strings.NewReader(postBody("qwen", true)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	started := time.Now()
	response, err := h.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1024)
	count, err := response.Body.Read(buffer)
	if err != nil || count == 0 {
		t.Fatalf("first stream read = %d, %v", count, err)
	}
	if elapsed := time.Since(started); elapsed > 150*time.Millisecond {
		t.Fatalf("first frame was buffered for %s", elapsed)
	}
	if !strings.Contains(string(buffer[:count]), `"content":"one"`) {
		t.Fatalf("first frame = %q", buffer[:count])
	}

	second, payload := h.public(http.MethodPost, "/v1/completions", key, postBody("qwen", false))
	if second.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("concurrent request = %d %s", second.StatusCode, payload)
	}
	cancel()
	response.Body.Close()
	eventually(t, time.Second, func() bool { return fake.Snapshot().CancelledRequests > 0 })

	fake.SetState(fakevllm.State{})
	eventually(t, time.Second, func() bool {
		third, _ := h.public(http.MethodPost, "/v1/completions", key, postBody("qwen", false))
		return third.StatusCode == http.StatusOK
	})
}

func TestStreamingIsByteExactAndRetryStopsAfterFirstByte(t *testing.T) {
	t.Run("byte exact stream", func(t *testing.T) {
		h := newHarness(t)
		poolID := h.createPool("qwen")
		_, backendID := h.addFake(poolID, "gpu-a", fakevllm.State{Tokens: []string{"one", "two"}, TokenDelay: time.Millisecond})
		h.waitBackend(backendID, eligible)
		_, key := h.createClient("stream-client", domain.PriorityHigh, -10, 2, poolID)
		response, payload := h.public(http.MethodPost, "/v1/completions", key, postBody("qwen", true))
		want := "data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"content\":\"two\"}}]}\n\n" +
			"data: [DONE]\n\n"
		if response.StatusCode != http.StatusOK || string(payload) != want {
			t.Fatalf("stream = %d %q, want %q", response.StatusCode, payload, want)
		}
	})

	t.Run("retry boundary", func(t *testing.T) {
		h := newHarness(t)
		poolID := h.createPool("qwen")
		fakeA, backendA := h.addFake(poolID, "gpu-a", fakevllm.State{ResetMode: fakevllm.ResetBeforeHeaders})
		fakeB, backendB := h.addFake(poolID, "gpu-b", fakevllm.State{Waiting: 4})
		h.waitBackend(backendA, eligible)
		h.waitBackend(backendB, eligible)
		_, key := h.createClient("retry-client", domain.PriorityHigh, -10, 2, poolID)

		response, payload := h.public(http.MethodPost, "/v1/completions", key, postBody("qwen", false))
		if response.StatusCode != http.StatusOK || len(fakeA.Snapshot().Requests) != 1 || len(fakeB.Snapshot().Requests) != 1 {
			t.Fatalf("pre-byte retry = %d %s, A=%d B=%d", response.StatusCode, payload, len(fakeA.Snapshot().Requests), len(fakeB.Snapshot().Requests))
		}

		fakeA.SetState(fakevllm.State{ResetMode: fakevllm.ResetAfterChunks, ResetAfterChunks: 1, Tokens: []string{"one", "two"}})
		request, err := http.NewRequest(http.MethodPost, h.server.URL+"/v1/completions", strings.NewReader(postBody("qwen", true)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		stream, err := h.client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		truncated, _ := io.ReadAll(stream.Body)
		stream.Body.Close()
		if !strings.Contains(string(truncated), `"content":"one"`) || strings.Contains(string(truncated), "[DONE]") {
			t.Fatalf("post-byte reset body = %q", truncated)
		}
		if len(fakeA.Snapshot().Requests) != 2 || len(fakeB.Snapshot().Requests) != 1 {
			t.Fatalf("post-byte request retried: A=%d B=%d", len(fakeA.Snapshot().Requests), len(fakeB.Snapshot().Requests))
		}
	})
}
