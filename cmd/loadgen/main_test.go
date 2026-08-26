package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunRejectsInvalidTrafficMix(t *testing.T) {
	err := run([]string{"-model", "qwen", "-class-keys", "high=llmgw_high", "-mix", "high=not-a-number"})
	if err == nil || !strings.Contains(err.Error(), "invalid weight") {
		t.Fatalf("run error = %v", err)
	}
}

func TestRunFailsOnTransportError(t *testing.T) {
	err := run([]string{"-url", "http://127.0.0.1:1", "-key", "llmgw_test", "-model", "qwen", "-requests", "1", "-parallelism", "1"})
	if err == nil || !strings.Contains(err.Error(), "transport layer") {
		t.Fatalf("run error = %v", err)
	}
}

func TestRunTreatsIntentionalOverloadAsSuccessfulExecution(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusTooManyRequests)
		_, _ = fmt.Fprint(writer, `{"error":{"code":"gateway_overloaded"}}`)
	}))
	defer server.Close()
	err := run([]string{"-url", server.URL, "-key", "llmgw_test", "-model", "qwen", "-requests", "1", "-parallelism", "1", "-json"})
	if err != nil {
		t.Fatalf("run error = %v", err)
	}
}
