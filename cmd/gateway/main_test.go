package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunServesHealthAndShutsDownGracefully(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	environment := validEnvironment(filepath.Join(t.TempDir(), "gateway.db"))
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	client := &http.Client{Timeout: 200 * time.Millisecond}
	url := "http://" + listener.Addr().String() + "/healthz"
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, requestErr := client.Get(url)
		if requestErr == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("gateway did not become healthy")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("gateway did not shut down within deadline")
	}
}

func TestRunRejectsMissingHMACSecretBeforeServing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	environment := validEnvironment(filepath.Join(t.TempDir(), "gateway.db"))
	delete(environment, "LLMGW_API_KEY_HMAC_SECRET")
	err = run(context.Background(), mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "LLMGW_API_KEY_HMAC_SECRET") {
		t.Fatalf("run error = %v", err)
	}
}

func validEnvironment(databasePath string) map[string]string {
	return map[string]string{
		"LLMGW_LISTEN_ADDRESS":        "127.0.0.1:0",
		"LLMGW_DATABASE_PATH":         databasePath,
		"LLMGW_ADMIN_USERNAME":        "operator",
		"LLMGW_ADMIN_PASSWORD":        "correct horse battery staple",
		"LLMGW_API_KEY_HMAC_SECRET":   strings.Repeat("h", 32),
		"LLMGW_SHUTDOWN_GRACE_PERIOD": "500ms",
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
