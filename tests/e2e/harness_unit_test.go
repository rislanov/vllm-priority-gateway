package e2e_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestFindDuplicateClientKeyDoesNotExposeSecret(t *testing.T) {
	secret := "llmgw_do-not-print-this-secret"
	first, second, duplicate := findDuplicateClientKey([]namedClientKey{
		{name: "High probe", value: secret},
		{name: "Critical probe", value: "llmgw_distinct"},
		{name: "Low probe 1", value: secret},
	})
	if !duplicate {
		t.Fatal("duplicate client key was not detected")
	}
	if first != "High probe" || second != "Low probe 1" {
		t.Fatalf("duplicate roles = %q and %q", first, second)
	}
	if strings.Contains(first+second, secret) {
		t.Fatal("duplicate report exposed the API key")
	}
}

func TestFetchMetricsHonorsContextDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	t.Cleanup(server.Close)
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	h := &remoteHarness{
		t:      t,
		cfg:    e2eConfig{baseURL: baseURL},
		client: server.Client(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, err = h.fetchMetrics(ctx)
	if err == nil {
		t.Fatal("fetchMetrics succeeded against a stalled endpoint")
	}
	if time.Since(started) > time.Second {
		t.Fatalf("fetchMetrics did not honor the deadline: %s", time.Since(started))
	}
}
