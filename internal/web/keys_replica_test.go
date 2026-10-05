package web_test

import (
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
	"github.com/rislanov/vllm-priority-gateway/internal/web"
)

// Exercise separate web, security, service, and registry instances against one
// real database. SQLite keeps this web regression test offline; this is not a
// test of PostgreSQL coordination or a supported SQLite multi-replica deployment.
func TestKeyCreationAcrossIndependentHTTPReplicas(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "keys.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	client, err := database.CreateClient(ctx, store.CreateClientParams{
		Name: "replica-test", PriorityClass: domain.PriorityNormal, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	hmacSecret := []byte(strings.Repeat("h", 32))
	var replicas []*httptest.Server
	var registries []*registry.Registry
	for range 2 {
		reg := registry.New(database)
		if err := reg.Reload(ctx); err != nil {
			t.Fatal(err)
		}
		service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
			Store: database, Analytics: database, Registry: reg, Runtime: webRuntime{}, HMACSecret: hmacSecret, Random: rand.Reader,
		})
		if err != nil {
			t.Fatal(err)
		}
		ui, err := web.New(service)
		if err != nil {
			t.Fatal(err)
		}
		security, err := httpapi.NewAdminSecurity(httpapi.AdminSecurityConfig{
			Username: "test", Password: "test-password", Random: rand.Reader,
		})
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(security.Wrap(ui))
		t.Cleanup(server.Close)
		replicas = append(replicas, server)
		registries = append(registries, reg)
	}
	httpClient := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request, _ := http.NewRequest(http.MethodGet, replicas[0].URL+"/admin/keys", nil)
	request.SetBasicAuth("test", "test-password")
	response, err := httpClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	var cookie *http.Cookie
	for _, value := range response.Cookies() {
		if value.Name == "llmgw_csrf" {
			cookie = value
		}
	}
	if cookie == nil {
		t.Fatal("missing CSRF cookie")
	}
	var secretsMu sync.Mutex
	var secrets []string
	var wait sync.WaitGroup
	for _, replica := range replicas {
		wait.Add(1)
		go func(replica *httptest.Server) {
			defer wait.Done()
			form := url.Values{"action": {"create"}, "client_id": {strconv.FormatInt(client.ID, 10)}, "csrf_token": {cookie.Value}}
			req, _ := http.NewRequest(http.MethodPost, replica.URL+"/admin/keys", strings.NewReader(form.Encode()))
			req.SetBasicAuth("test", "test-password")
			req.AddCookie(cookie)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			res, err := httpClient.Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Error(err)
				return
			}
			if res.StatusCode != http.StatusOK || res.Header.Get("Location") != "" || res.Header.Get("Cache-Control") != "no-store" {
				t.Errorf("creation must return an uncached secret directly: HTTP %d", res.StatusCode)
				return
			}
			secret := regexp.MustCompile(`llmgw_[A-Za-z0-9_-]{43}`).FindString(string(body))
			if secret == "" || !strings.Contains(string(body), `id="one-time-secret"`) {
				t.Error("creation response omitted full key")
				return
			}
			secretsMu.Lock()
			secrets = append(secrets, secret)
			secretsMu.Unlock()
		}(replica)
	}
	wait.Wait()
	if len(secrets) != 2 {
		t.Fatalf("received %d full secrets, want 2", len(secrets))
	}
	if secrets[0] == secrets[1] {
		t.Fatal("concurrent operations returned the same key")
	}
	for index, replica := range replicas {
		if err := registries[index].Reload(ctx); err != nil {
			t.Fatal(err)
		}
		snapshot, err := database.LoadSnapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		keys := snapshot.Keys
		if len(keys) != 2 {
			t.Fatalf("replica %d sees %d keys, want 2", index, len(keys))
		}
		for _, secret := range secrets {
			matches := 0
			for _, key := range keys {
				if key.SecretHash == apikey.Digest(hmacSecret, secret) {
					matches++
				}
			}
			if matches != 1 {
				t.Fatalf("secret matches %d persisted keys, want 1", matches)
			}
		}
		// Both normal refresh and an obsolete flash URL must reveal no secret.
		for _, path := range []string{"/admin/keys", "/admin/keys?flash=obsolete"} {
			req, _ := http.NewRequest(http.MethodGet, replica.URL+path, nil)
			req.SetBasicAuth("test", "test-password")
			req.AddCookie(cookie)
			res, err := httpClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK || strings.Contains(string(body), `id="one-time-secret"`) {
				t.Fatal("refresh disclosed a secret")
			}
			for _, secret := range secrets {
				if strings.Contains(string(body), secret) {
					t.Fatal("refresh disclosed plaintext")
				}
			}
		}
		for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
			req, _ := http.NewRequest(http.MethodPost, replica.URL+"/admin/keys", strings.NewReader("action=create&client_id="+strconv.FormatInt(client.ID, 10)))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if status == http.StatusForbidden {
				req.SetBasicAuth("test", "test-password")
				req.AddCookie(cookie)
			}
			res, err := httpClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			res.Body.Close()
			if res.StatusCode != status {
				t.Fatalf("security status %d, want %d", res.StatusCode, status)
			}
		}
		if err := registries[index].Reload(ctx); err != nil {
			t.Fatal(err)
		}
		if len(registries[index].Snapshot().KeyCandidates) != 2 {
			t.Fatal("rejected request created a key")
		}
	}
}
