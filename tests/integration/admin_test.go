package integration_test

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
)

func TestAdminAuthenticationCSRFCRUDAndOneTimeSecret(t *testing.T) {
	h := newHarness(t)

	request, _ := http.NewRequest(http.MethodGet, h.server.URL+"/admin", nil)
	response, err := h.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || response.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("unauthenticated admin = %d", response.StatusCode)
	}

	missingCSRF, _ := http.NewRequest(http.MethodPost, h.server.URL+"/admin/api/pools", strings.NewReader(`{}`))
	missingCSRF.SetBasicAuth(adminUsername, adminPassword)
	missingCSRF.Header.Set("Content-Type", "application/json")
	response, err = h.client.Do(missingCSRF)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("missing CSRF status = %d", response.StatusCode)
	}

	poolID := h.createPool("qwen")
	_, backendID := h.addFake(poolID, "gpu-a", fakevllm.State{})
	h.waitBackend(backendID, eligible)
	clientID, apiSecret := h.createClient("admin-client", domain.PriorityNormal, 0, 4, poolID)
	if !strings.HasPrefix(apiSecret, "llmgw_") {
		t.Fatalf("created API key = %q", apiSecret)
	}
	modelsResponse, modelsBody := h.public(http.MethodGet, "/v1/models", apiSecret, "")
	if modelsResponse.StatusCode != http.StatusOK {
		t.Fatalf("authenticated models = %d %s", modelsResponse.StatusCode, modelsBody)
	}
	eventually(t, time.Second, func() bool {
		status := h.adminObject(http.MethodGet, "/admin/api/status", nil, http.StatusOK)
		for _, value := range status["keys"].([]any) {
			key := value.(map[string]any)
			if key["lastUsedAt"] != nil {
				return true
			}
		}
		return false
	})

	for _, path := range []string{"/admin", "/admin/clients", "/admin/keys", "/admin/backends"} {
		request, _ := http.NewRequest(http.MethodGet, h.server.URL+path, nil)
		request.SetBasicAuth(adminUsername, adminPassword)
		page, pageErr := h.client.Do(request)
		if pageErr != nil {
			t.Fatal(pageErr)
		}
		body, _ := io.ReadAll(page.Body)
		page.Body.Close()
		if page.StatusCode != http.StatusOK || !strings.Contains(page.Header.Get("Content-Type"), "text/html") {
			t.Fatalf("GET %s = %d %s", path, page.StatusCode, body)
		}
		if strings.Contains(string(body), apiSecret) {
			t.Fatalf("API secret leaked on %s", path)
		}
	}

	form := url.Values{"csrf_token": {h.csrf}, "action": {"create"}, "client_id": {strconv.FormatInt(clientID, 10)}}
	request, _ = http.NewRequest(http.MethodPost, h.server.URL+"/admin/keys", strings.NewReader(form.Encode()))
	request.SetBasicAuth(adminUsername, adminPassword)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	page, err := h.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	oneTimeBody, _ := io.ReadAll(page.Body)
	page.Body.Close()
	if page.StatusCode != http.StatusOK || !strings.Contains(string(oneTimeBody), `id="one-time-secret"`) || !strings.Contains(string(oneTimeBody), "llmgw_") {
		t.Fatalf("one-time key page = %d %s", page.StatusCode, oneTimeBody)
	}
	uiSecret := regexp.MustCompile(`llmgw_[A-Za-z0-9_-]{43}`).FindString(string(oneTimeBody))
	if uiSecret == "" {
		t.Fatalf("one-time key page did not contain a complete secret: %s", oneTimeBody)
	}

	h.adminObject(http.MethodPost, "/admin/api/backends/"+strconv.FormatInt(backendID, 10)+"/drain", nil, http.StatusOK)
	if !h.registry.Snapshot().BackendsByID[backendID].Draining {
		t.Fatal("drain was not published")
	}
	h.adminObject(http.MethodPost, "/admin/api/backends/"+strconv.FormatInt(backendID, 10)+"/resume", nil, http.StatusOK)
	if h.registry.Snapshot().BackendsByID[backendID].Draining {
		t.Fatal("resume was not published")
	}
	status := h.adminObject(http.MethodGet, "/admin/api/status", nil, http.StatusOK)
	if numberID(t, status["revision"]) < 7 || len(status["backends"].([]any)) != 1 {
		t.Fatalf("aggregate status = %#v", status)
	}

	var onDisk strings.Builder
	files, err := filepath.Glob(h.databasePath + "*")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		onDisk.Write(data)
	}
	if strings.Contains(onDisk.String(), apiSecret) || strings.Contains(onDisk.String(), uiSecret) {
		t.Fatal("plaintext API key found in SQLite files")
	}
}
