package main

import (
	"context"
	"github.com/rislanov/vllm-priority-gateway/internal/config"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTokenizationApplicationRoute(t *testing.T) {
	cfg, e := config.Load(mapLookup(map[string]string{"LLMGW_ADMIN_USERNAME": "test", "LLMGW_ADMIN_PASSWORD": "test-local-password", "LLMGW_API_KEY_HMAC_SECRET": strings.Repeat("s", 32), "LLMGW_DATABASE_PATH": filepath.Join(t.TempDir(), "gateway.db")}))
	if e != nil {
		t.Fatal(e)
	}
	app, e := newGatewayApplication(context.Background(), cfg, mapLookup(nil), io.Discard)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := app.Close(ctx); e != nil {
			t.Error(e)
		}
	}()
	w := httptest.NewRecorder()
	app.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/tokenize", strings.NewReader(`{}`)))
	if w.Code != 401 || w.Header().Get("X-Request-Id") == "" {
		t.Fatalf("outer route: %d %s", w.Code, w.Body)
	}
}
