package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestTokenizationResponseCannotSelectAnActiveContentType(t *testing.T) {
	for _, contentType := range []string{"text/html", "application/xhtml+xml", "image/svg+xml", "", "application/json"} {
		for _, status := range []int{http.StatusOK, http.StatusUnprocessableEntity, http.StatusInternalServerError} {
			t.Run(contentType+"/"+http.StatusText(status), func(t *testing.T) {
				const body = `<script>alert(document.domain)</script>`
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					// An explicitly empty value disables net/http's automatic sniffing.
					w.Header()["Content-Type"] = []string{contentType}
					w.Header().Set("X-Content-Type-Options", "upstream-value")
					w.Header().Set("Retry-After", "7")
					w.WriteHeader(status)
					io.WriteString(w, body)
				}))
				defer upstream.Close()
				writer := httptest.NewRecorder()
				result := New(upstream.Client()).ForwardTokenization(context.Background(), writer, TokenizationRequest{
					Backend: domain.Backend{ID: 1, BaseURL: upstream.URL}, RequestID: "security-test",
				})
				response := writer.Result()
				defer response.Body.Close()
				if result.Err != nil || response.StatusCode != status || writer.Body.String() != body || result.BytesSent != int64(len(body)) {
					t.Fatalf("pass-through changed: result=%+v status=%d body=%q", result, response.StatusCode, writer.Body.String())
				}
				if got := response.Header.Get("Content-Type"); got != "application/json" {
					t.Errorf("unsafe response Content-Type: %q", got)
				}
				if got := response.Header.Get("X-Content-Type-Options"); got != "nosniff" {
					t.Errorf("response permits content sniffing: %q", got)
				}
				if response.Header.Get("Retry-After") != "7" || response.Header.Get("X-Request-Id") != "security-test" {
					t.Fatal("response metadata was lost")
				}
			})
		}
	}
}
