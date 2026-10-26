package proxy

import (
	"context"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenizationPassThrough(t *testing.T) {
	for _, status := range []int{200, 422, 500, 302} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			body := `{"count":2,"tokens":[1,2],"token_strs":["a","b"],"usage":{"prompt_tokens":999,"completion_tokens":999}}`
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				b, _ := io.ReadAll(r.Body)
				if string(b) != `{"model":"upstream","prompt":"hi"}` || r.URL.Path != "/tokenize" {
					t.Error("request changed")
				}
				if r.Header.Get("Authorization") != "Bearer upstream-secret" || r.Header.Get("X-Request-ID") != "utility-id" || r.Header.Get("X-LLM-Session-ID") != "" || r.Header.Get("X-Vllm-Priority") != "" || r.Header.Get("X-Remove") != "" {
					t.Errorf("unsafe headers: %v", r.Header)
				}
				w.Header().Set("Connection", "X-Private")
				w.Header().Set("X-Private", "hidden")
				w.Header().Set("Location", "/another")
				w.WriteHeader(status)
				io.WriteString(w, body)
			}))
			defer upstream.Close()
			h := http.Header{"Authorization": {"Bearer gateway-secret"}, "X-Llm-Session-Id": {"private"}, "X-Vllm-Priority": {"-100"}, "Connection": {"X-Remove"}, "X-Remove": {"secret"}}
			writer := httptest.NewRecorder()
			result := New(upstream.Client()).ForwardTokenization(context.Background(), writer, TokenizationRequest{Headers: h, Body: []byte(`{"model":"upstream","prompt":"hi"}`), RequestID: "utility-id", Backend: domain.Backend{ID: 1, BaseURL: upstream.URL}, UpstreamAPIKey: "upstream-secret"})
			if result.Err != nil || writer.Code != status || writer.Body.String() != body || result.Usage != nil || result.UsageParseFailure != "" || result.RetryCount != 0 || calls.Load() != 1 || writer.Header().Get("X-Private") != "" || writer.Header().Get("X-Request-ID") != "utility-id" {
				t.Fatalf("result=%+v code=%d body=%s", result, writer.Code, writer.Body)
			}
		})
	}
}
func TestTokenizationCancellation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard,r.Body); <-r.Context().Done() }))
	defer upstream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result := New(upstream.Client()).ForwardTokenization(ctx, httptest.NewRecorder(), TokenizationRequest{Backend: domain.Backend{BaseURL: upstream.URL}, Body: []byte(`{}`)})
	if result.Err == nil || !result.Cancelled || result.ResponseStarted {
		t.Fatal(result)
	}
}
func TestTokenizationReusableAdapter(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"count":1}`) }))
	defer upstream.Close()
	response, e := New(upstream.Client()).Tokenize(context.Background(), TokenizationRequest{Backend: domain.Backend{BaseURL: upstream.URL}, Body: []byte(`{}`)})
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	b, _ := io.ReadAll(response.Body)
	if !strings.Contains(string(b), `"count":1`) {
		t.Fatal(string(b))
	}
}
