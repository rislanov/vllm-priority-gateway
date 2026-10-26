package proxy

import (
	"context"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"io"
	"net"
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
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
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

type tokenizationRoundTripFunc func(*http.Request) (*http.Response, error)

func (f tokenizationRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type tokenizationBrokenBody struct{}

func (tokenizationBrokenBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (tokenizationBrokenBody) Close() error             { return nil }
func TestTokenizationReadFailureBeforeCommitDoesNotLeakHeaders(t *testing.T) {
	client := &http.Client{Transport: tokenizationRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Length": {"99"}, "Content-Encoding": {"gzip"}, "X-Upstream": {"not-committed"}}, Body: tokenizationBrokenBody{}}, nil
	})}
	w := httptest.NewRecorder()
	r := New(client).ForwardTokenization(context.Background(), w, TokenizationRequest{Backend: domain.Backend{BaseURL: "http://backend.invalid"}})
	if r.Err == nil || r.ResponseStarted {
		t.Fatalf("result %+v", r)
	}
	for _, key := range []string{"Content-Length", "Content-Encoding", "X-Upstream"} {
		if w.Header().Get(key) != "" {
			t.Fatalf("uncommitted upstream header %s leaked", key)
		}
	}
}
func TestTokenizationTruncatedCommittedResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		io.WriteString(w, `{"count":`)
	}))
	defer upstream.Close()
	r := New(upstream.Client()).ForwardTokenization(context.Background(), httptest.NewRecorder(), TokenizationRequest{Backend: domain.Backend{BaseURL: upstream.URL}})
	if r.Err == nil || !r.ResponseStarted || r.Status != 200 {
		t.Fatalf("truncation not reported: %+v", r)
	}
}

func TestTokenizationDoesNotRetryCallerIdempotencyKey(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 2 {
			conn, _, e := w.(http.Hijacker).Hijack()
			if e != nil {
				t.Error(e)
				return
			}
			conn.Close()
			return
		}
		io.WriteString(w, `{"count":1}`)
	}))
	defer upstream.Close()
	p := New(upstream.Client())
	request := TokenizationRequest{Backend: domain.Backend{BaseURL: upstream.URL}, Body: []byte(`{"prompt":"synthetic"}`), Headers: http.Header{"Idempotency-Key": {"caller"}, "X-Idempotency-Key": {"caller"}}}
	response, e := p.Tokenize(context.Background(), request)
	if e != nil {
		t.Fatal(e)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	response, e = p.Tokenize(context.Background(), request)
	if response != nil {
		response.Body.Close()
	}
	if e == nil || calls.Load() != 2 {
		t.Fatalf("transport replayed tokenization: calls=%d err=%v", calls.Load(), e)
	}
}

func TestTokenizationBoundsSlowDownstream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		io.CopyN(w, strings.NewReader(strings.Repeat("x", 16<<20)), 16<<20)
	}))
	defer upstream.Close()
	done := make(chan Result, 1)
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
		defer cancel()
		done <- New(upstream.Client()).ForwardTokenization(ctx, w, TokenizationRequest{Backend: domain.Backend{BaseURL: upstream.URL}, Body: []byte(`{}`)})
	}))
	defer downstream.Close()
	conn, e := net.Dial("tcp", strings.TrimPrefix(downstream.URL, "http://"))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	io.WriteString(conn, "GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	select {
	case result := <-done:
		if result.Err == nil || !result.ResponseStarted {
			t.Fatalf("slow delivery not bounded: %+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("downstream write exceeded utility deadline")
	}
}
