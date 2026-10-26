package proxy

import (
	"bytes"
	"context"
	"errors"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"io"
	"net/http"
	"strings"
	"time"
)

type TokenizationRequest struct {
	Headers        http.Header
	Body           []byte
	RequestID      string
	Backend        domain.Backend
	UpstreamAPIKey string
}

// Tokenize is the common upstream adapter, also usable by a future admission
// estimator. It performs one request, without inference accounting or retries.
// The caller owns response.Body and must supply a bounded context.
func (p *Proxy) Tokenize(ctx context.Context, r TokenizationRequest) (*http.Response, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(r.Backend.BaseURL, "/")+"/tokenize", bytes.NewReader(r.Body))
	if e != nil {
		return nil, e
	}
	req.Header = PrepareTokenizationHeaders(r.Headers, r.RequestID, r.UpstreamAPIKey)
	// A caller must not turn this one-attempt utility operation into a
	// replayable POST in net/http's reused-connection recovery path.
	req.GetBody = nil
	return p.client.Do(req)
}

func (p *Proxy) ForwardTokenization(ctx context.Context, w http.ResponseWriter, r TokenizationRequest) (result Result) {
	started := time.Now()
	result.BackendID = r.Backend.ID
	controller := http.NewResponseController(w)
	if deadline, ok := ctx.Deadline(); ok {
		if e := controller.SetWriteDeadline(deadline); e != nil && !errors.Is(e, http.ErrNotSupported) {
			result.Err = e
			return result
		}
		defer controller.SetWriteDeadline(time.Time{})
	}
	response, e := p.Tokenize(ctx, r)
	if e != nil {
		result.Err = e
		if ctx.Err() != nil {
			result.Err = ctx.Err()
			result.Cancelled = true
		}
		return result
	}
	defer response.Body.Close()
	result.Status = response.StatusCode
	// Commit upstream errors immediately, preserving their status even if the
	// error response body subsequently fails to arrive.
	commit := func() {
		if !result.ResponseStarted {
			CopyResponseHeaders(w.Header(), response.Header)
			w.Header().Set("X-Request-Id", r.RequestID)
			w.WriteHeader(response.StatusCode)
			result.ResponseStarted = true
			result.FirstByte = time.Since(started)
		}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		commit()
	}
	buffer := make([]byte, 32<<10)
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			commit()
			written, writeErr := w.Write(buffer[:n])
			result.BytesSent += int64(written)
			if writeErr != nil {
				result.Err = writeErr
				break
			}
			if written != n {
				result.Err = io.ErrShortWrite
				break
			}
			if e = controller.Flush(); e != nil && !errors.Is(e, http.ErrNotSupported) {
				result.Err = e
				break
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				result.Err = readErr
				result.UpstreamFailure = "upstream_body_read_error"
			} else {
				commit()
			}
			break
		}
	}
	if ctx.Err() != nil {
		result.Cancelled = true
		if result.Err == nil {
			result.Err = ctx.Err()
		}
	}
	return result
}
