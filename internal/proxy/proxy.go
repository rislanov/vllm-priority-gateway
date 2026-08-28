package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

type Target struct {
	Backend        domain.Backend
	UpstreamAPIKey string
	Complete       func(domain.InferenceOutcome)
}

type AlternateSelector func(exclude map[int64]struct{}) (Target, error)

type Request struct {
	Method          string
	Path            string
	Headers         http.Header
	Body            []byte
	RequestID       string
	Priority        int
	Target          Target
	SelectAlternate AlternateSelector
}

type Result struct {
	BackendID         int64
	Status            int
	BytesSent         int64
	FirstByte         time.Duration
	RetryCount        int
	ResponseStarted   bool
	Cancelled         bool
	Usage             *domain.TokenUsage
	UsageParseFailure string
	Err               error
}

type Proxy struct {
	client *http.Client
}

func New(client *http.Client) *Proxy {
	if client == nil {
		client = http.DefaultClient
	}
	noRedirects := *client
	noRedirects.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Proxy{client: &noRedirects}
}

func (p *Proxy) Forward(ctx context.Context, downstream http.ResponseWriter, request Request) Result {
	started := time.Now()
	target := request.Target
	excluded := make(map[int64]struct{}, 2)
	result := Result{BackendID: target.Backend.ID}
	for attempt := 0; attempt < 2; attempt++ {
		excluded[target.Backend.ID] = struct{}{}
		attemptResult, retryable, outcome := p.forwardOnce(ctx, downstream, request, target, started)
		if target.Complete != nil {
			target.Complete(outcome)
		}
		attemptResult.RetryCount = result.RetryCount
		result = attemptResult
		if !retryable || attempt == 1 || request.SelectAlternate == nil {
			return result
		}
		next, err := request.SelectAlternate(copyExclusions(excluded))
		if err != nil {
			result.Err = fmt.Errorf("select retry backend: %w", err)
			return result
		}
		result.RetryCount++
		target = next
	}
	return result
}

func (p *Proxy) forwardOnce(
	ctx context.Context,
	downstream http.ResponseWriter,
	request Request,
	target Target,
	started time.Time,
) (Result, bool, domain.InferenceOutcome) {
	result := Result{BackendID: target.Backend.ID}
	upstreamURL := strings.TrimRight(target.Backend.BaseURL, "/") + request.Path
	upstream, err := http.NewRequestWithContext(ctx, request.Method, upstreamURL, bytes.NewReader(request.Body))
	if err != nil {
		result.Err = fmt.Errorf("create upstream request: %w", err)
		return result, false, domain.InferenceNeutral
	}
	upstream.Header = PrepareUpstreamHeaders(request.Headers, request.RequestID, request.Priority, target.UpstreamAPIKey)
	upstream.ContentLength = int64(len(request.Body))
	response, err := p.client.Do(upstream)
	if err != nil {
		result.Err = err
		if ctx.Err() != nil {
			result.Err = ctx.Err()
			result.Cancelled = true
			return result, false, domain.InferenceNeutral
		}
		return result, true, domain.InferenceFailure
	}
	defer response.Body.Close()
	result.Status = response.StatusCode
	inspector := newUsageInspector(response.Header.Get("Content-Type"))
	commit := func() {
		if !result.ResponseStarted {
			CopyResponseHeaders(downstream.Header(), response.Header)
			downstream.WriteHeader(response.StatusCode)
			result.FirstByte = time.Since(started)
			result.ResponseStarted = true
		}
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		commit()
	}

	buffer := make([]byte, 32<<10)
	count, readErr := response.Body.Read(buffer)
	provenReadFailure := readErr != nil && !errors.Is(readErr, io.EOF) && ctx.Err() == nil
	if count == 0 && readErr != nil && !errors.Is(readErr, io.EOF) {
		result.Err = readErr
		if ctx.Err() != nil {
			result.Err = ctx.Err()
			result.Cancelled = true
			return result, false, interruptedOutcome(response.StatusCode, provenReadFailure)
		}
		return result, response.StatusCode >= 200 && response.StatusCode < 300, domain.InferenceFailure
	}

	if count == 0 && errors.Is(readErr, io.EOF) {
		commit()
		completeUsageInspection(&result, inspector)
		return result, false, completedOutcome(response.StatusCode)
	}
	if count > 0 {
		commit()
		written, writeErr := downstream.Write(buffer[:count])
		result.BytesSent += int64(written)
		flush(downstream)
		inspector.Write(buffer[:written])
		if writeErr != nil || written != count {
			if writeErr == nil {
				writeErr = io.ErrShortWrite
			}
			result.Err = writeErr
			result.Cancelled = ctx.Err() != nil
			return result, false, interruptedOutcome(response.StatusCode, provenReadFailure)
		}
	}
	if readErr != nil {
		if !errors.Is(readErr, io.EOF) {
			result.Err = readErr
			result.Cancelled = ctx.Err() != nil
			if result.Cancelled {
				return result, false, interruptedOutcome(response.StatusCode, provenReadFailure)
			}
			return result, false, domain.InferenceFailure
		} else {
			completeUsageInspection(&result, inspector)
		}
		return result, false, completedOutcome(response.StatusCode)
	}

	for {
		count, readErr = response.Body.Read(buffer)
		provenReadFailure = readErr != nil && !errors.Is(readErr, io.EOF) && ctx.Err() == nil
		if count > 0 {
			written, writeErr := downstream.Write(buffer[:count])
			result.BytesSent += int64(written)
			flush(downstream)
			inspector.Write(buffer[:written])
			if writeErr != nil || written != count {
				if writeErr == nil {
					writeErr = io.ErrShortWrite
				}
				result.Err = writeErr
				result.Cancelled = ctx.Err() != nil
				return result, false, interruptedOutcome(response.StatusCode, provenReadFailure)
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				result.Err = readErr
				result.Cancelled = ctx.Err() != nil
				if result.Cancelled {
					return result, false, interruptedOutcome(response.StatusCode, provenReadFailure)
				}
				return result, false, domain.InferenceFailure
			} else {
				completeUsageInspection(&result, inspector)
			}
			return result, false, completedOutcome(response.StatusCode)
		}
	}
}

func completeUsageInspection(result *Result, inspector *usageInspector) {
	usage, format, failed := inspector.Result()
	result.Usage = usage
	if failed {
		result.UsageParseFailure = format
	}
}

func completedOutcome(status int) domain.InferenceOutcome {
	if status >= http.StatusInternalServerError {
		return domain.InferenceFailure
	}
	return domain.InferenceSuccess
}

func interruptedOutcome(status int, provenReadFailure bool) domain.InferenceOutcome {
	if status >= http.StatusInternalServerError || provenReadFailure {
		return domain.InferenceFailure
	}
	return domain.InferenceNeutral
}

func flush(writer http.ResponseWriter) {
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
}

func copyExclusions(source map[int64]struct{}) map[int64]struct{} {
	destination := make(map[int64]struct{}, len(source))
	for id := range source {
		destination[id] = struct{}{}
	}
	return destination
}
