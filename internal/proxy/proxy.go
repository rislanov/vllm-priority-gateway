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

type responseCopier struct {
	ctx        context.Context
	downstream http.ResponseWriter
	response   *http.Response
	inspector  *usageInspector
	started    time.Time
	result     *Result
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
	copier := responseCopier{
		ctx:        ctx,
		downstream: downstream,
		response:   response,
		inspector:  inspector,
		started:    started,
		result:     &result,
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		copier.commit()
	}
	retryable, outcome := copier.copy()
	return result, retryable, outcome
}

func (c *responseCopier) commit() {
	if c.result.ResponseStarted {
		return
	}
	CopyResponseHeaders(c.downstream.Header(), c.response.Header)
	c.downstream.WriteHeader(c.response.StatusCode)
	c.result.FirstByte = time.Since(c.started)
	c.result.ResponseStarted = true
}

func (c *responseCopier) write(data []byte, provenReadFailure bool) (domain.InferenceOutcome, bool) {
	c.commit()
	written, writeErr := c.downstream.Write(data)
	c.result.BytesSent += int64(written)
	flush(c.downstream)
	c.inspector.Write(data[:written])
	if writeErr == nil && written == len(data) {
		return domain.InferenceNeutral, false
	}
	if writeErr == nil {
		writeErr = io.ErrShortWrite
	}
	c.result.Err = writeErr
	c.result.Cancelled = c.ctx.Err() != nil
	return interruptedOutcome(c.response.StatusCode, provenReadFailure), true
}

func (c *responseCopier) copy() (retryable bool, outcome domain.InferenceOutcome) {
	buffer := make([]byte, 32<<10)
	firstRead := true
	for {
		count, readErr := c.response.Body.Read(buffer)
		provenReadFailure := readErr != nil && !errors.Is(readErr, io.EOF) && c.ctx.Err() == nil
		if count > 0 {
			if outcome, terminal := c.write(buffer[:count], provenReadFailure); terminal {
				return false, outcome
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				c.commit()
				completeUsageInspection(c.result, c.inspector)
				return false, completedOutcome(c.response.StatusCode)
			}

			c.result.Err = readErr
			c.result.Cancelled = c.ctx.Err() != nil
			if c.result.Cancelled {
				c.result.Err = c.ctx.Err()
				return false, interruptedOutcome(c.response.StatusCode, provenReadFailure)
			}
			retryable := firstRead && count == 0 && !c.result.ResponseStarted &&
				c.response.StatusCode >= http.StatusOK && c.response.StatusCode < http.StatusMultipleChoices
			return retryable, domain.InferenceFailure
		}
		firstRead = false
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
