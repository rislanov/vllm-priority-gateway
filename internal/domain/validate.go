package domain

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
)

const (
	MaxPublicModelNameBytes       = 256
	MaxClientConcurrency          = 10_000
	MaxPoolGatewayInflight        = 100_000
	MaxRequestsPerMinute    int64 = 10_000_000
	MaxTokensPerMinute      int64 = 10_000_000_000_000
)

func (c *Client) Validate() error {
	return c.validatePolicy(nil)
}

// ValidateUpdate permits an unchanged grandfathered concurrency value from an
// older SQLite database, or lowering it into the supported range.
func (c *Client) ValidateUpdate(previous Client) error {
	return c.validatePolicy(&previous)
}

func (c *Client) validatePolicy(previous *Client) error {
	c.Name = strings.TrimSpace(c.Name)
	if c.Name == "" {
		return errors.New("client name is required")
	}
	if !c.PriorityClass.Valid() {
		return fmt.Errorf("invalid priority class %q", c.PriorityClass)
	}
	if c.VLLMPriority < math.MinInt32 || c.VLLMPriority > math.MaxInt32 {
		return errors.New("vLLM priority must fit a signed 32-bit integer")
	}
	if c.MaxConcurrency < 0 {
		return errors.New("max concurrency cannot be negative")
	}
	if c.MaxConcurrency > MaxClientConcurrency && (previous == nil || c.MaxConcurrency != previous.MaxConcurrency) {
		return fmt.Errorf("max concurrency must not exceed %d", MaxClientConcurrency)
	}
	if c.RequestsPerMinute < 0 || c.RequestsPerMinute > MaxRequestsPerMinute {
		return fmt.Errorf("requests per minute must be between 0 and %d", MaxRequestsPerMinute)
	}
	if c.TokensPerMinute < 0 || c.TokensPerMinute > MaxTokensPerMinute {
		return fmt.Errorf("tokens per minute must be between 0 and %d", MaxTokensPerMinute)
	}
	return nil
}

func (p *ModelPool) Validate() error {
	return p.validatePolicy(nil)
}

// ValidateUpdate has the same narrow SQLite compatibility rule as Client.
func (p *ModelPool) ValidateUpdate(previous ModelPool) error {
	return p.validatePolicy(&previous)
}

func (p *ModelPool) validatePolicy(previous *ModelPool) error {
	p.PublicModelName = strings.TrimSpace(p.PublicModelName)
	p.UpstreamModelName = strings.TrimSpace(p.UpstreamModelName)
	if p.PublicModelName == "" {
		return errors.New("public model name is required")
	}
	if len(p.PublicModelName) > MaxPublicModelNameBytes {
		return fmt.Errorf("public model name must not exceed %d bytes", MaxPublicModelNameBytes)
	}
	if p.UpstreamModelName == "" {
		return errors.New("upstream model name is required")
	}
	if p.MaxGatewayInflight < 0 {
		return errors.New("max gateway inflight cannot be negative")
	}
	if p.MaxGatewayInflight > MaxPoolGatewayInflight && (previous == nil || p.MaxGatewayInflight != previous.MaxGatewayInflight) {
		return fmt.Errorf("max gateway inflight must not exceed %d", MaxPoolGatewayInflight)
	}
	if p.MaxWaiting < 0 {
		return errors.New("max waiting cannot be negative")
	}
	return nil
}

func (b *Backend) Validate() error {
	b.Name = strings.TrimSpace(b.Name)
	if b.Name == "" {
		return errors.New("backend name is required")
	}
	if math.IsNaN(b.CapacityHint) || math.IsInf(b.CapacityHint, 0) || b.CapacityHint <= 0 {
		return errors.New("capacity hint must be a positive finite number")
	}
	if math.IsNaN(b.RunningSoftLimit) || math.IsInf(b.RunningSoftLimit, 0) || b.RunningSoftLimit <= 0 {
		return errors.New("running soft limit must be a positive finite number")
	}

	parsed, err := url.Parse(strings.TrimSpace(b.BaseURL))
	if err != nil {
		return fmt.Errorf("parse backend base URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("backend base URL must use http or https")
	}
	if parsed.Host == "" {
		return errors.New("backend base URL must be absolute")
	}
	if parsed.User != nil {
		return errors.New("backend base URL cannot contain user information")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("backend base URL cannot contain query or fragment")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = strings.TrimRight(parsed.RawPath, "/")
	b.BaseURL = parsed.String()
	b.UpstreamAPIKeyEnv = strings.TrimSpace(b.UpstreamAPIKeyEnv)
	return nil
}
