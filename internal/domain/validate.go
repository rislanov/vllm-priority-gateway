package domain

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
)

func (c *Client) Validate() error {
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
	return nil
}

func (p *ModelPool) Validate() error {
	p.PublicModelName = strings.TrimSpace(p.PublicModelName)
	p.UpstreamModelName = strings.TrimSpace(p.UpstreamModelName)
	if p.PublicModelName == "" {
		return errors.New("public model name is required")
	}
	if p.UpstreamModelName == "" {
		return errors.New("upstream model name is required")
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
