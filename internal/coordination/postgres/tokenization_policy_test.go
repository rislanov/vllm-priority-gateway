package postgres

import (
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"testing"
	"time"
)

func TestTokenizationPolicyFingerprint(t *testing.T) {
	p := ReplicaPolicy{Tokenization: coordination.DefaultTokenizationPolicy()}
	base := PolicyFingerprint(p)
	for _, change := range []func(*coordination.TokenizationPolicy){func(p *coordination.TokenizationPolicy) { p.ClientMaxInflight++ }, func(p *coordination.TokenizationPolicy) { p.BackendMaxInflight++ }, func(p *coordination.TokenizationPolicy) { p.ClientRPM++ }, func(p *coordination.TokenizationPolicy) { p.Timeout += time.Second }} {
		q := p
		change(&q.Tokenization)
		if PolicyFingerprint(q) == base {
			t.Fatal("policy field missing from fingerprint")
		}
	}
}
