package apikey_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
)

func TestGenerateProducesExpectedFormatAndPrefix(t *testing.T) {
	random := bytes.Repeat([]byte{0x42}, 32)
	key, err := apikey.Generate(bytes.NewReader(random))
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	const want = "llmgw_QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI"
	if key.Value != want {
		t.Fatalf("Value = %q, want %q", key.Value, want)
	}
	if key.Prefix != want[:12] {
		t.Fatalf("Prefix = %q, want %q", key.Prefix, want[:12])
	}
}

func TestGeneratePropagatesEntropyFailure(t *testing.T) {
	broken := errorReader{err: errors.New("entropy unavailable")}
	if _, err := apikey.Generate(broken); !errors.Is(err, broken.err) {
		t.Fatalf("Generate() error = %v", err)
	}
}

func TestDigestIsStableAndSecretKeyed(t *testing.T) {
	raw := "llmgw_example"
	first := apikey.Digest([]byte("01234567890123456789012345678901"), raw)
	second := apikey.Digest([]byte("01234567890123456789012345678901"), raw)
	otherSecret := apikey.Digest([]byte("abcdefghijklmnopqrstuvwxyzABCDEF"), raw)
	if first != second {
		t.Fatal("same inputs produced different digests")
	}
	if first == otherSecret {
		t.Fatal("different server secrets produced the same digest")
	}
}

func TestVerifyAcceptsExactKeyAndRejectsDifferentKey(t *testing.T) {
	secret := []byte("01234567890123456789012345678901")
	digest := apikey.Digest(secret, "llmgw_original")
	if !apikey.Verify(secret, "llmgw_original", digest) {
		t.Fatal("Verify() rejected exact key")
	}
	if apikey.Verify(secret, "llmgw_attacker", digest) {
		t.Fatal("Verify() accepted different key")
	}
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}
