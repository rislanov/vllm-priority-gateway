package apikey

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"io"
)

const (
	keyPrefix        = "llmgw_"
	randomByteCount  = 32
	displayPrefixLen = 12
)

type Plaintext struct {
	Value  string `json:"-"`
	Prefix string `json:"-"`
}

func (p Plaintext) String() string {
	return p.Prefix + "…"
}

func (p Plaintext) GoString() string {
	return fmt.Sprintf("apikey.Plaintext{Prefix:%q}", p.Prefix)
}

func Generate(random io.Reader) (Plaintext, error) {
	entropy := make([]byte, randomByteCount)
	if _, err := io.ReadFull(random, entropy); err != nil {
		return Plaintext{}, fmt.Errorf("read API key entropy: %w", err)
	}
	value := keyPrefix + base64.RawURLEncoding.EncodeToString(entropy)
	return Plaintext{Value: value, Prefix: value[:displayPrefixLen]}, nil
}

func Digest(secret []byte, raw string) [sha256.Size]byte {
	mac := hmac.New(sha256.New, secret)
	_, _ = io.WriteString(mac, raw)
	var digest [sha256.Size]byte
	copy(digest[:], mac.Sum(nil))
	return digest
}

func Verify(secret []byte, raw string, expected [sha256.Size]byte) bool {
	actual := Digest(secret, raw)
	return subtle.ConstantTimeCompare(actual[:], expected[:]) == 1
}
