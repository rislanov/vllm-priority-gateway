package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"unicode/utf8"
)

type IDGenerator func() (string, error)

func generateRequestID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func bearerToken(value string) (string, error) {
	parts := strings.Fields(value)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", errors.New("missing bearer token")
	}
	return parts[1], nil
}

func validParentRequestID(value string) string {
	if value == "" || len(value) > 128 || !utf8.ValidString(value) {
		return ""
	}
	for _, character := range value {
		if character < 0x20 || character > 0x7e {
			return ""
		}
	}
	return value
}
