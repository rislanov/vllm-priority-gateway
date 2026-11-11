package proxy

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestDecodeTokenEstimateBounds(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		limit      int64
		ok         bool
	}{
		{"valid", `{"count":2,"max_model_len":4096,"tokens":[1,2]}`, 256, true},
		{"missing count", `{"max_model_len":4096}`, 256, false},
		{"null count", `{"count":null,"max_model_len":4096}`, 256, false},
		{"noninteger count", `{"count":2.5,"max_model_len":4096}`, 256, false},
		{"inconsistent IDs", `{"count":2,"max_model_len":4096,"tokens":[1]}`, 256, false},
		{"truncated", `{"count":2,"max_model_len":4096`, 256, false},
		{"oversized", `{"count":2,"max_model_len":4096,"tokens":[1,2]}`, 16, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(tc.body))}
			count, contextLimit, err := DecodeTokenEstimate(response, tc.limit)
			if tc.ok {
				if err != nil || count != 2 || contextLimit != 4096 {
					t.Fatalf("decoded %d/%d, err=%v", count, contextLimit, err)
				}
			} else if err == nil {
				t.Fatal("accepted invalid tokenization response")
			}
		})
	}
}
