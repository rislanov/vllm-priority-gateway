package proxy

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

var errInvalidTokenEstimate = errors.New("invalid tokenizer estimate")

// DecodeTokenEstimate reads a bounded tokenizer response. The caller owns the body.
func DecodeTokenEstimate(response *http.Response, maxBytes int64) (count, contextLimit int64, err error) {
	if response.Body == nil || maxBytes < 1 || maxBytes > 64<<20 {
		return 0, 0, errInvalidTokenEstimate
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil || int64(len(body)) > maxBytes {
		return 0, 0, errInvalidTokenEstimate
	}
	var result struct {
		Count        json.RawMessage `json:"count"`
		ContextLimit json.RawMessage `json:"max_model_len"`
		Tokens       json.RawMessage `json:"tokens"`
	}
	if json.Unmarshal(body, &result) != nil {
		return 0, 0, errInvalidTokenEstimate
	}
	count, err = strconv.ParseInt(string(result.Count), 10, 64)
	if err != nil || count < 0 || count > domain.MaxRequestSizeTokens {
		return 0, 0, errInvalidTokenEstimate
	}
	contextLimit, err = strconv.ParseInt(string(result.ContextLimit), 10, 64)
	if err != nil || contextLimit <= 0 || contextLimit > domain.MaxRequestSizeTokens {
		return 0, 0, errInvalidTokenEstimate
	}
	if len(result.Tokens) != 0 {
		var tokens []int64
		if json.Unmarshal(result.Tokens, &tokens) != nil || tokens == nil || int64(len(tokens)) != count {
			return 0, 0, errInvalidTokenEstimate
		}
	}
	return count, contextLimit, nil
}
