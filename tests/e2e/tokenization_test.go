package e2e_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestTokenizationWithRealVLLM(t *testing.T) {
	cfg := loadE2EConfig(t, modeSmoke)
	direct := os.Getenv("LLMGW_E2E_TOKENIZE_BACKEND_URL")
	upstreamModel := os.Getenv("LLMGW_E2E_TOKENIZE_UPSTREAM_MODEL")
	if direct == "" || upstreamModel == "" {
		t.Skip("set LLMGW_E2E_TOKENIZE_BACKEND_URL and LLMGW_E2E_TOKENIZE_UPSTREAM_MODEL for direct comparison")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	for _, payload := range []map[string]any{{"prompt": "Synthetic tokenization compatibility check.", "add_special_tokens": false, "return_token_strs": true}, {"messages": []map[string]string{{"role": "user", "content": "Synthetic chat check."}}, "add_generation_prompt": true, "return_token_strs": true}, {"prompt": "Synthetic special token check.", "add_special_tokens": true}} {
		call := func(base, model, key string) map[string]any {
			t.Helper()
			payload["model"] = model
			b, _ := json.Marshal(payload)
			r, e := http.NewRequest("POST", base+"/tokenize", bytes.NewReader(b))
			if e != nil {
				t.Fatal(e)
			}
			r.Header.Set("Content-Type", "application/json")
			if key != "" {
				r.Header.Set("Authorization", "Bearer "+key)
			}
			response, e := client.Do(r)
			if e != nil {
				t.Fatal(e)
			}
			defer response.Body.Close()
			if response.StatusCode != 200 {
				io.Copy(io.Discard, response.Body)
				t.Fatalf("tokenization returned HTTP %d", response.StatusCode)
			}
			var value map[string]any
			if e = json.NewDecoder(response.Body).Decode(&value); e != nil {
				t.Fatal(e)
			}
			return value
		}
		a := call(direct, upstreamModel, os.Getenv("LLMGW_E2E_TOKENIZE_BACKEND_KEY"))
		b := call(cfg.baseURL.String(), cfg.model, cfg.highKey)
		if !reflect.DeepEqual(a, b) {
			t.Fatal("direct and gateway tokenization differ (bodies deliberately omitted)")
		}
	}
	t.Log("prompt/chat/special-token/optional-token-string responses match direct vLLM")
}
