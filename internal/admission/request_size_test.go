package admission

import (
	"encoding/json"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestPrepareRequestSizeEndpoints(t *testing.T) {
	policy := domain.EffectiveRequestSizePolicy{
		Enabled:      true,
		Limits:       domain.RequestSizeLimits{MaxInputTokens: 8192, MaxOutputTokens: 2048, DefaultOutputTokens: 512},
		FallbackMode: domain.TokenFallbackUTF8Bytes, FallbackOverheadTokens: 64,
	}
	for _, tc := range []struct {
		name, path, body, outputKey string
		want                        int64
	}{
		{"chat default", "/v1/chat/completions", `{"messages":[{"role":"user","content":"hi"}]}`, "max_completion_tokens", 512},
		{"chat aliases", "/v1/chat/completions", `{"messages":[{"role":"user","content":"hi"}],"max_tokens":128,"max_completion_tokens":128}`, "max_completion_tokens", 128},
		{"completion stream default", "/v1/completions", `{"prompt":"hi","stream":true,"max_tokens":null}`, "max_tokens", 512},
		{"responses explicit", "/v1/responses", `{"input":"hi","max_output_tokens":128}`, "max_output_tokens", 128},
		{"too large", "/v1/completions", `{"prompt":"hi","max_tokens":2049}`, "", 0},
		{"conflicting chat aliases", "/v1/chat/completions", `{"messages":[{"role":"user","content":"hi"}],"max_tokens":128,"max_completion_tokens":64}`, "", 0},
		{"wrong alias", "/v1/responses", `{"input":"hi","max_tokens":128}`, "", 0},
		{"minimum above cap", "/v1/completions", `{"prompt":"hi","max_tokens":16,"min_tokens":17}`, "", 0},
		{"coerced numeric cap", "/v1/completions", `{"prompt":"hi","max_tokens":"16"}`, "", 0},
		{"unknown field", "/v1/chat/completions", `{"messages":[{"role":"user","content":"hi"}],"other_context":"hidden"}`, "", 0},
		{"mixed batch", "/v1/completions", `{"prompt":["hi",[1]]}`, "", 0},
		{"responses hidden history", "/v1/responses", `{"input":"hi","previous_response_id":"response-123"}`, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PrepareRequestSize(tc.path, []byte(tc.body), policy)
			if tc.outputKey == "" {
				if err == nil {
					t.Fatal("invalid payload accepted")
				}
				return
			}
			if err != nil || got.OutputAllowance != tc.want {
				t.Fatalf("output allowance = %d, err = %v; want %d", got.OutputAllowance, err, tc.want)
			}
			var forwarded map[string]json.RawMessage
			if err := json.Unmarshal(got.Payload, &forwarded); err != nil {
				t.Fatal(err)
			}
			var cap int64
			if err := json.Unmarshal(forwarded[tc.outputKey], &cap); err != nil || cap != tc.want {
				t.Fatalf("forwarded cap = %d, err = %v; want %d", cap, err, tc.want)
			}
			if tc.outputKey == "max_completion_tokens" && forwarded["max_tokens"] != nil {
				t.Fatal("legacy alias remains in forwarded payload")
			}
		})
	}
}

func TestReservationTokensMultipliers(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		inputs                []int64
		output, samples, want int64
	}{
		{"batch and samples", []int64{100, 200}, 50, 2, 800},
		{"empty input still has a positive reservation", []int64{0}, 50, 1, 51},
		{"zero samples", []int64{100}, 50, 0, 0},
		{"too many samples", []int64{100}, 50, 33, 0},
		{"too many prompts", make([]int64, 33), 50, 1, 0},
		{"reservation above bound", []int64{domain.MaxRequestSizeTokens}, 1, 1, 0},
		{"multiplication above bound", []int64{domain.MaxRequestSizeTokens / 2}, 1, 2, 0},
		{"int64 overflow rejected", []int64{9223372036854775807}, 1, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReservationTokens(tc.inputs, tc.output, tc.samples)
			if tc.want == 0 {
				if err == nil {
					t.Fatalf("invalid reservation accepted: %d", got)
				}
			} else if err != nil || got != tc.want {
				t.Fatalf("reservation = %d, err = %v; want %d", got, err, tc.want)
			}
		})
	}
}

func TestEstimatorCompatibility(t *testing.T) {
	policy := domain.EffectiveRequestSizePolicy{
		Enabled: true, FallbackMode: domain.TokenFallbackUTF8Bytes, FallbackOverheadTokens: 64,
		Limits: domain.RequestSizeLimits{MaxInputTokens: 8192, MaxOutputTokens: 2048, DefaultOutputTokens: 512},
	}
	for _, tc := range []struct {
		name, path, body, canonical string
		fallback, reject            bool
		tokenIDs                    int64
	}{
		{
			name: "chat preserves Unicode and formatting options", path: "/v1/chat/completions",
			body:      `{"messages":[{"role":"assistant","content":[{"type":"text","text":"é <tag>"}]}],"continue_final_message":true,"chat_template":"template","chat_template_kwargs":{"argument":7}}`,
			canonical: `{"add_generation_prompt":false,"add_special_tokens":false,"chat_template":"template","chat_template_kwargs":{"argument":7},"continue_final_message":true,"messages":[{"content":[{"text":"é <tag>","type":"text"}],"role":"assistant"}]}`,
		},
		{
			name: "chat schema uses fallback", path: "/v1/chat/completions", fallback: true,
			body:      `{"messages":[{"role":"user","content":"hi"}],"response_format":{"type":"json_object"}}`,
			canonical: `{"add_generation_prompt":true,"add_special_tokens":false,"continue_final_message":false,"messages":[{"content":"hi","role":"user"}],"response_format":{"type":"json_object"}}`,
		},
		{
			name: "legacy completion preserves tokenizer options", path: "/v1/completions",
			body: `{"prompt":"<&","add_special_tokens":false}`, canonical: `{"add_special_tokens":false,"prompt":"<&"}`,
		},
		{
			name: "token IDs are one prompt", path: "/v1/completions", tokenIDs: 3,
			body: `{"prompt":[0,42,123]}`, canonical: `{"add_special_tokens":true,"prompt":[0,42,123]}`,
		},
		{
			name: "Responses counts instructions and inline tool history", path: "/v1/responses", fallback: true,
			body:      `{"instructions":"Be brief","input":[{"type":"function_call_output","call_id":"id","output":"result"}],"tools":[{"type":"function","name":"lookup"}],"background":false}`,
			canonical: `{"input":[{"call_id":"id","output":"result","type":"function_call_output"}],"instructions":"Be brief","tools":[{"name":"lookup","type":"function"}]}`,
		},
		{name: "media rejected", path: "/v1/chat/completions", body: `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.test/image"}}]}]}`, reject: true},
		{name: "server-side tools rejected", path: "/v1/responses", body: `{"input":"hi","tools":[{"type":"web_search"}]}`, reject: true},
		{name: "remote chat tools rejected", path: "/v1/chat/completions", body: `{"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"mcp","server_url":"https://example.test"}]}`, reject: true},
		{name: "background rejected", path: "/v1/responses", body: `{"input":"hi","background":true}`, reject: true},
		{name: "truncation rejected", path: "/v1/responses", body: `{"input":"hi","truncation":"auto"}`, reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PrepareRequestSize(tc.path, []byte(tc.body), policy)
			if tc.reject {
				if err == nil {
					t.Fatal("unsupported input accepted")
				}
				return
			}
			if err != nil || len(got.Inputs) != 1 || string(got.Inputs[0]) != tc.canonical || got.UseFallback != tc.fallback {
				t.Fatalf("inputs=%s fallback=%v err=%v", got.Inputs, got.UseFallback, err)
			}
			if tc.tokenIDs > 0 && (len(got.TokenIDs) != 1 || got.TokenIDs[0] != tc.tokenIDs) {
				t.Fatalf("token IDs count=%v, want %d", got.TokenIDs, tc.tokenIDs)
			}
			if tc.fallback {
				reject := policy
				reject.FallbackMode = domain.TokenFallbackReject
				if _, err := PrepareRequestSize(tc.path, []byte(tc.body), reject); err == nil {
					t.Fatal("unsupported exact input accepted with fallback disabled")
				}
			}
		})
	}
}
