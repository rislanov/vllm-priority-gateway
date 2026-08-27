package proxy

import (
	"strings"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestUsageInspectorNormalizesOrdinaryJSONAtEverySplit(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    domain.TokenUsage
	}{
		{
			name: "chat completions with reordered fields and escaped content",
			payload: `{"choices":[{"message":{"content":"escaped } brace and \"usage\" text"}}],` +
				`"usage":{"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":3},"prompt_tokens":12}}`,
			want: domain.TokenUsage{InputTokens: 12, OutputTokens: 5, CacheReadTokens: int64Pointer(3)},
		},
		{
			name: "responses completed event with nested usage",
			payload: `{"type":"response.completed","response":{"output":[],"usage":` +
				`{"output_tokens":8,"input_tokens_details":{"cached_tokens":2},"input_tokens":21}}}`,
			want: domain.TokenUsage{InputTokens: 21, OutputTokens: 8, CacheReadTokens: int64Pointer(2)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for split := 0; split <= len(test.payload); split++ {
				inspector := newUsageInspector("application/json; charset=utf-8")
				inspector.Write([]byte(test.payload[:split]))
				inspector.Write([]byte(test.payload[split:]))
				got, format, failed := inspector.Result()
				if failed || format != "" || got == nil || !equalTokenUsage(*got, test.want) {
					t.Fatalf("split %d: usage=%+v format=%q failed=%v, want %+v", split, got, format, failed, test.want)
				}
			}
		})
	}
}

func TestUsageInspectorValidatesOrdinaryJSONNumbers(t *testing.T) {
	tests := []struct {
		name       string
		payload    string
		want       *domain.TokenUsage
		wantFailed bool
	}{
		{name: "missing cache remains unavailable", payload: `{"usage":{"prompt_tokens":9,"completion_tokens":4}}`, want: &domain.TokenUsage{InputTokens: 9, OutputTokens: 4}},
		{name: "cache larger than input discards cache only", payload: `{"usage":{"prompt_tokens":9,"completion_tokens":4,"prompt_tokens_details":{"cached_tokens":10}}}`, want: &domain.TokenUsage{InputTokens: 9, OutputTokens: 4}, wantFailed: true},
		{name: "missing input", payload: `{"usage":{"completion_tokens":4}}`, wantFailed: true},
		{name: "missing output", payload: `{"usage":{"input_tokens":9}}`, wantFailed: true},
		{name: "negative", payload: `{"usage":{"prompt_tokens":-1,"completion_tokens":4}}`, wantFailed: true},
		{name: "fractional", payload: `{"usage":{"prompt_tokens":9.5,"completion_tokens":4}}`, wantFailed: true},
		{name: "overflow", payload: `{"usage":{"prompt_tokens":9223372036854775808,"completion_tokens":4}}`, wantFailed: true},
		{name: "quoted number", payload: `{"usage":{"prompt_tokens":"9","completion_tokens":4}}`, wantFailed: true},
		{name: "usage absent is not failure", payload: `{"choices":[]}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inspector := newUsageInspector("application/json")
			inspector.Write([]byte(test.payload))
			got, format, failed := inspector.Result()
			if failed != test.wantFailed {
				t.Fatalf("failed=%v, want %v (format=%q usage=%+v)", failed, test.wantFailed, format, got)
			}
			if failed && format != "json" {
				t.Fatalf("format=%q, want json", format)
			}
			if !failed && format != "" {
				t.Fatalf("format=%q without failure", format)
			}
			if test.want == nil {
				if got != nil {
					t.Fatalf("usage=%+v, want nil", got)
				}
				return
			}
			if got == nil || !equalTokenUsage(*got, *test.want) {
				t.Fatalf("usage=%+v, want %+v", got, test.want)
			}
		})
	}
}

func TestUsageInspectorParsesSSEAtEverySplit(t *testing.T) {
	tests := []struct {
		name       string
		stream     string
		want       domain.TokenUsage
		wantFailed bool
	}{
		{
			name: "LF chat stream with comments keepalive multiple data lines and done",
			stream: ": keep-alive\n\n\n" +
				"data: {\"choices\":[],\"usage\":\n" +
				"data: {\"completion_tokens\":6,\"prompt_tokens_details\":{\"cached_tokens\":4},\"prompt_tokens\":14}}\n\n" +
				"data: [DONE]\n\n",
			want: domain.TokenUsage{InputTokens: 14, OutputTokens: 6, CacheReadTokens: int64Pointer(4)},
		},
		{
			name: "CRLF responses completion stream",
			stream: "event: response.completed\r\n" +
				"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":17,\"output_tokens\":7}}}\r\n\r\n",
			want: domain.TokenUsage{InputTokens: 17, OutputTokens: 7},
		},
		{
			name: "malformed event does not prevent later final usage",
			stream: "data: {not-json}\n\n" +
				"data: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\n",
			want:       domain.TokenUsage{InputTokens: 3, OutputTokens: 2},
			wantFailed: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for split := 0; split <= len(test.stream); split++ {
				inspector := newUsageInspector("text/event-stream; charset=utf-8")
				inspector.Write([]byte(test.stream[:split]))
				inspector.Write([]byte(test.stream[split:]))
				got, format, failed := inspector.Result()
				if failed != test.wantFailed || got == nil || !equalTokenUsage(*got, test.want) {
					t.Fatalf("split %d: usage=%+v format=%q failed=%v, want %+v failed=%v", split, got, format, failed, test.want, test.wantFailed)
				}
				if failed && format != "sse" {
					t.Fatalf("split %d: format=%q, want sse", split, format)
				}
				if !failed && format != "" {
					t.Fatalf("split %d: unexpected format=%q", split, format)
				}
			}
		})
	}
}

func TestUsageInspectorDisablesAfterOversizedSSEEvent(t *testing.T) {
	stream := "data: \"" + strings.Repeat("x", maxUsageCaptureSize+1) + "\"\n\n" +
		"data: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\n"
	inspector := newUsageInspector("text/event-stream")
	inspector.Write([]byte(stream))
	got, format, failed := inspector.Result()
	if got != nil || !failed || format != "sse" {
		t.Fatalf("usage=%+v format=%q failed=%v", got, format, failed)
	}
}

func TestUsageInspectorDisablesAfterOversizedJSONUsage(t *testing.T) {
	payload := `{"usage":{"padding":"` + strings.Repeat("x", maxUsageCaptureSize+1) +
		`","prompt_tokens":3,"completion_tokens":2}}`
	inspector := newUsageInspector("application/json")
	inspector.Write([]byte(payload))
	got, format, failed := inspector.Result()
	if got != nil || !failed || format != "json" {
		t.Fatalf("usage=%+v format=%q failed=%v", got, format, failed)
	}
}

func equalTokenUsage(got, want domain.TokenUsage) bool {
	if got.InputTokens != want.InputTokens || got.OutputTokens != want.OutputTokens {
		return false
	}
	if got.CacheReadTokens == nil || want.CacheReadTokens == nil {
		return got.CacheReadTokens == nil && want.CacheReadTokens == nil
	}
	return *got.CacheReadTokens == *want.CacheReadTokens
}

func int64Pointer(value int64) *int64 { return &value }
