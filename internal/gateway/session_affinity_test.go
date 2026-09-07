package gateway

import (
	"net/http"
	"strings"
	"testing"
)

func TestSessionAffinityID(t *testing.T) {
	for i, name := range sessionAffinityHeaders {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				values  []string
				want    string
				invalid bool
			}{
				{"trim", []string{" \talpha \t"}, "alpha", false},
				{"empty", []string{"", " \t"}, "", false},
				{"boundary", []string{strings.Repeat("s", 256)}, strings.Repeat("s", 256), false},
				{"oversized", []string{strings.Repeat("s", 257)}, "", true},
				{"byte limit", []string{strings.Repeat("я", 129)}, "", true},
				{"identical duplicates", []string{"alpha", " alpha "}, "alpha", false},
				{"conflicting duplicates", []string{"alpha", "beta"}, "", true},
				{"oversized duplicate", []string{"alpha", strings.Repeat("s", 257)}, "", true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					h := make(http.Header)
					for _, value := range tc.values {
						h.Add(strings.ToLower(name), value)
					}
					got, err := sessionAffinityID(h)
					if got != tc.want || (err != nil) != tc.invalid {
						t.Fatalf("got %q, error %v; want %q, invalid %v", got, err, tc.want, tc.invalid)
					}
				})
			}
			// Every earlier nonempty alias wins, but ignored aliases are still validated.
			for _, earlier := range sessionAffinityHeaders[:i] {
				h := make(http.Header)
				h.Set(earlier, "override")
				h.Set(name, "fallback")
				if got, err := sessionAffinityID(h); err != nil || got != "override" {
					t.Fatalf("precedence %s over %s: %q, %v", earlier, name, got, err)
				}
				h.Set(earlier, " \t")
				if got, err := sessionAffinityID(h); err != nil || got != "fallback" {
					t.Fatalf("empty fallback: %q, %v", got, err)
				}
				h.Set(earlier, "override")
				h.Set(name, strings.Repeat("s", 257))
				if _, err := sessionAffinityID(h); err == nil {
					t.Fatal("ignored oversized alias accepted")
				}
			}
		})
	}
	if got, err := sessionAffinityID(nil); err != nil || got != "" {
		t.Fatalf("no headers: %q, %v", got, err)
	}
}

func TestPiClientRequestIDFallback(t *testing.T) {
	t.Run("Pi client uses and strips fallback", func(t *testing.T) {
		h := http.Header{"User-Agent": {"pi (linux 6.0; x64)"}, piSessionAffinityFallbackHeader: {" alpha "}}
		if got, err := sessionAffinityID(h); err != nil || got != "alpha" {
			t.Fatalf("session ID = %q, error %v", got, err)
		}
		stripSessionAffinityHeaders(h)
		if got := h.Get(piSessionAffinityFallbackHeader); got != "" {
			t.Fatalf("Pi fallback was not stripped: %q", got)
		}
	})

	t.Run("generic client preserves request ID", func(t *testing.T) {
		h := http.Header{"User-Agent": {"other-client/1.0"}, piSessionAffinityFallbackHeader: {"request-1"}}
		if got, err := sessionAffinityID(h); err != nil || got != "" {
			t.Fatalf("session ID = %q, error %v", got, err)
		}
		stripSessionAffinityHeaders(h)
		if got := h.Get(piSessionAffinityFallbackHeader); got != "request-1" {
			t.Fatalf("generic request ID = %q", got)
		}
	})

	t.Run("Pi fallback is validated", func(t *testing.T) {
		h := http.Header{"User-Agent": {"PI (browser)"}, piSessionAffinityFallbackHeader: {strings.Repeat("s", 257)}}
		if _, err := sessionAffinityID(h); err == nil {
			t.Fatal("oversized Pi fallback accepted")
		}
	})
}
