package config

import (
	"testing"
	"time"
)

func TestRequestSizeEstimatorConfig(t *testing.T) {
	cfg := defaultConfig()
	if cfg.AdmissionEstimateTimeout != 2*time.Second || cfg.AdmissionEstimateMaxResponseBytes != 8<<20 {
		t.Fatal("unexpected estimation defaults")
	}
	for _, setting := range []struct{ name, value string }{
		{"LLMGW_ADMISSION_ESTIMATE_TIMEOUT", "0s"},
		{"LLMGW_ADMISSION_ESTIMATE_TIMEOUT", "31s"},
		{"LLMGW_ADMISSION_ESTIMATE_MAX_RESPONSE_BYTES", "1023"},
		{"LLMGW_ADMISSION_ESTIMATE_MAX_RESPONSE_BYTES", "67108865"},
	} {
		t.Run(setting.name+setting.value, func(t *testing.T) {
			cfg := defaultConfig()
			if err := cfg.loadRequestSize(func(k string) (string, bool) { return setting.value, k == setting.name }); err == nil {
				t.Fatal("accepted an out-of-range setting")
			}
		})
	}
}
