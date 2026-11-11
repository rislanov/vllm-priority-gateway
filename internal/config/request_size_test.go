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

func TestEmergencyEstimatorConfig(t *testing.T) {
	cfg := defaultConfig()
	if err := cfg.loadRequestSize(func(key string) (string, bool) {
		switch key {
		case "LLMGW_ADMISSION_ESTIMATE_TIMEOUT":
			return "1s", true
		case "LLMGW_ADMISSION_EMERGENCY_ESTIMATE_CLIENT_RPM":
			return "45", true
		default:
			return "", false
		}
	}); err != nil {
		t.Fatal(err)
	}
	p := cfg.EmergencyTokenization
	if p.ClientMaxInflight != 1 || p.BackendMaxInflight != 1 || p.ClientRPM != 45 || p.Timeout != time.Second {
		t.Fatalf("emergency tokenizer policy = %+v", p)
	}
	if err := cfg.loadRequestSize(func(key string) (string, bool) {
		return "0", key == "LLMGW_ADMISSION_EMERGENCY_ESTIMATE_BACKEND_MAX_INFLIGHT"
	}); err == nil {
		t.Fatal("accepted a zero emergency backend limit")
	}
}
