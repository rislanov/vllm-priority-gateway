package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/config"
)

func validEnvironment() map[string]string {
	return map[string]string{
		"LLMGW_ADMIN_USERNAME":      "admin",
		"LLMGW_ADMIN_PASSWORD":      "correct horse battery staple",
		"LLMGW_API_KEY_HMAC_SECRET": strings.Repeat("s", 32),
	}
}

func lookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func TestLoadUsesMVPDefaults(t *testing.T) {
	cfg, err := config.Load(lookup(validEnvironment()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.ListenAddress != ":8080" {
		t.Fatalf("ListenAddress = %q", cfg.ListenAddress)
	}
	if cfg.DatabasePath != "./data/llmgw.db" {
		t.Fatalf("DatabasePath = %q", cfg.DatabasePath)
	}
	if cfg.HealthInterval != 2*time.Second || cfg.MetricsInterval != time.Second {
		t.Fatalf("poll intervals = %s/%s", cfg.HealthInterval, cfg.MetricsInterval)
	}
	if cfg.MetricsStaleAfter != 5*time.Second || cfg.EWMAWindow != 4*time.Second {
		t.Fatalf("stale/window = %s/%s", cfg.MetricsStaleAfter, cfg.EWMAWindow)
	}
	if cfg.QueueSoftLimit != 2 || cfg.KVSoftLimit != 0.80 || cfg.KVHardLimit != 0.95 {
		t.Fatalf("pressure defaults = %+v", cfg)
	}
	if cfg.RequestBodyLimit != 16<<20 || cfg.RetryAfter != 2*time.Second {
		t.Fatalf("HTTP defaults = %+v", cfg)
	}
}

func TestLoadRejectsMissingOrWeakSecrets(t *testing.T) {
	tests := []struct {
		name string
		edit func(map[string]string)
	}{
		{name: "missing admin username", edit: func(env map[string]string) { delete(env, "LLMGW_ADMIN_USERNAME") }},
		{name: "missing admin password", edit: func(env map[string]string) { delete(env, "LLMGW_ADMIN_PASSWORD") }},
		{name: "short HMAC secret", edit: func(env map[string]string) { env["LLMGW_API_KEY_HMAC_SECRET"] = "short" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := validEnvironment()
			tt.edit(env)
			if _, err := config.Load(lookup(env)); err == nil {
				t.Fatal("Load() unexpectedly succeeded")
			}
		})
	}
}

func TestLoadRejectsInvalidThresholdOrder(t *testing.T) {
	env := validEnvironment()
	env["LLMGW_KV_SOFT_LIMIT"] = "0.96"
	env["LLMGW_KV_HARD_LIMIT"] = "0.95"
	if _, err := config.Load(lookup(env)); err == nil {
		t.Fatal("Load() unexpectedly accepted KV soft limit above hard limit")
	}
}

func TestLoadAppliesEnvironmentOverrides(t *testing.T) {
	env := validEnvironment()
	env["LLMGW_LISTEN_ADDRESS"] = "127.0.0.1:9090"
	env["LLMGW_HEALTH_INTERVAL"] = "750ms"
	env["LLMGW_REQUEST_BODY_LIMIT"] = "2097152"

	cfg, err := config.Load(lookup(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ListenAddress != "127.0.0.1:9090" || cfg.HealthInterval != 750*time.Millisecond || cfg.RequestBodyLimit != 2<<20 {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
}
