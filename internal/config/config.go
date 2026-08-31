package config

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type LookupFunc func(string) (string, bool)

type Config struct {
	ListenAddress              string
	DatabasePath               string
	AdminUsername              string
	AdminPassword              string
	APIKeyHMACSecret           []byte
	HealthInterval             time.Duration
	HealthTimeout              time.Duration
	MetricsInterval            time.Duration
	MetricsTimeout             time.Duration
	MetricsStaleAfter          time.Duration
	UnhealthyAfter             int
	RecoveryAfter              int
	CircuitFailureThreshold    int
	CircuitFailureWindow       time.Duration
	CircuitOpenCooldown        time.Duration
	CircuitHalfOpenMaxProbes   int
	QueueSoftLimit             float64
	KVSoftLimit                float64
	KVHardLimit                float64
	EWMAWindow                 time.Duration
	BusyThreshold              float64
	SaturatedThreshold         float64
	EmergencyThreshold         float64
	BusyRecoveryThreshold      float64
	SaturatedRecoveryThreshold float64
	EmergencyRecoveryThreshold float64
	OverloadEnterWindow        time.Duration
	OverloadRecoveryWindow     time.Duration
	RequestBodyLimit           int64
	RetryAfter                 time.Duration
	RoutingPressureEpsilon     float64
	SessionAffinityMaxPressure float64
	DialTimeout                time.Duration
	TLSHandshakeTimeout        time.Duration
	ResponseHeaderTimeout      time.Duration
	ShutdownGracePeriod        time.Duration
	AnalyticsRetention         time.Duration
}

func Load(lookup LookupFunc) (Config, error) {
	cfg := defaultConfig()

	cfg.ListenAddress = stringValue(lookup, "LLMGW_LISTEN_ADDRESS", cfg.ListenAddress)
	cfg.DatabasePath = stringValue(lookup, "LLMGW_DATABASE_PATH", cfg.DatabasePath)
	cfg.AdminUsername = stringValue(lookup, "LLMGW_ADMIN_USERNAME", "")
	cfg.AdminPassword = stringValue(lookup, "LLMGW_ADMIN_PASSWORD", "")
	cfg.APIKeyHMACSecret = []byte(stringValue(lookup, "LLMGW_API_KEY_HMAC_SECRET", ""))

	if err := cfg.loadHealth(lookup); err != nil {
		return Config{}, err
	}
	if err := cfg.loadCircuit(lookup); err != nil {
		return Config{}, err
	}
	if err := cfg.loadPressure(lookup); err != nil {
		return Config{}, err
	}
	if err := cfg.loadRequestPolicy(lookup); err != nil {
		return Config{}, err
	}
	if err := cfg.loadTransport(lookup); err != nil {
		return Config{}, err
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultConfig() Config {
	return Config{
		ListenAddress:              ":8080",
		DatabasePath:               "./data/llmgw.db",
		HealthInterval:             2 * time.Second,
		HealthTimeout:              time.Second,
		MetricsInterval:            time.Second,
		MetricsTimeout:             time.Second,
		MetricsStaleAfter:          5 * time.Second,
		UnhealthyAfter:             3,
		RecoveryAfter:              2,
		CircuitFailureThreshold:    5,
		CircuitFailureWindow:       30 * time.Second,
		CircuitOpenCooldown:        15 * time.Second,
		CircuitHalfOpenMaxProbes:   1,
		QueueSoftLimit:             2,
		KVSoftLimit:                .80,
		KVHardLimit:                .95,
		EWMAWindow:                 4 * time.Second,
		BusyThreshold:              .70,
		SaturatedThreshold:         1.00,
		EmergencyThreshold:         1.40,
		BusyRecoveryThreshold:      .55,
		SaturatedRecoveryThreshold: .85,
		EmergencyRecoveryThreshold: 1.20,
		OverloadEnterWindow:        3 * time.Second,
		OverloadRecoveryWindow:     10 * time.Second,
		RequestBodyLimit:           16 << 20,
		RetryAfter:                 2 * time.Second,
		RoutingPressureEpsilon:     .02,
		SessionAffinityMaxPressure: 1.00,
		DialTimeout:                3 * time.Second,
		TLSHandshakeTimeout:        3 * time.Second,
		ResponseHeaderTimeout:      30 * time.Second,
		ShutdownGracePeriod:        30 * time.Second,
		AnalyticsRetention:         2160 * time.Hour,
	}
}

func (c *Config) loadHealth(lookup LookupFunc) error {
	var err error
	if c.HealthInterval, err = durationValue(lookup, "LLMGW_HEALTH_INTERVAL", c.HealthInterval); err != nil {
		return err
	}
	if c.HealthTimeout, err = durationValue(lookup, "LLMGW_HEALTH_TIMEOUT", c.HealthTimeout); err != nil {
		return err
	}
	if c.MetricsInterval, err = durationValue(lookup, "LLMGW_METRICS_INTERVAL", c.MetricsInterval); err != nil {
		return err
	}
	if c.MetricsTimeout, err = durationValue(lookup, "LLMGW_METRICS_TIMEOUT", c.MetricsTimeout); err != nil {
		return err
	}
	if c.MetricsStaleAfter, err = durationValue(lookup, "LLMGW_METRICS_STALE_AFTER", c.MetricsStaleAfter); err != nil {
		return err
	}
	if c.UnhealthyAfter, err = intValue(lookup, "LLMGW_UNHEALTHY_AFTER", c.UnhealthyAfter); err != nil {
		return err
	}
	if c.RecoveryAfter, err = intValue(lookup, "LLMGW_RECOVERY_AFTER", c.RecoveryAfter); err != nil {
		return err
	}
	return nil
}

func (c *Config) loadCircuit(lookup LookupFunc) error {
	var err error
	if c.CircuitFailureThreshold, err = intValue(lookup, "LLMGW_CIRCUIT_FAILURE_THRESHOLD", c.CircuitFailureThreshold); err != nil {
		return err
	}
	if c.CircuitFailureWindow, err = durationValue(lookup, "LLMGW_CIRCUIT_FAILURE_WINDOW", c.CircuitFailureWindow); err != nil {
		return err
	}
	if c.CircuitOpenCooldown, err = durationValue(lookup, "LLMGW_CIRCUIT_OPEN_COOLDOWN", c.CircuitOpenCooldown); err != nil {
		return err
	}
	if c.CircuitHalfOpenMaxProbes, err = intValue(lookup, "LLMGW_CIRCUIT_HALF_OPEN_MAX_PROBES", c.CircuitHalfOpenMaxProbes); err != nil {
		return err
	}
	return nil
}

func (c *Config) loadPressure(lookup LookupFunc) error {
	var err error
	if c.QueueSoftLimit, err = floatValue(lookup, "LLMGW_QUEUE_SOFT_LIMIT", c.QueueSoftLimit); err != nil {
		return err
	}
	if c.KVSoftLimit, err = floatValue(lookup, "LLMGW_KV_SOFT_LIMIT", c.KVSoftLimit); err != nil {
		return err
	}
	if c.KVHardLimit, err = floatValue(lookup, "LLMGW_KV_HARD_LIMIT", c.KVHardLimit); err != nil {
		return err
	}
	if c.EWMAWindow, err = durationValue(lookup, "LLMGW_EWMA_WINDOW", c.EWMAWindow); err != nil {
		return err
	}
	if c.BusyThreshold, err = floatValue(lookup, "LLMGW_BUSY_THRESHOLD", c.BusyThreshold); err != nil {
		return err
	}
	if c.SaturatedThreshold, err = floatValue(lookup, "LLMGW_SATURATED_THRESHOLD", c.SaturatedThreshold); err != nil {
		return err
	}
	if c.EmergencyThreshold, err = floatValue(lookup, "LLMGW_EMERGENCY_THRESHOLD", c.EmergencyThreshold); err != nil {
		return err
	}
	if c.BusyRecoveryThreshold, err = floatValue(lookup, "LLMGW_BUSY_RECOVERY_THRESHOLD", c.BusyRecoveryThreshold); err != nil {
		return err
	}
	if c.SaturatedRecoveryThreshold, err = floatValue(lookup, "LLMGW_SATURATED_RECOVERY_THRESHOLD", c.SaturatedRecoveryThreshold); err != nil {
		return err
	}
	if c.EmergencyRecoveryThreshold, err = floatValue(lookup, "LLMGW_EMERGENCY_RECOVERY_THRESHOLD", c.EmergencyRecoveryThreshold); err != nil {
		return err
	}
	if c.OverloadEnterWindow, err = durationValue(lookup, "LLMGW_OVERLOAD_ENTER_WINDOW", c.OverloadEnterWindow); err != nil {
		return err
	}
	if c.OverloadRecoveryWindow, err = durationValue(lookup, "LLMGW_OVERLOAD_RECOVERY_WINDOW", c.OverloadRecoveryWindow); err != nil {
		return err
	}
	return nil
}

func (c *Config) loadRequestPolicy(lookup LookupFunc) error {
	var err error
	if c.RequestBodyLimit, err = int64Value(lookup, "LLMGW_REQUEST_BODY_LIMIT", c.RequestBodyLimit); err != nil {
		return err
	}
	if c.RetryAfter, err = durationValue(lookup, "LLMGW_RETRY_AFTER", c.RetryAfter); err != nil {
		return err
	}
	if c.RoutingPressureEpsilon, err = floatValue(lookup, "LLMGW_ROUTING_PRESSURE_EPSILON", c.RoutingPressureEpsilon); err != nil {
		return err
	}
	if c.SessionAffinityMaxPressure, err = floatValue(lookup, "LLMGW_SESSION_AFFINITY_MAX_PRESSURE", c.SessionAffinityMaxPressure); err != nil {
		return err
	}
	return nil
}

func (c *Config) loadTransport(lookup LookupFunc) error {
	var err error
	if c.DialTimeout, err = durationValue(lookup, "LLMGW_DIAL_TIMEOUT", c.DialTimeout); err != nil {
		return err
	}
	if c.TLSHandshakeTimeout, err = durationValue(lookup, "LLMGW_TLS_HANDSHAKE_TIMEOUT", c.TLSHandshakeTimeout); err != nil {
		return err
	}
	if c.ResponseHeaderTimeout, err = durationValue(lookup, "LLMGW_RESPONSE_HEADER_TIMEOUT", c.ResponseHeaderTimeout); err != nil {
		return err
	}
	if c.ShutdownGracePeriod, err = durationValue(lookup, "LLMGW_SHUTDOWN_GRACE_PERIOD", c.ShutdownGracePeriod); err != nil {
		return err
	}
	if c.AnalyticsRetention, err = durationValue(lookup, "LLMGW_ANALYTICS_RETENTION", c.AnalyticsRetention); err != nil {
		return err
	}
	return nil
}

func (c Config) validate() error {
	if strings.TrimSpace(c.AdminUsername) == "" {
		return errors.New("LLMGW_ADMIN_USERNAME is required")
	}
	if len(c.AdminPassword) < 16 {
		return errors.New("LLMGW_ADMIN_PASSWORD must contain at least 16 bytes")
	}
	if len(c.APIKeyHMACSecret) < 32 {
		return errors.New("LLMGW_API_KEY_HMAC_SECRET must contain at least 32 bytes")
	}
	if strings.TrimSpace(c.ListenAddress) == "" || strings.TrimSpace(c.DatabasePath) == "" {
		return errors.New("listen address and database path are required")
	}
	for name, value := range map[string]time.Duration{
		"health interval": c.HealthInterval, "health timeout": c.HealthTimeout,
		"metrics interval": c.MetricsInterval, "metrics timeout": c.MetricsTimeout,
		"metrics stale after": c.MetricsStaleAfter, "EWMA window": c.EWMAWindow,
		"circuit failure window": c.CircuitFailureWindow, "circuit open cooldown": c.CircuitOpenCooldown,
		"overload enter window": c.OverloadEnterWindow, "overload recovery window": c.OverloadRecoveryWindow,
		"retry after": c.RetryAfter, "dial timeout": c.DialTimeout,
		"TLS handshake timeout": c.TLSHandshakeTimeout, "response header timeout": c.ResponseHeaderTimeout,
		"shutdown grace period": c.ShutdownGracePeriod,
	} {
		if value <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if c.UnhealthyAfter <= 0 || c.RecoveryAfter <= 0 {
		return errors.New("health transition counts must be positive")
	}
	if c.AnalyticsRetention < 0 {
		return errors.New("analytics retention must be non-negative")
	}
	if c.CircuitFailureThreshold <= 0 || c.CircuitHalfOpenMaxProbes <= 0 {
		return errors.New("circuit breaker counts must be positive")
	}
	if !finitePositive(c.QueueSoftLimit) {
		return errors.New("queue soft limit must be positive")
	}
	if math.IsNaN(c.KVSoftLimit) || math.IsInf(c.KVSoftLimit, 0) ||
		math.IsNaN(c.KVHardLimit) || math.IsInf(c.KVHardLimit, 0) ||
		c.KVSoftLimit < 0 || c.KVHardLimit > 1 || c.KVSoftLimit >= c.KVHardLimit {
		return errors.New("KV limits must satisfy 0 <= soft < hard <= 1")
	}
	for _, threshold := range []float64{
		c.BusyRecoveryThreshold,
		c.BusyThreshold,
		c.SaturatedRecoveryThreshold,
		c.SaturatedThreshold,
		c.EmergencyRecoveryThreshold,
		c.EmergencyThreshold,
	} {
		if !finitePositive(threshold) {
			return errors.New("pool thresholds and recovery thresholds must be finite and positive")
		}
	}
	if !(c.BusyRecoveryThreshold < c.BusyThreshold && c.BusyThreshold < c.SaturatedRecoveryThreshold && c.SaturatedRecoveryThreshold < c.SaturatedThreshold && c.SaturatedThreshold < c.EmergencyRecoveryThreshold && c.EmergencyRecoveryThreshold < c.EmergencyThreshold) {
		return errors.New("pool thresholds and recovery thresholds are out of order")
	}
	if c.RequestBodyLimit < 1024 {
		return errors.New("request body limit must be at least 1024 bytes")
	}
	if c.RoutingPressureEpsilon < 0 || math.IsNaN(c.RoutingPressureEpsilon) || math.IsInf(c.RoutingPressureEpsilon, 0) {
		return errors.New("routing pressure epsilon must be finite and non-negative")
	}
	if !finitePositive(c.SessionAffinityMaxPressure) {
		return errors.New("session affinity max pressure must be finite and positive")
	}
	return nil
}

func stringValue(lookup LookupFunc, key, fallback string) string {
	if value, ok := lookup(key); ok {
		return value
	}
	return fallback
}

func durationValue(lookup LookupFunc, key string, fallback time.Duration) (time.Duration, error) {
	value, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

func intValue(lookup LookupFunc, key string, fallback int) (int, error) {
	value, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

func int64Value(lookup LookupFunc, key string, fallback int64) (int64, error) {
	value, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

func floatValue(lookup LookupFunc, key string, fallback float64) (float64, error) {
	value, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
