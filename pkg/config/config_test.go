package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_WithSASLAndTLS(t *testing.T) {
	// Create a temporary config file with SASL and TLS settings
	configContent := `
kafka:
  broker: "test.servicebus.windows.net:9093"
  source_topic: "test-events"
  dest_topic: "test-output"
  group_id: "$Default"
  sasl:
    enabled: true
    mechanism: "PLAIN"
    username: "$ConnectionString"
    password: "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test-key"
  tls:
    enabled: true

mqtt:
  broker: "localhost"
  port: 1883
  client_id: "test-client"
  dest_topic: "mqtt/events"

bridge:
  name: "test-bridge"
  log_level: "debug"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error: %v", err)
	}

	// Verify SASL settings
	if !cfg.Kafka.SASL.Enabled {
		t.Error("SASL should be enabled")
	}
	if cfg.Kafka.SASL.Mechanism != "PLAIN" {
		t.Errorf("SASL mechanism = %q, want %q", cfg.Kafka.SASL.Mechanism, "PLAIN")
	}
	if cfg.Kafka.SASL.Username != "$ConnectionString" {
		t.Errorf("SASL username = %q, want %q", cfg.Kafka.SASL.Username, "$ConnectionString")
	}
	if cfg.Kafka.SASL.Password == "" {
		t.Error("SASL password should not be empty")
	}

	// Verify TLS settings
	if !cfg.Kafka.TLS.Enabled {
		t.Error("TLS should be enabled")
	}

	// Verify other settings
	if cfg.Kafka.Broker != "test.servicebus.windows.net:9093" {
		t.Errorf("Broker = %q, want %q", cfg.Kafka.Broker, "test.servicebus.windows.net:9093")
	}
	if cfg.Kafka.GroupID != "$Default" {
		t.Errorf("GroupID = %q, want %q", cfg.Kafka.GroupID, "$Default")
	}
}

func TestLoadConfig_WithoutSASL(t *testing.T) {
	// Create a temporary config file without SASL settings
	configContent := `
kafka:
  broker: "localhost:9092"
  source_topic: "events"

mqtt:
  broker: "localhost"
  port: 1883

bridge:
  name: "test-bridge"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error: %v", err)
	}

	// Verify SASL is disabled by default
	if cfg.Kafka.SASL.Enabled {
		t.Error("SASL should be disabled by default")
	}

	// Verify TLS is disabled by default
	if cfg.Kafka.TLS.Enabled {
		t.Error("TLS should be disabled by default")
	}

	// Verify defaults are applied
	if cfg.Kafka.GroupID != "kafka-mqtt-bridge" {
		t.Errorf("GroupID = %q, want default %q", cfg.Kafka.GroupID, "kafka-mqtt-bridge")
	}
	if cfg.MQTT.Port != 1883 {
		t.Errorf("MQTT.Port = %d, want default %d", cfg.MQTT.Port, 1883)
	}
	if cfg.MQTT.ClientID != "kafka-mqtt-bridge" {
		t.Errorf("MQTT.ClientID = %q, want default %q", cfg.MQTT.ClientID, "kafka-mqtt-bridge")
	}
}

func TestLoadConfig_SASLEnabledOnly(t *testing.T) {
	// Create a config with only SASL enabled (no TLS) - edge case
	configContent := `
kafka:
  broker: "localhost:9092"
  sasl:
    enabled: true
    mechanism: "PLAIN"
    username: "user"
    password: "pass"

mqtt:
  broker: "localhost"

bridge:
  name: "test-bridge"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error: %v", err)
	}

	// Verify SASL is enabled
	if !cfg.Kafka.SASL.Enabled {
		t.Error("SASL should be enabled")
	}

	// Verify TLS is still disabled
	if cfg.Kafka.TLS.Enabled {
		t.Error("TLS should be disabled when not explicitly enabled")
	}
}
