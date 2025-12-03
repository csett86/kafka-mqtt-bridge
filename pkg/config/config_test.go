package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigWithAvroSerializationOnMQTTToKafka(t *testing.T) {
	// Create a temporary config file with avro config on mqtt_to_kafka
	configContent := `
kafka:
  broker: "localhost:9092"
  group_id: "test-group"

mqtt:
  broker: "localhost"
  port: 1883
  client_id: "test-client"

bridge:
  name: "test-bridge"
  log_level: "info"
  buffer_size: 100
  mqtt_to_kafka:
    source_topic: "mqtt/events"
    dest_topic: "kafka-topic"
    avro:
      schema_group: "test-group"
      schema_name: "test-schema"

schema_registry:
  fully_qualified_namespace: "test.servicebus.windows.net"
  cache_enabled: true
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify avro config on mqtt_to_kafka
	if cfg.Bridge.MQTTToKafka == nil || cfg.Bridge.MQTTToKafka.Avro == nil {
		t.Fatal("Bridge.MQTTToKafka.Avro should not be nil")
	}
	if cfg.Bridge.MQTTToKafka.Avro.SchemaGroup != "test-group" {
		t.Errorf("Avro.SchemaGroup = %q, want %q",
			cfg.Bridge.MQTTToKafka.Avro.SchemaGroup, "test-group")
	}
	if cfg.Bridge.MQTTToKafka.Avro.SchemaName != "test-schema" {
		t.Errorf("Avro.SchemaName = %q, want %q",
			cfg.Bridge.MQTTToKafka.Avro.SchemaName, "test-schema")
	}

	// Verify schema registry settings
	if cfg.SchemaRegistry.FullyQualifiedNamespace != "test.servicebus.windows.net" {
		t.Errorf("SchemaRegistry.FullyQualifiedNamespace = %q, want %q",
			cfg.SchemaRegistry.FullyQualifiedNamespace, "test.servicebus.windows.net")
	}
	if cfg.SchemaRegistry.CacheEnabled == nil || !*cfg.SchemaRegistry.CacheEnabled {
		t.Error("SchemaRegistry.CacheEnabled should be true")
	}
}

func TestLoadConfigWithAvroDeserializationOnKafkaToMQTT(t *testing.T) {
	// Create a config file with avro on kafka_to_mqtt
	configContent := `
kafka:
  broker: "localhost:9092"

mqtt:
  broker: "localhost"
  port: 1883

bridge:
  name: "test-bridge"
  kafka_to_mqtt:
    source_topic: "kafka-topic"
    dest_topic: "mqtt/events"
    avro:
      schema_group: "deserialize-group"
      schema_name: "deserialize-schema"

schema_registry:
  fully_qualified_namespace: "test.servicebus.windows.net"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify avro config on kafka_to_mqtt
	if cfg.Bridge.KafkaToMQTT == nil || cfg.Bridge.KafkaToMQTT.Avro == nil {
		t.Fatal("Bridge.KafkaToMQTT.Avro should not be nil")
	}
	if cfg.Bridge.KafkaToMQTT.Avro.SchemaGroup != "deserialize-group" {
		t.Errorf("Avro.SchemaGroup = %q, want %q",
			cfg.Bridge.KafkaToMQTT.Avro.SchemaGroup, "deserialize-group")
	}
	if cfg.Bridge.KafkaToMQTT.Avro.SchemaName != "deserialize-schema" {
		t.Errorf("Avro.SchemaName = %q, want %q",
			cfg.Bridge.KafkaToMQTT.Avro.SchemaName, "deserialize-schema")
	}
}

func TestLoadConfigSchemaRegistryDefaults(t *testing.T) {
	// Create a minimal config file without schema registry settings
	configContent := `
kafka:
  broker: "localhost:9092"

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
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify schema registry defaults
	if cfg.SchemaRegistry.CacheEnabled == nil {
		t.Error("SchemaRegistry.CacheEnabled should have default value")
	} else if !*cfg.SchemaRegistry.CacheEnabled {
		t.Error("SchemaRegistry.CacheEnabled should default to true")
	}
}

func TestLoadConfigSchemaRegistryWithCredentials(t *testing.T) {
	// Create a config file with Azure credentials
	configContent := `
kafka:
  broker: "localhost:9092"

mqtt:
  broker: "localhost"

bridge:
  name: "test-bridge"
  mqtt_to_kafka:
    source_topic: "mqtt/events"
    dest_topic: "kafka-topic"
    avro:
      schema_group: "test-group"
      schema_name: "test-schema"

schema_registry:
  fully_qualified_namespace: "test.servicebus.windows.net"
  tenant_id: "test-tenant-id"
  client_id: "test-client-id"
  client_secret: "test-client-secret"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify Azure credentials
	if cfg.SchemaRegistry.TenantID != "test-tenant-id" {
		t.Errorf("SchemaRegistry.TenantID = %q, want %q",
			cfg.SchemaRegistry.TenantID, "test-tenant-id")
	}
	if cfg.SchemaRegistry.ClientID != "test-client-id" {
		t.Errorf("SchemaRegistry.ClientID = %q, want %q",
			cfg.SchemaRegistry.ClientID, "test-client-id")
	}
	if cfg.SchemaRegistry.ClientSecret != "test-client-secret" {
		t.Errorf("SchemaRegistry.ClientSecret = %q, want %q",
			cfg.SchemaRegistry.ClientSecret, "test-client-secret")
	}
}

func TestLoadConfigWithoutAvro(t *testing.T) {
	// Create a config file without any avro settings
	configContent := `
kafka:
  broker: "localhost:9092"

mqtt:
  broker: "localhost"

bridge:
  name: "test-bridge"
  mqtt_to_kafka:
    source_topic: "mqtt/events"
    dest_topic: "kafka-topic"
  kafka_to_mqtt:
    source_topic: "kafka-topic"
    dest_topic: "mqtt/events"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify avro is not configured
	if cfg.Bridge.MQTTToKafka.Avro != nil {
		t.Error("Bridge.MQTTToKafka.Avro should be nil when not configured")
	}
	if cfg.Bridge.KafkaToMQTT.Avro != nil {
		t.Error("Bridge.KafkaToMQTT.Avro should be nil when not configured")
	}
}
