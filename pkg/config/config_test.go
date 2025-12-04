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

// clearEnvVars clears all BRIDGE_ prefixed environment variables
func clearEnvVars() {
	const bridgePrefix = "BRIDGE_"
	prefixLen := len(bridgePrefix)
	for _, env := range os.Environ() {
		if len(env) > prefixLen && env[:prefixLen] == bridgePrefix {
			var key string
			for i := range env {
				if env[i] == '=' {
					key = env[:i]
					break
				}
			}
			if key != "" {
				os.Unsetenv(key)
			}
		}
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	// Clear any existing BRIDGE_ env vars
	clearEnvVars()
	defer clearEnvVars()

	// Set environment variables
	os.Setenv("BRIDGE_KAFKA_BROKER", "kafka.example.com:9092")
	os.Setenv("BRIDGE_KAFKA_GROUP_ID", "env-group")
	os.Setenv("BRIDGE_MQTT_BROKER", "mqtt.example.com")
	os.Setenv("BRIDGE_MQTT_PORT", "8883")
	os.Setenv("BRIDGE_MQTT_USERNAME", "env-user")
	os.Setenv("BRIDGE_MQTT_PASSWORD", "env-pass")
	os.Setenv("BRIDGE_BRIDGE_NAME", "env-bridge")
	os.Setenv("BRIDGE_BRIDGE_LOG_LEVEL", "debug")
	os.Setenv("BRIDGE_BRIDGE_BUFFER_SIZE", "200")
	defer func() {
		os.Unsetenv("BRIDGE_KAFKA_BROKER")
		os.Unsetenv("BRIDGE_KAFKA_GROUP_ID")
		os.Unsetenv("BRIDGE_MQTT_BROKER")
		os.Unsetenv("BRIDGE_MQTT_PORT")
		os.Unsetenv("BRIDGE_MQTT_USERNAME")
		os.Unsetenv("BRIDGE_MQTT_PASSWORD")
		os.Unsetenv("BRIDGE_BRIDGE_NAME")
		os.Unsetenv("BRIDGE_BRIDGE_LOG_LEVEL")
		os.Unsetenv("BRIDGE_BRIDGE_BUFFER_SIZE")
	}()

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}

	// Verify Kafka settings
	if cfg.Kafka.Broker != "kafka.example.com:9092" {
		t.Errorf("Kafka.Broker = %q, want %q", cfg.Kafka.Broker, "kafka.example.com:9092")
	}
	if cfg.Kafka.GroupID != "env-group" {
		t.Errorf("Kafka.GroupID = %q, want %q", cfg.Kafka.GroupID, "env-group")
	}

	// Verify MQTT settings
	if cfg.MQTT.Broker != "mqtt.example.com" {
		t.Errorf("MQTT.Broker = %q, want %q", cfg.MQTT.Broker, "mqtt.example.com")
	}
	if cfg.MQTT.Port != 8883 {
		t.Errorf("MQTT.Port = %d, want %d", cfg.MQTT.Port, 8883)
	}
	if cfg.MQTT.Username != "env-user" {
		t.Errorf("MQTT.Username = %q, want %q", cfg.MQTT.Username, "env-user")
	}
	if cfg.MQTT.Password != "env-pass" {
		t.Errorf("MQTT.Password = %q, want %q", cfg.MQTT.Password, "env-pass")
	}

	// Verify Bridge settings
	if cfg.Bridge.Name != "env-bridge" {
		t.Errorf("Bridge.Name = %q, want %q", cfg.Bridge.Name, "env-bridge")
	}
	if cfg.Bridge.LogLevel != "debug" {
		t.Errorf("Bridge.LogLevel = %q, want %q", cfg.Bridge.LogLevel, "debug")
	}
	if cfg.Bridge.BufferSize != 200 {
		t.Errorf("Bridge.BufferSize = %d, want %d", cfg.Bridge.BufferSize, 200)
	}
}

func TestLoadConfigFromEnvWithTopicMappings(t *testing.T) {
	clearEnvVars()
	defer clearEnvVars()

	// Set environment variables for topic mappings
	os.Setenv("BRIDGE_KAFKA_BROKER", "kafka.example.com:9092")
	os.Setenv("BRIDGE_MQTT_BROKER", "mqtt.example.com")
	os.Setenv("BRIDGE_BRIDGE_MQTT_TO_KAFKA_SOURCE_TOPIC", "mqtt/input")
	os.Setenv("BRIDGE_BRIDGE_MQTT_TO_KAFKA_DEST_TOPIC", "kafka-output")
	os.Setenv("BRIDGE_BRIDGE_KAFKA_TO_MQTT_SOURCE_TOPIC", "kafka-input")
	os.Setenv("BRIDGE_BRIDGE_KAFKA_TO_MQTT_DEST_TOPIC", "mqtt/output")
	defer func() {
		os.Unsetenv("BRIDGE_KAFKA_BROKER")
		os.Unsetenv("BRIDGE_MQTT_BROKER")
		os.Unsetenv("BRIDGE_BRIDGE_MQTT_TO_KAFKA_SOURCE_TOPIC")
		os.Unsetenv("BRIDGE_BRIDGE_MQTT_TO_KAFKA_DEST_TOPIC")
		os.Unsetenv("BRIDGE_BRIDGE_KAFKA_TO_MQTT_SOURCE_TOPIC")
		os.Unsetenv("BRIDGE_BRIDGE_KAFKA_TO_MQTT_DEST_TOPIC")
	}()

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}

	// Verify MQTT to Kafka mapping
	if cfg.Bridge.MQTTToKafka == nil {
		t.Fatal("Bridge.MQTTToKafka should not be nil")
	}
	if cfg.Bridge.MQTTToKafka.SourceTopic != "mqtt/input" {
		t.Errorf("MQTTToKafka.SourceTopic = %q, want %q", cfg.Bridge.MQTTToKafka.SourceTopic, "mqtt/input")
	}
	if cfg.Bridge.MQTTToKafka.DestTopic != "kafka-output" {
		t.Errorf("MQTTToKafka.DestTopic = %q, want %q", cfg.Bridge.MQTTToKafka.DestTopic, "kafka-output")
	}

	// Verify Kafka to MQTT mapping
	if cfg.Bridge.KafkaToMQTT == nil {
		t.Fatal("Bridge.KafkaToMQTT should not be nil")
	}
	if cfg.Bridge.KafkaToMQTT.SourceTopic != "kafka-input" {
		t.Errorf("KafkaToMQTT.SourceTopic = %q, want %q", cfg.Bridge.KafkaToMQTT.SourceTopic, "kafka-input")
	}
	if cfg.Bridge.KafkaToMQTT.DestTopic != "mqtt/output" {
		t.Errorf("KafkaToMQTT.DestTopic = %q, want %q", cfg.Bridge.KafkaToMQTT.DestTopic, "mqtt/output")
	}
}

func TestLoadConfigEnvOverridesYAML(t *testing.T) {
	clearEnvVars()
	defer clearEnvVars()

	// Create a YAML config file
	configContent := `
kafka:
  broker: "yaml-kafka:9092"
  group_id: "yaml-group"

mqtt:
  broker: "yaml-mqtt"
  port: 1883

bridge:
  name: "yaml-bridge"
  log_level: "info"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	// Set env vars to override some YAML values
	os.Setenv("BRIDGE_KAFKA_BROKER", "env-kafka:9092")
	os.Setenv("BRIDGE_BRIDGE_LOG_LEVEL", "debug")
	defer func() {
		os.Unsetenv("BRIDGE_KAFKA_BROKER")
		os.Unsetenv("BRIDGE_BRIDGE_LOG_LEVEL")
	}()

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify env vars override YAML
	if cfg.Kafka.Broker != "env-kafka:9092" {
		t.Errorf("Kafka.Broker = %q, want %q (env should override yaml)", cfg.Kafka.Broker, "env-kafka:9092")
	}
	if cfg.Bridge.LogLevel != "debug" {
		t.Errorf("Bridge.LogLevel = %q, want %q (env should override yaml)", cfg.Bridge.LogLevel, "debug")
	}

	// Verify non-overridden YAML values remain
	if cfg.Kafka.GroupID != "yaml-group" {
		t.Errorf("Kafka.GroupID = %q, want %q", cfg.Kafka.GroupID, "yaml-group")
	}
	if cfg.MQTT.Broker != "yaml-mqtt" {
		t.Errorf("MQTT.Broker = %q, want %q", cfg.MQTT.Broker, "yaml-mqtt")
	}
	if cfg.Bridge.Name != "yaml-bridge" {
		t.Errorf("Bridge.Name = %q, want %q", cfg.Bridge.Name, "yaml-bridge")
	}
}

func TestLoadConfigFromEnvDefaults(t *testing.T) {
	clearEnvVars()
	defer clearEnvVars()

	// Only set minimal required values
	os.Setenv("BRIDGE_KAFKA_BROKER", "kafka:9092")
	os.Setenv("BRIDGE_MQTT_BROKER", "mqtt")
	defer func() {
		os.Unsetenv("BRIDGE_KAFKA_BROKER")
		os.Unsetenv("BRIDGE_MQTT_BROKER")
	}()

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}

	// Verify defaults are applied
	if cfg.Kafka.GroupID != "kafka-mqtt-bridge" {
		t.Errorf("Kafka.GroupID = %q, want default %q", cfg.Kafka.GroupID, "kafka-mqtt-bridge")
	}
	if cfg.MQTT.Port != 1883 {
		t.Errorf("MQTT.Port = %d, want default %d", cfg.MQTT.Port, 1883)
	}
	if cfg.MQTT.ClientID != "kafka-mqtt-bridge" {
		t.Errorf("MQTT.ClientID = %q, want default %q", cfg.MQTT.ClientID, "kafka-mqtt-bridge")
	}
	if cfg.Bridge.BufferSize != 100 {
		t.Errorf("Bridge.BufferSize = %d, want default %d", cfg.Bridge.BufferSize, 100)
	}
	if cfg.Bridge.LogLevel != "info" {
		t.Errorf("Bridge.LogLevel = %q, want default %q", cfg.Bridge.LogLevel, "info")
	}
	if cfg.SchemaRegistry.CacheEnabled == nil || !*cfg.SchemaRegistry.CacheEnabled {
		t.Error("SchemaRegistry.CacheEnabled should default to true")
	}
}

func TestLoadConfigFromEnvWithTLS(t *testing.T) {
	clearEnvVars()
	defer clearEnvVars()

	// Set TLS configuration via env vars
	os.Setenv("BRIDGE_KAFKA_BROKER", "kafka:9092")
	os.Setenv("BRIDGE_KAFKA_TLS_ENABLED", "true")
	os.Setenv("BRIDGE_KAFKA_TLS_CA_FILE", "/path/to/ca.crt")
	os.Setenv("BRIDGE_KAFKA_TLS_INSECURE_SKIP_VERIFY", "true")
	os.Setenv("BRIDGE_MQTT_BROKER", "mqtt")
	os.Setenv("BRIDGE_MQTT_TLS_ENABLED", "true")
	os.Setenv("BRIDGE_MQTT_TLS_CERT_FILE", "/path/to/client.crt")
	os.Setenv("BRIDGE_MQTT_TLS_KEY_FILE", "/path/to/client.key")
	defer func() {
		os.Unsetenv("BRIDGE_KAFKA_BROKER")
		os.Unsetenv("BRIDGE_KAFKA_TLS_ENABLED")
		os.Unsetenv("BRIDGE_KAFKA_TLS_CA_FILE")
		os.Unsetenv("BRIDGE_KAFKA_TLS_INSECURE_SKIP_VERIFY")
		os.Unsetenv("BRIDGE_MQTT_BROKER")
		os.Unsetenv("BRIDGE_MQTT_TLS_ENABLED")
		os.Unsetenv("BRIDGE_MQTT_TLS_CERT_FILE")
		os.Unsetenv("BRIDGE_MQTT_TLS_KEY_FILE")
	}()

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}

	// Verify Kafka TLS settings
	if !cfg.Kafka.TLS.Enabled {
		t.Error("Kafka.TLS.Enabled should be true")
	}
	if cfg.Kafka.TLS.CAFile != "/path/to/ca.crt" {
		t.Errorf("Kafka.TLS.CAFile = %q, want %q", cfg.Kafka.TLS.CAFile, "/path/to/ca.crt")
	}
	if !cfg.Kafka.TLS.InsecureSkipVerify {
		t.Error("Kafka.TLS.InsecureSkipVerify should be true")
	}

	// Verify MQTT TLS settings
	if !cfg.MQTT.TLS.Enabled {
		t.Error("MQTT.TLS.Enabled should be true")
	}
	if cfg.MQTT.TLS.CertFile != "/path/to/client.crt" {
		t.Errorf("MQTT.TLS.CertFile = %q, want %q", cfg.MQTT.TLS.CertFile, "/path/to/client.crt")
	}
	if cfg.MQTT.TLS.KeyFile != "/path/to/client.key" {
		t.Errorf("MQTT.TLS.KeyFile = %q, want %q", cfg.MQTT.TLS.KeyFile, "/path/to/client.key")
	}
}

func TestLoadConfigFromEnvWithSASL(t *testing.T) {
	clearEnvVars()
	defer clearEnvVars()

	// Set SASL configuration via env vars
	os.Setenv("BRIDGE_KAFKA_BROKER", "kafka:9092")
	os.Setenv("BRIDGE_KAFKA_SASL_ENABLED", "true")
	os.Setenv("BRIDGE_KAFKA_SASL_MECHANISM", "PLAIN")
	os.Setenv("BRIDGE_KAFKA_SASL_USERNAME", "sasl-user")
	os.Setenv("BRIDGE_KAFKA_SASL_PASSWORD", "sasl-pass")
	os.Setenv("BRIDGE_MQTT_BROKER", "mqtt")
	defer func() {
		os.Unsetenv("BRIDGE_KAFKA_BROKER")
		os.Unsetenv("BRIDGE_KAFKA_SASL_ENABLED")
		os.Unsetenv("BRIDGE_KAFKA_SASL_MECHANISM")
		os.Unsetenv("BRIDGE_KAFKA_SASL_USERNAME")
		os.Unsetenv("BRIDGE_KAFKA_SASL_PASSWORD")
		os.Unsetenv("BRIDGE_MQTT_BROKER")
	}()

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}

	// Verify Kafka SASL settings
	if !cfg.Kafka.SASL.Enabled {
		t.Error("Kafka.SASL.Enabled should be true")
	}
	if cfg.Kafka.SASL.Mechanism != "PLAIN" {
		t.Errorf("Kafka.SASL.Mechanism = %q, want %q", cfg.Kafka.SASL.Mechanism, "PLAIN")
	}
	if cfg.Kafka.SASL.Username != "sasl-user" {
		t.Errorf("Kafka.SASL.Username = %q, want %q", cfg.Kafka.SASL.Username, "sasl-user")
	}
	if cfg.Kafka.SASL.Password != "sasl-pass" {
		t.Errorf("Kafka.SASL.Password = %q, want %q", cfg.Kafka.SASL.Password, "sasl-pass")
	}
}

func TestLoadConfigFromEnvWithSchemaRegistry(t *testing.T) {
	clearEnvVars()
	defer clearEnvVars()

	// Set Schema Registry configuration via env vars
	os.Setenv("BRIDGE_KAFKA_BROKER", "kafka:9092")
	os.Setenv("BRIDGE_MQTT_BROKER", "mqtt")
	os.Setenv("BRIDGE_SCHEMA_REGISTRY_FULLY_QUALIFIED_NAMESPACE", "test.servicebus.windows.net")
	os.Setenv("BRIDGE_SCHEMA_REGISTRY_CACHE_ENABLED", "false")
	os.Setenv("BRIDGE_SCHEMA_REGISTRY_TENANT_ID", "tenant-123")
	os.Setenv("BRIDGE_SCHEMA_REGISTRY_CLIENT_ID", "client-456")
	os.Setenv("BRIDGE_SCHEMA_REGISTRY_CLIENT_SECRET", "secret-789")
	defer func() {
		os.Unsetenv("BRIDGE_KAFKA_BROKER")
		os.Unsetenv("BRIDGE_MQTT_BROKER")
		os.Unsetenv("BRIDGE_SCHEMA_REGISTRY_FULLY_QUALIFIED_NAMESPACE")
		os.Unsetenv("BRIDGE_SCHEMA_REGISTRY_CACHE_ENABLED")
		os.Unsetenv("BRIDGE_SCHEMA_REGISTRY_TENANT_ID")
		os.Unsetenv("BRIDGE_SCHEMA_REGISTRY_CLIENT_ID")
		os.Unsetenv("BRIDGE_SCHEMA_REGISTRY_CLIENT_SECRET")
	}()

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}

	// Verify Schema Registry settings
	if cfg.SchemaRegistry.FullyQualifiedNamespace != "test.servicebus.windows.net" {
		t.Errorf("SchemaRegistry.FullyQualifiedNamespace = %q, want %q",
			cfg.SchemaRegistry.FullyQualifiedNamespace, "test.servicebus.windows.net")
	}
	if cfg.SchemaRegistry.CacheEnabled == nil || *cfg.SchemaRegistry.CacheEnabled {
		t.Error("SchemaRegistry.CacheEnabled should be false when explicitly set")
	}
	if cfg.SchemaRegistry.TenantID != "tenant-123" {
		t.Errorf("SchemaRegistry.TenantID = %q, want %q", cfg.SchemaRegistry.TenantID, "tenant-123")
	}
	if cfg.SchemaRegistry.ClientID != "client-456" {
		t.Errorf("SchemaRegistry.ClientID = %q, want %q", cfg.SchemaRegistry.ClientID, "client-456")
	}
	if cfg.SchemaRegistry.ClientSecret != "secret-789" {
		t.Errorf("SchemaRegistry.ClientSecret = %q, want %q", cfg.SchemaRegistry.ClientSecret, "secret-789")
	}
}

func TestLoadConfigFromEnvEmptyPointers(t *testing.T) {
	clearEnvVars()
	defer clearEnvVars()

	// Set minimal configuration without topic mappings
	os.Setenv("BRIDGE_KAFKA_BROKER", "kafka:9092")
	os.Setenv("BRIDGE_MQTT_BROKER", "mqtt")
	defer func() {
		os.Unsetenv("BRIDGE_KAFKA_BROKER")
		os.Unsetenv("BRIDGE_MQTT_BROKER")
	}()

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}

	// Verify that empty pointer fields remain nil
	if cfg.Bridge.MQTTToKafka != nil {
		t.Error("Bridge.MQTTToKafka should be nil when not configured")
	}
	if cfg.Bridge.KafkaToMQTT != nil {
		t.Error("Bridge.KafkaToMQTT should be nil when not configured")
	}
}
