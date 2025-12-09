package config

import (
	"fmt"
	"os"

	"github.com/kelseyhightower/envconfig"
	"gopkg.in/yaml.v3"
)

// Environment variable prefix for all configuration settings.
// All environment variables should be prefixed with BRIDGE_ (e.g., BRIDGE_KAFKA_BROKER).
const envPrefix = "BRIDGE"

// Config represents the application configuration
type Config struct {
	Kafka          KafkaConfig          `yaml:"kafka" envconfig:"KAFKA"`
	MQTT           MQTTConfig           `yaml:"mqtt" envconfig:"MQTT"`
	Bridge         BridgeConfig         `yaml:"bridge" envconfig:"BRIDGE"`
	SchemaRegistry SchemaRegistryConfig `yaml:"schema_registry" envconfig:"SCHEMA_REGISTRY"` // Schema Registry configuration for Avro support
}

// KafkaSASLConfig contains SASL authentication settings for Kafka
type KafkaSASLConfig struct {
	Enabled   bool   `yaml:"enabled" envconfig:"ENABLED"`     // Enable SASL authentication
	Mechanism string `yaml:"mechanism" envconfig:"MECHANISM"` // SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
	Username  string `yaml:"username" envconfig:"USERNAME"`   // SASL username
	Password  string `yaml:"password" envconfig:"PASSWORD"`   // SASL password
}

// KafkaTLSConfig contains TLS settings for Kafka connection
type KafkaTLSConfig struct {
	Enabled            bool   `yaml:"enabled" envconfig:"ENABLED"`                           // Enable TLS connection
	CAFile             string `yaml:"ca_file" envconfig:"CA_FILE"`                           // Path to CA certificate file (optional; if not set, system CA certificates are used)
	CertFile           string `yaml:"cert_file" envconfig:"CERT_FILE"`                       // Path to client certificate file (optional, for mutual TLS)
	KeyFile            string `yaml:"key_file" envconfig:"KEY_FILE"`                         // Path to client private key file (optional, for mutual TLS)
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify" envconfig:"INSECURE_SKIP_VERIFY"` // Skip server certificate verification (not recommended for production)
}

// KafkaConfig contains Kafka connection settings
type KafkaConfig struct {
	Broker  string          `yaml:"broker" envconfig:"BROKER"`
	GroupID string          `yaml:"group_id" envconfig:"GROUP_ID"`
	SASL    KafkaSASLConfig `yaml:"sasl" envconfig:"SASL"` // SASL authentication settings
	TLS     KafkaTLSConfig  `yaml:"tls" envconfig:"TLS"`   // TLS settings
}

// MQTTTLSConfig contains TLS settings for MQTT connection
type MQTTTLSConfig struct {
	Enabled            bool   `yaml:"enabled" envconfig:"ENABLED"`                           // Enable TLS connection
	CAFile             string `yaml:"ca_file" envconfig:"CA_FILE"`                           // Path to CA certificate file (optional; if not set, system CA certificates are used)
	CertFile           string `yaml:"cert_file" envconfig:"CERT_FILE"`                       // Path to client certificate file (optional, for mutual TLS)
	KeyFile            string `yaml:"key_file" envconfig:"KEY_FILE"`                         // Path to client private key file (optional, for mutual TLS)
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify" envconfig:"INSECURE_SKIP_VERIFY"` // Skip server certificate verification (not recommended for production)
}

// MQTTConfig contains MQTT connection settings
type MQTTConfig struct {
	Broker       string        `yaml:"broker" envconfig:"BROKER"`
	Port         int           `yaml:"port" envconfig:"PORT"`
	Username     string        `yaml:"username" envconfig:"USERNAME"`
	Password     string        `yaml:"password" envconfig:"PASSWORD"`
	ClientID     string        `yaml:"client_id" envconfig:"CLIENT_ID"`
	QoS          int           `yaml:"qos" envconfig:"QOS"`                       // QoS level for MQTT operations (0, 1, or 2). Default: 1
	CleanSession *bool         `yaml:"clean_session" envconfig:"CLEAN_SESSION"`   // CleanSession flag. Default: false when QoS > 0, true otherwise
	TLS          MQTTTLSConfig `yaml:"tls" envconfig:"TLS"`                       // TLS configuration
}

// AvroConfig contains Avro serialization/deserialization settings for a topic
type AvroConfig struct {
	// SchemaGroup is the schema group name in the registry
	SchemaGroup string `yaml:"schema_group" envconfig:"SCHEMA_GROUP"`
	// SchemaName is the name of the schema to use
	SchemaName string `yaml:"schema_name" envconfig:"SCHEMA_NAME"`
}

// TopicMapping defines a source and destination topic pair for bridging
type TopicMapping struct {
	SourceTopic string      `yaml:"source_topic" envconfig:"SOURCE_TOPIC"` // Topic to read from
	DestTopic   string      `yaml:"dest_topic" envconfig:"DEST_TOPIC"`     // Topic to write to
	Avro        *AvroConfig `yaml:"avro" envconfig:"AVRO"`                 // Optional Avro config for serialization/deserialization
}

// BridgeConfig contains bridge-specific settings
type BridgeConfig struct {
	Name        string        `yaml:"name" envconfig:"NAME"`
	LogLevel    string        `yaml:"log_level" envconfig:"LOG_LEVEL"`
	BufferSize  int           `yaml:"buffer_size" envconfig:"BUFFER_SIZE"`
	MQTTToKafka *TopicMapping `yaml:"mqtt_to_kafka" envconfig:"MQTT_TO_KAFKA"` // MQTT→Kafka topic mapping
	KafkaToMQTT *TopicMapping `yaml:"kafka_to_mqtt" envconfig:"KAFKA_TO_MQTT"` // Kafka→MQTT topic mapping
}

// SchemaRegistryConfig contains Azure Event Hubs Schema Registry connection settings
type SchemaRegistryConfig struct {
	// FullyQualifiedNamespace is the fully qualified namespace of the Schema Registry
	// e.g., "<namespace>.servicebus.windows.net"
	FullyQualifiedNamespace string `yaml:"fully_qualified_namespace" envconfig:"FULLY_QUALIFIED_NAMESPACE"`
	// TenantID is the Azure tenant ID for authentication (optional)
	// If not provided, DefaultAzureCredential will be used
	TenantID string `yaml:"tenant_id" envconfig:"TENANT_ID"`
	// ClientID is the Azure client ID for authentication (optional)
	// If not provided, DefaultAzureCredential will be used
	ClientID string `yaml:"client_id" envconfig:"CLIENT_ID"`
	// ClientSecret is the Azure client secret for authentication (optional)
	// If not provided, DefaultAzureCredential will be used
	ClientSecret string `yaml:"client_secret" envconfig:"CLIENT_SECRET"`
}

// isEmpty returns true if the AvroConfig has no values set
func (a *AvroConfig) isEmpty() bool {
	return a == nil || (a.SchemaGroup == "" && a.SchemaName == "")
}

// isEmpty returns true if the TopicMapping has no values set
func (t *TopicMapping) isEmpty() bool {
	if t == nil {
		return true
	}
	return t.SourceTopic == "" && t.DestTopic == "" && t.Avro.isEmpty()
}

// cleanupEmptyPointers sets pointer fields to nil if they contain only zero values.
// This is needed because envconfig initializes all pointer struct fields, even when
// no environment variables are set for them.
func cleanupEmptyPointers(cfg *Config) {
	// Clean up Avro configs within topic mappings
	if cfg.Bridge.MQTTToKafka != nil && cfg.Bridge.MQTTToKafka.Avro.isEmpty() {
		cfg.Bridge.MQTTToKafka.Avro = nil
	}
	if cfg.Bridge.KafkaToMQTT != nil && cfg.Bridge.KafkaToMQTT.Avro.isEmpty() {
		cfg.Bridge.KafkaToMQTT.Avro = nil
	}

	// Clean up TopicMapping configs (check after Avro cleanup)
	if cfg.Bridge.MQTTToKafka != nil && cfg.Bridge.MQTTToKafka.isEmpty() {
		cfg.Bridge.MQTTToKafka = nil
	}
	if cfg.Bridge.KafkaToMQTT != nil && cfg.Bridge.KafkaToMQTT.isEmpty() {
		cfg.Bridge.KafkaToMQTT = nil
	}
}

// applyDefaults sets default values for configuration fields
func applyDefaults(cfg *Config) {
	if cfg.Kafka.GroupID == "" {
		cfg.Kafka.GroupID = "kafka-mqtt-bridge"
	}
	if cfg.MQTT.Port == 0 {
		cfg.MQTT.Port = 1883
	}
	if cfg.MQTT.ClientID == "" {
		cfg.MQTT.ClientID = "kafka-mqtt-bridge"
	}
	if cfg.MQTT.QoS < 0 || cfg.MQTT.QoS > 2 {
		cfg.MQTT.QoS = 1
	}
	if cfg.MQTT.CleanSession == nil {
		cleanSession := cfg.MQTT.QoS == 0
		cfg.MQTT.CleanSession = &cleanSession
	}
	if cfg.Bridge.BufferSize == 0 {
		cfg.Bridge.BufferSize = 100
	}
	if cfg.Bridge.LogLevel == "" {
		cfg.Bridge.LogLevel = "info"
	}
}

// LoadConfig loads configuration from a YAML file.
// Environment variables with the prefix BRIDGE_ can override YAML values.
// For example, BRIDGE_KAFKA_BROKER overrides kafka.broker.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Apply environment variable overrides
	if err := envconfig.Process(envPrefix, &cfg); err != nil {
		return nil, fmt.Errorf("failed to process environment variables: %w", err)
	}

	cleanupEmptyPointers(&cfg)
	applyDefaults(&cfg)

	return &cfg, nil
}

// LoadConfigFromEnv loads configuration purely from environment variables.
// All environment variables should be prefixed with BRIDGE_ (e.g., BRIDGE_KAFKA_BROKER).
// This is the recommended approach for containerized deployments.
func LoadConfigFromEnv() (*Config, error) {
	var cfg Config
	if err := envconfig.Process(envPrefix, &cfg); err != nil {
		return nil, fmt.Errorf("failed to process environment variables: %w", err)
	}

	cleanupEmptyPointers(&cfg)
	applyDefaults(&cfg)

	return &cfg, nil
}
