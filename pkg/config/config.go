package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration
type Config struct {
	Kafka          KafkaConfig          `yaml:"kafka"`
	MQTT           MQTTConfig           `yaml:"mqtt"`
	Bridge         BridgeConfig         `yaml:"bridge"`
	SchemaRegistry SchemaRegistryConfig `yaml:"schema_registry"` // Schema Registry configuration for Avro support
}

// KafkaSASLConfig contains SASL authentication settings for Kafka
type KafkaSASLConfig struct {
	Enabled   bool   `yaml:"enabled"`   // Enable SASL authentication
	Mechanism string `yaml:"mechanism"` // SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
	Username  string `yaml:"username"`  // SASL username
	Password  string `yaml:"password"`  // SASL password
}

// KafkaTLSConfig contains TLS settings for Kafka connection
type KafkaTLSConfig struct {
	Enabled            bool   `yaml:"enabled"`              // Enable TLS connection
	CAFile             string `yaml:"ca_file"`              // Path to CA certificate file (optional; if not set, system CA certificates are used)
	CertFile           string `yaml:"cert_file"`            // Path to client certificate file (optional, for mutual TLS)
	KeyFile            string `yaml:"key_file"`             // Path to client private key file (optional, for mutual TLS)
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"` // Skip server certificate verification (not recommended for production)
}

// KafkaConfig contains Kafka connection settings
type KafkaConfig struct {
	Broker  string          `yaml:"broker"`
	GroupID string          `yaml:"group_id"`
	SASL    KafkaSASLConfig `yaml:"sasl"` // SASL authentication settings
	TLS     KafkaTLSConfig  `yaml:"tls"`  // TLS settings
}

// MQTTTLSConfig contains TLS settings for MQTT connection
type MQTTTLSConfig struct {
	Enabled            bool   `yaml:"enabled"`              // Enable TLS connection
	CAFile             string `yaml:"ca_file"`              // Path to CA certificate file (optional; if not set, system CA certificates are used)
	CertFile           string `yaml:"cert_file"`            // Path to client certificate file (optional, for mutual TLS)
	KeyFile            string `yaml:"key_file"`             // Path to client private key file (optional, for mutual TLS)
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"` // Skip server certificate verification (not recommended for production)
}

// MQTTConfig contains MQTT connection settings
type MQTTConfig struct {
	Broker       string        `yaml:"broker"`
	Port         int           `yaml:"port"`
	Username     string        `yaml:"username"`
	Password     string        `yaml:"password"`
	ClientID     string        `yaml:"client_id"`
	QoS          int           `yaml:"qos"`           // QoS level for MQTT operations (0, 1, or 2). Default: 1
	CleanSession *bool         `yaml:"clean_session"` // CleanSession flag. Default: false when QoS > 0, true otherwise
	TLS          MQTTTLSConfig `yaml:"tls"`           // TLS configuration
}

// TopicMapping defines a source and destination topic pair for bridging
type TopicMapping struct {
	SourceTopic string `yaml:"source_topic"` // Topic to read from
	DestTopic   string `yaml:"dest_topic"`   // Topic to write to
}

// BridgeConfig contains bridge-specific settings
type BridgeConfig struct {
	Name        string        `yaml:"name"`
	LogLevel    string        `yaml:"log_level"`
	BufferSize  int           `yaml:"buffer_size"`
	MQTTToKafka *TopicMapping `yaml:"mqtt_to_kafka"` // MQTT→Kafka topic mapping
	KafkaToMQTT *TopicMapping `yaml:"kafka_to_mqtt"` // Kafka→MQTT topic mapping
}

// SchemaRegistryConfig contains Azure Event Hubs Schema Registry settings
type SchemaRegistryConfig struct {
	Enabled bool `yaml:"enabled"` // Enable Schema Registry integration for Avro serialization
	// FullyQualifiedNamespace is the fully qualified namespace of the Schema Registry
	// e.g., "<namespace>.servicebus.windows.net"
	FullyQualifiedNamespace string `yaml:"fully_qualified_namespace"`
	// GroupName is the schema group name in the registry
	GroupName string `yaml:"group_name"`
	// SchemaName is the name of the schema to use for serialization/deserialization
	SchemaName string `yaml:"schema_name"`
	// CacheEnabled enables schema caching (default: true)
	CacheEnabled *bool `yaml:"cache_enabled"`
	// TenantID is the Azure tenant ID for authentication (optional)
	// If not provided, DefaultAzureCredential will be used
	TenantID string `yaml:"tenant_id"`
	// ClientID is the Azure client ID for authentication (optional)
	// If not provided, DefaultAzureCredential will be used
	ClientID string `yaml:"client_id"`
	// ClientSecret is the Azure client secret for authentication (optional)
	// If not provided, DefaultAzureCredential will be used
	ClientSecret string `yaml:"client_secret"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	if cfg.Kafka.GroupID == "" {
		cfg.Kafka.GroupID = "kafka-mqtt-bridge"
	}
	if cfg.MQTT.Port == 0 {
		cfg.MQTT.Port = 1883
	}
	if cfg.MQTT.ClientID == "" {
		cfg.MQTT.ClientID = "kafka-mqtt-bridge"
	}
	// Validate and set default QoS (default: 1 for at-least-once delivery)
	if cfg.MQTT.QoS < 0 || cfg.MQTT.QoS > 2 {
		cfg.MQTT.QoS = 1
	}
	// Set CleanSession default: false when QoS > 0 to support session persistence
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

	// Set Schema Registry defaults
	if cfg.SchemaRegistry.CacheEnabled == nil {
		cacheEnabled := true
		cfg.SchemaRegistry.CacheEnabled = &cacheEnabled
	}

	return &cfg, nil
}
