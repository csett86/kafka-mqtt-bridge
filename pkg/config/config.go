package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration
type Config struct {
	Kafka  KafkaConfig  `yaml:"kafka"`
	MQTT   MQTTConfig   `yaml:"mqtt"`
	Bridge BridgeConfig `yaml:"bridge"`
}

// TopicMapping defines a mapping from a source topic pattern to a target topic template
// Source patterns support wildcards:
//   - For MQTT sources: + (single level) and # (multi-level)
//   - For Kafka sources: * (matches any segment)
//
// Target templates can reference captured segments using {1}, {2}, etc.
// Example: source="sensors/+/data" target="kafka/sensors/{1}" would map
// "sensors/temp/data" to "kafka/sensors/temp"
type TopicMapping struct {
	Source string `yaml:"source"` // Source topic pattern with wildcards
	Target string `yaml:"target"` // Target topic template with {1}, {2}, etc.
}

// SASLConfig contains SASL authentication settings for Kafka
// Used for Azure Event Hubs and other SASL-enabled Kafka clusters
type SASLConfig struct {
	// Enabled enables SASL authentication (default: false)
	Enabled bool `yaml:"enabled"`
	// Mechanism specifies the SASL mechanism (currently only "PLAIN" is supported)
	Mechanism string `yaml:"mechanism"`
	// Username for SASL authentication
	// For Azure Event Hubs, use "$ConnectionString"
	Username string `yaml:"username"`
	// Password for SASL authentication
	// For Azure Event Hubs, use the full connection string
	Password string `yaml:"password"`
}

// TLSConfig contains TLS settings for Kafka connections
type TLSConfig struct {
	// Enabled enables TLS encryption (default: false)
	// Required for Azure Event Hubs
	Enabled bool `yaml:"enabled"`
}

// KafkaConfig contains Kafka connection settings
type KafkaConfig struct {
	Broker        string         `yaml:"broker"`
	SourceTopic   string         `yaml:"source_topic"`   // Topic to read from (for Kafka→MQTT) - deprecated, use topic_mappings
	SourceTopics  []string       `yaml:"source_topics"`  // Multiple topics to read from (for Kafka→MQTT)
	DestTopic     string         `yaml:"dest_topic"`     // Topic to write to (for MQTT→Kafka) - deprecated, use topic_mappings
	TopicMappings []TopicMapping `yaml:"topic_mappings"` // Dynamic topic mappings for MQTT→Kafka
	GroupID       string         `yaml:"group_id"`
	SASL          SASLConfig     `yaml:"sasl"` // SASL authentication configuration
	TLS           TLSConfig      `yaml:"tls"`  // TLS encryption configuration
}

// MQTTTLSConfig contains TLS settings for MQTT connection
type MQTTTLSConfig struct {
	Enabled            bool   `yaml:"enabled"`              // Enable TLS connection
	CAFile             string `yaml:"ca_file"`              // Path to CA certificate file
	CertFile           string `yaml:"cert_file"`            // Path to client certificate file (for mutual TLS)
	KeyFile            string `yaml:"key_file"`             // Path to client private key file (for mutual TLS)
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"` // Skip server certificate verification (not recommended for production)
}

// MQTTConfig contains MQTT connection settings
type MQTTConfig struct {
	Broker        string         `yaml:"broker"`
	Port          int            `yaml:"port"`
	Username      string         `yaml:"username"`
	Password      string         `yaml:"password"`
	SourceTopic   string         `yaml:"source_topic"`   // Topic to subscribe to (for MQTT→Kafka) - deprecated, use topic_mappings
	SourceTopics  []string       `yaml:"source_topics"`  // Multiple topics to subscribe to (for MQTT→Kafka)
	DestTopic     string         `yaml:"dest_topic"`     // Topic to publish to (for Kafka→MQTT) - deprecated, use topic_mappings
	TopicMappings []TopicMapping `yaml:"topic_mappings"` // Dynamic topic mappings for Kafka→MQTT
	ClientID      string         `yaml:"client_id"`
	TLS           MQTTTLSConfig  `yaml:"tls"` // TLS configuration
}

// BridgeConfig contains bridge-specific settings
type BridgeConfig struct {
	Name       string `yaml:"name"`
	LogLevel   string `yaml:"log_level"`
	BufferSize int    `yaml:"buffer_size"`
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
	if cfg.Bridge.BufferSize == 0 {
		cfg.Bridge.BufferSize = 100
	}
	if cfg.Bridge.LogLevel == "" {
		cfg.Bridge.LogLevel = "info"
	}

	return &cfg, nil
}
