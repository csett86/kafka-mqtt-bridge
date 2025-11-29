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

// KafkaConfig contains Kafka connection settings
type KafkaConfig struct {
	Brokers     []string `yaml:"brokers"`
	Topic       string   `yaml:"topic"`        // Deprecated: use source_topic/destination_topic
	SourceTopic string   `yaml:"source_topic"` // Topic to read from (for Kafka→MQTT)
	DestTopic   string   `yaml:"dest_topic"`   // Topic to write to (for MQTT→Kafka)
	GroupID     string   `yaml:"group_id"`
}

// MQTTConfig contains MQTT connection settings
type MQTTConfig struct {
	Broker      string `yaml:"broker"`
	Port        int    `yaml:"port"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	Topic       string `yaml:"topic"`        // Deprecated: use source_topic/destination_topic
	SourceTopic string `yaml:"source_topic"` // Topic to subscribe to (for MQTT→Kafka)
	DestTopic   string `yaml:"dest_topic"`   // Topic to publish to (for Kafka→MQTT)
	ClientID    string `yaml:"client_id"`
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
	// Support backward compatibility: use 'topic' if source_topic/dest_topic not set
	if cfg.Kafka.SourceTopic == "" && cfg.Kafka.Topic != "" {
		cfg.Kafka.SourceTopic = cfg.Kafka.Topic
	}
	if cfg.Kafka.DestTopic == "" && cfg.Kafka.Topic != "" {
		cfg.Kafka.DestTopic = cfg.Kafka.Topic
	}
	if cfg.MQTT.Port == 0 {
		cfg.MQTT.Port = 1883
	}
	if cfg.MQTT.ClientID == "" {
		cfg.MQTT.ClientID = "kafka-mqtt-bridge"
	}
	// Support backward compatibility: use 'topic' if source_topic/dest_topic not set
	if cfg.MQTT.SourceTopic == "" && cfg.MQTT.Topic != "" {
		cfg.MQTT.SourceTopic = cfg.MQTT.Topic
	}
	if cfg.MQTT.DestTopic == "" && cfg.MQTT.Topic != "" {
		cfg.MQTT.DestTopic = cfg.MQTT.Topic
	}
	if cfg.Bridge.BufferSize == 0 {
		cfg.Bridge.BufferSize = 100
	}
	if cfg.Bridge.LogLevel == "" {
		cfg.Bridge.LogLevel = "info"
	}

	return &cfg, nil
}
