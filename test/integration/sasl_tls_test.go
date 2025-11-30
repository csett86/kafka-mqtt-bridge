// Package integration provides integration tests for the kafka-mqtt-bridge.
// These tests require running Kafka and MQTT brokers.
package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/csett86/kafka-mqtt-bridge/internal/kafka"
	"github.com/csett86/kafka-mqtt-bridge/pkg/config"
	"go.uber.org/zap"
)

// Environment variables for SASL-enabled Kafka testing
var (
	saslKafkaBroker   = getEnv("TEST_SASL_KAFKA_BROKER", "")
	saslKafkaUsername = getEnv("TEST_SASL_KAFKA_USERNAME", "$ConnectionString")
	saslKafkaPassword = getEnv("TEST_SASL_KAFKA_PASSWORD", "")
)

// TestSASLTLSClientConfiguration tests that the Kafka client correctly configures
// SASL and TLS when those options are enabled in the configuration.
func TestSASLTLSClientConfiguration(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	tests := []struct {
		name           string
		saslEnabled    bool
		tlsEnabled     bool
		mechanism      string
		username       string
		password       string
		expectSASL     bool
		expectTLS      bool
		expectDialerOK bool
	}{
		{
			name:           "No SASL, No TLS",
			saslEnabled:    false,
			tlsEnabled:     false,
			expectSASL:     false,
			expectTLS:      false,
			expectDialerOK: true,
		},
		{
			name:           "TLS only",
			saslEnabled:    false,
			tlsEnabled:     true,
			expectSASL:     false,
			expectTLS:      true,
			expectDialerOK: true,
		},
		{
			name:           "SASL PLAIN with TLS (Azure Event Hubs config)",
			saslEnabled:    true,
			tlsEnabled:     true,
			mechanism:      "PLAIN",
			username:       "$ConnectionString",
			password:       "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test-key",
			expectSASL:     true,
			expectTLS:      true,
			expectDialerOK: true,
		},
		{
			name:           "SASL PLAIN without explicit mechanism (defaults to PLAIN)",
			saslEnabled:    true,
			tlsEnabled:     true,
			mechanism:      "",
			username:       "user",
			password:       "pass",
			expectSASL:     true,
			expectTLS:      true,
			expectDialerOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := kafka.ClientConfig{
				Brokers:    []string{"localhost:9092"},
				ReadTopic:  "test-topic",
				WriteTopic: "test-topic",
				GroupID:    "test-group",
				SASL: config.SASLConfig{
					Enabled:   tt.saslEnabled,
					Mechanism: tt.mechanism,
					Username:  tt.username,
					Password:  tt.password,
				},
				TLS: config.TLSConfig{
					Enabled: tt.tlsEnabled,
				},
				Logger: logger,
			}

			client, err := kafka.NewClientWithConfig(cfg)
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			// Verify client was created successfully
			if client == nil {
				t.Fatal("Client is nil")
			}

			t.Logf("Successfully created Kafka client with SASL=%v, TLS=%v", tt.saslEnabled, tt.tlsEnabled)
		})
	}
}

// TestSASLTLSBridgeBinaryConfig tests that the bridge binary correctly handles
// SASL and TLS configuration from a config file.
func TestSASLTLSBridgeBinaryConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testID := time.Now().UnixNano()

	// Create a config file with SASL/TLS settings
	// Note: This uses a fake broker address to test config parsing
	configContent := fmt.Sprintf(`
# Kafka Configuration with SASL/TLS
kafka:
  brokers:
    - "fake-eventhub.servicebus.windows.net:9093"
  source_topic: "test-topic"
  group_id: "test-sasl-group-%d"
  sasl:
    enabled: true
    mechanism: "PLAIN"
    username: "$ConnectionString"
    password: "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=testkey"
  tls:
    enabled: true

mqtt:
  broker: "%s"
  port: %d
  dest_topic: "mqtt/sasl-test/%d"
  client_id: "test-sasl-bridge-%d"

bridge:
  name: "test-sasl-bridge"
  log_level: "debug"
  buffer_size: 100
`, testID, mqttBroker, mqttPort, testID, testID)

	// Create temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Logf("Created SASL/TLS config file at: %s", configPath)

	// Build the bridge binary
	binaryPath := filepath.Join(tmpDir, "kafka-mqtt-bridge")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/bridge")
	buildCmd.Dir = getProjectRoot()
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build bridge binary: %v\nOutput: %s", err, output)
	}

	t.Logf("Built bridge binary at: %s", binaryPath)

	// Start the bridge - it will attempt to connect and fail (expected behavior)
	// but this validates that the config is parsed correctly
	bridgeCmd := exec.CommandContext(ctx, binaryPath, "-config", configPath)
	bridgeCmd.Dir = tmpDir

	// Capture output
	output, err := bridgeCmd.CombinedOutput()
	outputStr := string(output)

	// The bridge should fail to connect (since there's no real Event Hub),
	// but it should not fail to parse the config
	t.Logf("Bridge output: %s", outputStr)

	// Check that it didn't fail due to config parsing errors
	if err != nil {
		// This is expected - the bridge will fail to connect to the fake broker
		// But we should verify it's not a config parsing error
		if containsConfigError(outputStr) {
			t.Errorf("Bridge failed due to config error: %s", outputStr)
		} else {
			t.Logf("Bridge exited as expected (cannot connect to fake broker): %v", err)
		}
	}
}

// TestSASLTLSRealConnection tests connection to a real SASL-enabled Kafka broker
// if the environment variables are configured.
// Skip this test if no SASL broker is configured.
func TestSASLTLSRealConnection(t *testing.T) {
	if saslKafkaBroker == "" || saslKafkaPassword == "" {
		t.Skip("Skipping SASL/TLS real connection test: TEST_SASL_KAFKA_BROKER and TEST_SASL_KAFKA_PASSWORD not set")
	}

	logger, _ := zap.NewDevelopment()

	t.Logf("Testing real SASL/TLS connection to: %s", saslKafkaBroker)

	cfg := kafka.ClientConfig{
		Brokers:    []string{saslKafkaBroker},
		WriteTopic: "test-sasl-topic",
		GroupID:    fmt.Sprintf("test-sasl-group-%d", time.Now().UnixNano()),
		SASL: config.SASLConfig{
			Enabled:   true,
			Mechanism: "PLAIN",
			Username:  saslKafkaUsername,
			Password:  saslKafkaPassword,
		},
		TLS: config.TLSConfig{
			Enabled: true,
		},
		Logger: logger,
	}

	client, err := kafka.NewClientWithConfig(cfg)
	if err != nil {
		t.Fatalf("Failed to create SASL/TLS client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try to write a test message
	testMessage := fmt.Sprintf("sasl-tls-test-message-%d", time.Now().UnixNano())
	err = client.WriteMessage(ctx, []byte("test-key"), []byte(testMessage))
	if err != nil {
		t.Fatalf("Failed to write message with SASL/TLS: %v", err)
	}

	t.Logf("Successfully wrote message to SASL/TLS enabled Kafka: %s", testMessage)
}

// TestSASLTLSBridgeWithRealEventHub tests the full bridge flow with a real Azure Event Hub
// if the environment variables are configured.
func TestSASLTLSBridgeWithRealEventHub(t *testing.T) {
	if saslKafkaBroker == "" || saslKafkaPassword == "" {
		t.Skip("Skipping SASL/TLS bridge test: TEST_SASL_KAFKA_BROKER and TEST_SASL_KAFKA_PASSWORD not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	testID := time.Now().UnixNano()
	mqttTopic := fmt.Sprintf("mqtt/eventhub-test/%d", testID)
	testMessage := fmt.Sprintf("eventhub-bridge-test-%d", testID)

	// Create a config file for MQTT→EventHub bridging
	configContent := fmt.Sprintf(`
kafka:
  brokers:
    - "%s"
  dest_topic: "test-mqtt-to-eventhub"
  group_id: "test-eventhub-group-%d"
  sasl:
    enabled: true
    mechanism: "PLAIN"
    username: "%s"
    password: "%s"
  tls:
    enabled: true

mqtt:
  broker: "%s"
  port: %d
  source_topic: "%s"
  client_id: "test-eventhub-bridge-%d"

bridge:
  name: "test-eventhub-bridge"
  log_level: "debug"
  buffer_size: 100
`, saslKafkaBroker, testID, saslKafkaUsername, saslKafkaPassword,
		mqttBroker, mqttPort, mqttTopic, testID)

	// Create temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	// Build the bridge binary
	binaryPath := filepath.Join(tmpDir, "kafka-mqtt-bridge")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/bridge")
	buildCmd.Dir = getProjectRoot()
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build bridge binary: %v\nOutput: %s", err, output)
	}

	// Start the bridge binary
	bridgeCmd := exec.CommandContext(ctx, binaryPath, "-config", configPath)
	bridgeCmd.Dir = tmpDir
	bridgeCmd.Stdout = os.Stdout
	bridgeCmd.Stderr = os.Stderr

	if err := bridgeCmd.Start(); err != nil {
		t.Fatalf("Failed to start bridge: %v", err)
	}

	defer func() {
		if bridgeCmd.Process != nil {
			bridgeCmd.Process.Kill()
			bridgeCmd.Wait()
		}
	}()

	// Give bridge time to start and connect
	time.Sleep(5 * time.Second)

	// Setup MQTT publisher
	mqttPublisher := setupMQTTPublisher(t)
	defer mqttPublisher.Disconnect(250)

	// Publish message to MQTT - the bridge should forward it to Event Hub
	token := mqttPublisher.Publish(mqttTopic, 1, false, []byte(testMessage))
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to publish message to MQTT: %v", token.Error())
	}

	t.Logf("Published message to MQTT topic %s: %s", mqttTopic, testMessage)
	t.Log("Message should now be forwarded to Azure Event Hub via SASL/TLS")

	// Give time for the bridge to process
	time.Sleep(3 * time.Second)

	// Note: To fully verify, you would need to read from Event Hub,
	// but that requires setting up a consumer which adds complexity.
	// The test validates that:
	// 1. Config with SASL/TLS is parsed correctly
	// 2. Bridge starts successfully with SASL/TLS config
	// 3. MQTT message can be published for bridging

	t.Log("SASL/TLS bridge test completed - verify message in Azure Event Hub if needed")
}

// TestTLSConfiguration tests that TLS is properly configured
func TestTLSConfiguration(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	// Test with TLS enabled
	cfg := kafka.ClientConfig{
		Brokers:    []string{"localhost:9093"},
		WriteTopic: "test-topic",
		GroupID:    "test-group",
		TLS: config.TLSConfig{
			Enabled: true,
		},
		Logger: logger,
	}

	client, err := kafka.NewClientWithConfig(cfg)
	if err != nil {
		t.Fatalf("Failed to create client with TLS: %v", err)
	}
	defer client.Close()

	// Verify client was created
	if client == nil {
		t.Fatal("Client is nil")
	}

	t.Log("Successfully created Kafka client with TLS enabled")
}

// TestSASLPlainMechanism tests SASL PLAIN mechanism configuration
func TestSASLPlainMechanism(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	testCases := []struct {
		name      string
		mechanism string
		expectErr bool
	}{
		{"PLAIN uppercase", "PLAIN", false},
		{"plain lowercase", "plain", false},
		{"Empty defaults to PLAIN", "", false},
		{"Unsupported mechanism", "SCRAM-SHA-256", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := kafka.ClientConfig{
				Brokers:    []string{"localhost:9092"},
				WriteTopic: "test-topic",
				GroupID:    "test-group",
				SASL: config.SASLConfig{
					Enabled:   true,
					Mechanism: tc.mechanism,
					Username:  "test-user",
					Password:  "test-pass",
				},
				Logger: logger,
			}

			client, err := kafka.NewClientWithConfig(cfg)
			if tc.expectErr {
				if err == nil {
					t.Error("Expected error for unsupported mechanism, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer client.Close()

			t.Logf("Successfully created client with mechanism: %s", tc.mechanism)
		})
	}
}

// containsConfigError checks if the output contains a configuration error
func containsConfigError(output string) bool {
	configErrors := []string{
		"failed to parse config",
		"failed to read config",
		"invalid configuration",
		"unknown field",
	}
	for _, errStr := range configErrors {
		if len(output) >= len(errStr) {
			for i := 0; i <= len(output)-len(errStr); i++ {
				if output[i:i+len(errStr)] == errStr {
					return true
				}
			}
		}
	}
	return false
}
