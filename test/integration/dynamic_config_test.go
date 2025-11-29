// Package integration provides integration tests for the kafka-mqtt-bridge.
// These tests require running Kafka and MQTT brokers.
package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// TestDynamicTopicMappingWithBinary tests dynamic topic mapping using the built binary
// and a config file. This test:
// 1. Creates a config file with topic_mappings
// 2. Builds and runs the bridge binary
// 3. Publishes messages to MQTT topics matching wildcard patterns
// 4. Verifies messages are routed to the correct Kafka topics
func TestDynamicTopicMappingWithBinary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create unique topic names for this test
	testID := time.Now().UnixNano()

	// Create a temporary config file with dynamic topic mappings
	// The config uses:
	// - MQTT wildcard pattern: sensors/+/data-{testID} where + matches any single level (e.g., "temperature", "humidity")
	// - Kafka target template: kafka-sensors-{1}-{testID} where {1} is replaced with the captured wildcard value
	configContent := fmt.Sprintf(`
# Kafka Configuration
kafka:
  brokers:
    - "localhost:9092"
  group_id: "test-dynamic-mapping-group"
  # Dynamic topic mappings for MQTT→Kafka bridging
  # The + wildcard matches exactly one topic level
  # {1} in target references the first captured wildcard segment
  topic_mappings:
    - source: "sensors/+/data-%d"
      target: "kafka-sensors-{1}-%d"

# MQTT Configuration
mqtt:
  broker: "localhost"
  port: 1883
  client_id: "test-dynamic-bridge-client"

# Bridge Configuration
bridge:
  name: "test-dynamic-bridge"
  log_level: "debug"
  buffer_size: 100
`, testID, testID)

	// Create temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Logf("Created config file at: %s", configPath)
	t.Logf("Config content:\n%s", configContent)

	// Build the bridge binary
	binaryPath := filepath.Join(tmpDir, "kafka-mqtt-bridge")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/bridge")
	buildCmd.Dir = "/home/runner/work/kafka-mqtt-bridge/kafka-mqtt-bridge"
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build bridge binary: %v\nOutput: %s", err, output)
	}

	t.Logf("Built bridge binary at: %s", binaryPath)

	// Start the bridge binary
	bridgeCmd := exec.CommandContext(ctx, binaryPath, "-config", configPath)
	bridgeCmd.Dir = tmpDir
	bridgeCmd.Stdout = os.Stdout
	bridgeCmd.Stderr = os.Stderr

	if err := bridgeCmd.Start(); err != nil {
		t.Fatalf("Failed to start bridge: %v", err)
	}

	// Ensure we clean up the process
	defer func() {
		if bridgeCmd.Process != nil {
			_ = bridgeCmd.Process.Signal(syscall.SIGTERM)
			_ = bridgeCmd.Wait()
		}
	}()

	t.Log("Bridge binary started, waiting for it to initialize...")

	// Give the bridge time to start and connect
	time.Sleep(3 * time.Second)

	// Test sensor types to use with wildcard
	sensorTypes := []string{"temperature", "humidity", "pressure"}
	testMessages := make(map[string]string)

	for _, sensorType := range sensorTypes {
		testMessages[sensorType] = fmt.Sprintf("test-data-from-%s-%d", sensorType, testID)
	}

	// Setup MQTT publisher
	mqttPublisher := setupMQTTPublisher(t)
	defer mqttPublisher.Disconnect(250)

	// Publish messages to different MQTT topics that match the wildcard pattern
	for sensorType, message := range testMessages {
		mqttTopic := fmt.Sprintf("sensors/%s/data-%d", sensorType, testID)
		token := mqttPublisher.Publish(mqttTopic, 1, false, []byte(message))
		if token.Wait() && token.Error() != nil {
			t.Fatalf("Failed to publish message to MQTT topic %s: %v", mqttTopic, token.Error())
		}
		t.Logf("Published message to MQTT topic %s: %s", mqttTopic, message)
	}

	// Give the bridge time to process messages
	time.Sleep(2 * time.Second)

	// Read from the corresponding Kafka topics and verify the messages
	for sensorType, expectedMessage := range testMessages {
		kafkaTopic := fmt.Sprintf("kafka-sensors-%s-%d", sensorType, testID)
		t.Logf("Checking Kafka topic: %s for message: %s", kafkaTopic, expectedMessage)

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaBrokers},
			Topic:    kafkaTopic,
			GroupID:  fmt.Sprintf("test-reader-%s-%d", sensorType, testID),
			MinBytes: 1,
			MaxBytes: 10e6,
			MaxWait:  100 * time.Millisecond,
		})
		defer reader.Close()

		readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
		msg, err := reader.ReadMessage(readCtx)
		readCancel()

		if err != nil {
			t.Errorf("Failed to read message from Kafka topic %s: %v", kafkaTopic, err)
			continue
		}

		if string(msg.Value) != expectedMessage {
			t.Errorf("Message mismatch on topic %s: got %q, want %q", kafkaTopic, string(msg.Value), expectedMessage)
		} else {
			t.Logf("Successfully received message on Kafka topic %s: %s", kafkaTopic, string(msg.Value))
		}
	}

	t.Log("Successfully tested dynamic topic mapping with binary")
}

// TestDynamicTopicMappingMultiLevel tests multi-level wildcard (#) with binary
func TestDynamicTopicMappingMultiLevel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create unique topic names for this test
	testID := time.Now().UnixNano()

	// Create a temporary config file with multi-level wildcard mapping
	// The config uses:
	// - MQTT wildcard pattern: devices/{testID}/# where # matches zero or more topic levels
	// - Kafka target: kafka-devices-{testID} (all matching messages go to this single topic)
	configContent := fmt.Sprintf(`
# Kafka Configuration
kafka:
  brokers:
    - "localhost:9092"
  group_id: "test-multilevel-group"
  # Dynamic topic mappings with multi-level wildcard
  # The # wildcard matches zero or more topic levels (including nested paths)
  # All messages matching this pattern go to the same Kafka topic
  topic_mappings:
    - source: "devices/%d/#"
      target: "kafka-devices-%d"

# MQTT Configuration
mqtt:
  broker: "localhost"
  port: 1883
  client_id: "test-multilevel-bridge-client"

# Bridge Configuration
bridge:
  name: "test-multilevel-bridge"
  log_level: "debug"
  buffer_size: 100
`, testID, testID)

	// Create temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Logf("Created config file at: %s", configPath)

	// Build the bridge binary
	binaryPath := filepath.Join(tmpDir, "kafka-mqtt-bridge")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/bridge")
	buildCmd.Dir = "/home/runner/work/kafka-mqtt-bridge/kafka-mqtt-bridge"
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
			_ = bridgeCmd.Process.Signal(syscall.SIGTERM)
			_ = bridgeCmd.Wait()
		}
	}()

	// Give the bridge time to start
	time.Sleep(3 * time.Second)

	// Setup MQTT publisher
	mqttPublisher := setupMQTTPublisher(t)
	defer mqttPublisher.Disconnect(250)

	// Test messages from different multi-level paths
	testCases := []struct {
		mqttPath string
		message  string
	}{
		{fmt.Sprintf("devices/%d/floor1/room1", testID), "floor1-room1-data"},
		{fmt.Sprintf("devices/%d/floor2/room3/sensor", testID), "floor2-room3-sensor-data"},
		{fmt.Sprintf("devices/%d/basement", testID), "basement-data"},
	}

	kafkaTopic := fmt.Sprintf("kafka-devices-%d", testID)

	// Setup Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    kafkaTopic,
		GroupID:  fmt.Sprintf("test-multilevel-reader-%d", testID),
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  100 * time.Millisecond,
	})
	defer reader.Close()

	// Publish all messages
	for _, tc := range testCases {
		token := mqttPublisher.Publish(tc.mqttPath, 1, false, []byte(tc.message))
		if token.Wait() && token.Error() != nil {
			t.Fatalf("Failed to publish message to MQTT topic %s: %v", tc.mqttPath, token.Error())
		}
		t.Logf("Published message to MQTT topic %s: %s", tc.mqttPath, tc.message)
	}

	// Give the bridge time to process
	time.Sleep(2 * time.Second)

	// Read and verify all messages arrived at the same Kafka topic
	receivedMessages := make(map[string]bool)
	for i := 0; i < len(testCases); i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
		msg, err := reader.ReadMessage(readCtx)
		readCancel()

		if err != nil {
			t.Errorf("Failed to read message %d from Kafka: %v", i+1, err)
			continue
		}

		receivedMessages[string(msg.Value)] = true
		t.Logf("Received message on Kafka topic %s: %s", kafkaTopic, string(msg.Value))
	}

	// Verify all messages were received
	for _, tc := range testCases {
		if !receivedMessages[tc.message] {
			t.Errorf("Message %q was not received on Kafka topic %s", tc.message, kafkaTopic)
		}
	}

	t.Log("Successfully tested multi-level wildcard topic mapping with binary")
}

// TestDynamicTopicMappingKafkaToMQTT tests Kafka→MQTT direction with topic mappings using binary
func TestDynamicTopicMappingKafkaToMQTT(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create unique topic names for this test
	testID := time.Now().UnixNano()
	kafkaSourceTopic := fmt.Sprintf("kafka-source-%d", testID)

	// Create a temporary config file with Kafka→MQTT topic mappings
	// The config uses:
	// - Kafka source topic: kafka-source-{testID}
	// - MQTT topic mapping: source matches the Kafka topic, target is mqtt/dynamic/{testID}/events
	configContent := fmt.Sprintf(`
# Kafka Configuration
kafka:
  brokers:
    - "localhost:9092"
  # Source topic to read from Kafka
  source_topic: "kafka-source-%d"
  group_id: "test-k2m-dynamic-group"

# MQTT Configuration
mqtt:
  broker: "localhost"
  port: 1883
  client_id: "test-k2m-dynamic-bridge-client"
  # Dynamic topic mappings for Kafka→MQTT bridging
  # Messages from the Kafka source topic are published to the MQTT target topic
  topic_mappings:
    - source: "kafka-source-%d"
      target: "mqtt/dynamic/%d/events"

# Bridge Configuration
bridge:
  name: "test-k2m-dynamic-bridge"
  log_level: "debug"
  buffer_size: 100
`, testID, testID, testID)

	// Create temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Logf("Created config file at: %s", configPath)

	// Build the bridge binary
	binaryPath := filepath.Join(tmpDir, "kafka-mqtt-bridge")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/bridge")
	buildCmd.Dir = "/home/runner/work/kafka-mqtt-bridge/kafka-mqtt-bridge"
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build bridge binary: %v\nOutput: %s", err, output)
	}

	// Setup MQTT subscriber to receive messages
	mqttTopic := fmt.Sprintf("mqtt/dynamic/%d/events", testID)
	receivedMessages := make(chan string, 10)
	mqttSubscriber := setupMQTTSubscriber(t, mqttTopic, receivedMessages)
	defer mqttSubscriber.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

	// Setup Kafka writer and publish message BEFORE starting bridge
	// This ensures the message exists when the bridge starts reading
	kafkaWriter := setupKafkaWriter(t, kafkaSourceTopic)
	defer kafkaWriter.Close()

	testMessage := fmt.Sprintf("kafka-to-mqtt-dynamic-%d", testID)
	err := writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte(testMessage),
	}, 10)
	if err != nil {
		t.Fatalf("Failed to write message to Kafka: %v", err)
	}
	t.Logf("Published message to Kafka topic %s: %s", kafkaSourceTopic, testMessage)

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
			_ = bridgeCmd.Process.Signal(syscall.SIGTERM)
			_ = bridgeCmd.Wait()
		}
	}()

	// Give the bridge time to start and process
	time.Sleep(5 * time.Second)

	// Wait for the message to appear on MQTT
	select {
	case received := <-receivedMessages:
		if received != testMessage {
			t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
		} else {
			t.Logf("Successfully received bridged message on MQTT topic %s: %s", mqttTopic, received)
		}
	case <-time.After(15 * time.Second):
		t.Error("Timeout waiting for bridged message on MQTT")
	}

	t.Log("Successfully tested Kafka to MQTT dynamic topic mapping with binary")
}
