// Package integration provides integration tests for the kafka-mqtt-bridge.
// This file contains tests for Azure Event Hubs Emulator support.
// The tests use the Azure Event Hubs Emulator which provides Kafka protocol compatibility.
//
// These tests run as part of the integration test suite when the infrastructure is started
// with docker compose -f docker-compose.integration.yml up -d
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
	"github.com/segmentio/kafka-go/sasl/plain"
)

// Azure Event Hubs Emulator configuration
// These defaults work with docker-compose.integration.yml (Event Hubs on port 9093)
var (
	// Event Hubs Emulator Kafka broker endpoint (port 9093 to avoid conflict with regular Kafka)
	eventHubsEmulatorBroker = getEnv("TEST_EVENTHUBS_BROKER", "localhost:9093")
	// Event Hubs Emulator connection string (used for SASL authentication)
	// The SAS_KEY_VALUE is a placeholder that works with the emulator
	eventHubsEmulatorConnectionString = getEnv("TEST_EVENTHUBS_CONNECTION_STRING",
		"Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")
	// Event Hub name (must match the config in eventhubs-emulator-config.json)
	eventHubsEmulatorTopic = getEnv("TEST_EVENTHUBS_TOPIC", "eh1")
)

// checkEventHubsEmulatorConnection checks if the Event Hubs Emulator is available
func checkEventHubsEmulatorConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create dialer with SASL PLAIN authentication
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		SASLMechanism: plain.Mechanism{
			Username: "$ConnectionString",
			Password: eventHubsEmulatorConnectionString,
		},
	}

	conn, err := dialer.DialContext(ctx, "tcp", eventHubsEmulatorBroker)
	if err != nil {
		return fmt.Errorf("failed to connect to Event Hubs Emulator: %w", err)
	}
	defer conn.Close()
	return nil
}

// TestEventHubsEmulatorConnection tests basic connectivity to the Azure Event Hubs Emulator
// using SASL PLAIN authentication over the Kafka protocol.
// This test uses the local Event Hubs Emulator started via docker-compose.eventhubs.yml
func TestEventHubsEmulatorConnection(t *testing.T) {
	if err := checkEventHubsEmulatorConnection(); err != nil {
		t.Skipf("Skipping Event Hubs Emulator test: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create dialer with SASL PLAIN authentication
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		SASLMechanism: plain.Mechanism{
			Username: "$ConnectionString",
			Password: eventHubsEmulatorConnectionString,
		},
	}

	// Connect to Event Hubs Emulator
	conn, err := dialer.DialContext(ctx, "tcp", eventHubsEmulatorBroker)
	if err != nil {
		t.Fatalf("Failed to connect to Event Hubs Emulator: %v", err)
	}
	defer conn.Close()

	t.Log("Successfully connected to Event Hubs Emulator with SASL authentication")

	// Verify connection by reading broker metadata
	brokers, err := conn.Brokers()
	if err != nil {
		t.Fatalf("Failed to get broker metadata: %v", err)
	}

	t.Logf("Connected to Event Hubs Emulator, found %d broker(s)", len(brokers))
	for _, b := range brokers {
		t.Logf("  Broker: %s:%d", b.Host, b.Port)
	}
}

// TestEventHubsEmulatorProduceConsume tests message round-trip through the Event Hubs Emulator
func TestEventHubsEmulatorProduceConsume(t *testing.T) {
	if err := checkEventHubsEmulatorConnection(); err != nil {
		t.Skipf("Skipping Event Hubs Emulator test: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	testID := time.Now().UnixNano()
	testMessage := fmt.Sprintf("eventhubs-emulator-test-message-%d", testID)

	// Create dialer for reader
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		SASLMechanism: plain.Mechanism{
			Username: "$ConnectionString",
			Password: eventHubsEmulatorConnectionString,
		},
	}

	// Create reader for Event Hubs Emulator BEFORE writing to ensure we catch the message
	// Use a unique consumer group to start fresh
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{eventHubsEmulatorBroker},
		Topic:       eventHubsEmulatorTopic,
		GroupID:     fmt.Sprintf("test-emulator-group-%d", testID),
		Dialer:      dialer,
		StartOffset: kafka.FirstOffset, // Start from beginning for this consumer group
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
	})
	defer reader.Close()

	// Create transport with SASL PLAIN authentication (no TLS for emulator)
	transport := &kafka.Transport{
		SASL: plain.Mechanism{
			Username: "$ConnectionString",
			Password: eventHubsEmulatorConnectionString,
		},
	}

	// Create writer for Event Hubs Emulator
	writer := &kafka.Writer{
		Addr:      kafka.TCP(eventHubsEmulatorBroker),
		Topic:     eventHubsEmulatorTopic,
		Balancer:  &kafka.LeastBytes{},
		Transport: transport,
		// Event Hubs doesn't support auto topic creation
		AllowAutoTopicCreation: false,
		BatchTimeout:           100 * time.Millisecond,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
	}
	defer writer.Close()

	// Write test message
	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("test-key-%d", testID)),
		Value: []byte(testMessage),
	})
	if err != nil {
		t.Fatalf("Failed to write message to Event Hubs Emulator: %v", err)
	}

	t.Logf("Published message to Event Hubs Emulator topic %s: %s", eventHubsEmulatorTopic, testMessage)

	// Read messages and look for our test message
	// The readCtx timeout ensures we don't wait forever - ReadMessage will return
	// context.DeadlineExceeded when the timeout is reached
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()

	for {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("Failed to read message from Event Hubs Emulator: %v", err)
		}

		received := string(msg.Value)
		if received == testMessage {
			t.Logf("Successfully received message from Event Hubs Emulator: %s", received)
			break
		}
		// Skip messages from previous test runs
		t.Logf("Skipping old message: %s", received)
	}

	t.Log("Successfully tested Event Hubs Emulator produce/consume round-trip")
}

// TestEventHubsEmulatorToMQTTBridge tests the bridge component with Event Hubs Emulator
// as the Kafka backend using the built binary.
func TestEventHubsEmulatorToMQTTBridge(t *testing.T) {
	if err := checkEventHubsEmulatorConnection(); err != nil {
		t.Skipf("Skipping Event Hubs Emulator bridge test: %v", err)
	}

	// Check if MQTT broker is available
	if err := checkMQTTConnection(); err != nil {
		t.Skipf("Skipping Event Hubs Emulator bridge test: MQTT not available: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Use unique IDs for this test
	testID := time.Now().UnixNano()
	mqttTopic := fmt.Sprintf("mqtt/eventhubs/bridge/test/%d", testID)
	testMessage := fmt.Sprintf("eventhubs-bridge-test-message-%d", testID)

	projectRoot := getProjectRoot()

	// Create a temporary config file for the bridge with Event Hubs Emulator configuration
	// Note: No TLS for the emulator
	configContent := fmt.Sprintf(`
kafka:
  broker: "%s"
  group_id: "test-eventhubs-bridge-group-%d"
  sasl:
    enabled: true
    mechanism: "PLAIN"
    username: "$ConnectionString"
    password: "%s"
  tls:
    enabled: false

mqtt:
  broker: "%s"
  port: %d
  client_id: "test-eventhubs-bridge-%d"

bridge:
  name: "test-eventhubs-bridge"
  log_level: "debug"
  buffer_size: 100
  kafka_to_mqtt:
    source_topic: "%s"
    dest_topic: "%s"
`, eventHubsEmulatorBroker, testID, eventHubsEmulatorConnectionString, mqttBroker, mqttPort, testID, eventHubsEmulatorTopic, mqttTopic)

	// Create temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0600); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Logf("Created Event Hubs Emulator config file at: %s", configPath)

	// Build the bridge binary
	binaryPath := filepath.Join(tmpDir, "kafka-mqtt-bridge")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/bridge")
	buildCmd.Dir = projectRoot
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build bridge binary: %v\nOutput: %s", err, output)
	}

	t.Logf("Built bridge binary at: %s", binaryPath)

	// Setup MQTT subscriber to receive messages from the bridge
	// Buffer size of 10 is sufficient for this test since we process messages as they arrive
	receivedMessages := make(chan string, 10)
	mqttClient := setupMQTTSubscriber(t, mqttTopic, receivedMessages)
	defer mqttClient.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

	// Create transport with SASL PLAIN authentication for writing test message
	transport := &kafka.Transport{
		SASL: plain.Mechanism{
			Username: "$ConnectionString",
			Password: eventHubsEmulatorConnectionString,
		},
	}

	// Setup Event Hubs Emulator writer and publish message BEFORE starting bridge
	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP(eventHubsEmulatorBroker),
		Topic:                  eventHubsEmulatorTopic,
		Balancer:               &kafka.LeastBytes{},
		Transport:              transport,
		AllowAutoTopicCreation: false,
		BatchTimeout:           100 * time.Millisecond,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
	}
	defer kafkaWriter.Close()

	// Publish message to Event Hubs Emulator
	err := kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte(testMessage),
	})
	if err != nil {
		t.Fatalf("Failed to write message to Event Hubs Emulator: %v", err)
	}

	t.Logf("Published message to Event Hubs Emulator topic %s: %s", eventHubsEmulatorTopic, testMessage)

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

	t.Log("Event Hubs Emulator bridge binary started, waiting for it to initialize...")

	// Wait for our specific message to appear on MQTT (forwarded by the bridge)
	// The bridge may forward old messages first, so we wait for our specific message
	timeout := time.After(30 * time.Second)
	for {
		select {
		case received := <-receivedMessages:
			if received == testMessage {
				t.Logf("Successfully received Event Hubs Emulator bridged message on MQTT: %s", received)
				t.Log("Successfully tested Event Hubs Emulator to MQTT bridge flow")
				return
			}
			// Skip old messages from previous test runs
			t.Logf("Skipping old message: %s", received)
		case <-timeout:
			t.Error("Timeout waiting for Event Hubs Emulator bridged message on MQTT")
			return
		}
	}
}

// TestMQTTToEventHubsEmulatorBridge tests the MQTT→Event Hubs Emulator direction
func TestMQTTToEventHubsEmulatorBridge(t *testing.T) {
	if err := checkEventHubsEmulatorConnection(); err != nil {
		t.Skipf("Skipping Event Hubs Emulator bridge test: %v", err)
	}

	// Check if MQTT broker is available
	if err := checkMQTTConnection(); err != nil {
		t.Skipf("Skipping Event Hubs Emulator bridge test: MQTT not available: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Use unique IDs for this test
	testID := time.Now().UnixNano()
	mqttTopic := fmt.Sprintf("mqtt/to-eventhubs/test/%d", testID)
	testMessage := fmt.Sprintf("mqtt-to-eventhubs-test-message-%d", testID)

	projectRoot := getProjectRoot()

	// Create a temporary config file for the bridge (MQTT→Event Hubs Emulator)
	configContent := fmt.Sprintf(`
kafka:
  broker: "%s"
  group_id: "test-mqtt-to-eventhubs-group-%d"
  sasl:
    enabled: true
    mechanism: "PLAIN"
    username: "$ConnectionString"
    password: "%s"
  tls:
    enabled: false

mqtt:
  broker: "%s"
  port: %d
  client_id: "test-mqtt-to-eventhubs-%d"

bridge:
  name: "test-mqtt-to-eventhubs"
  log_level: "debug"
  buffer_size: 100
  mqtt_to_kafka:
    source_topic: "%s"
    dest_topic: "%s"
`, eventHubsEmulatorBroker, testID, eventHubsEmulatorConnectionString, mqttBroker, mqttPort, testID, mqttTopic, eventHubsEmulatorTopic)

	// Create temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0600); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Logf("Created MQTT to Event Hubs Emulator config file at: %s", configPath)

	// Build the bridge binary
	binaryPath := filepath.Join(tmpDir, "kafka-mqtt-bridge")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/bridge")
	buildCmd.Dir = projectRoot
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

	// Ensure we clean up the process
	defer func() {
		if bridgeCmd.Process != nil {
			_ = bridgeCmd.Process.Signal(syscall.SIGTERM)
			_ = bridgeCmd.Wait()
		}
	}()

	t.Log("MQTT to Event Hubs Emulator bridge binary started, waiting for it to initialize...")

	// Give bridge time to start and subscribe
	time.Sleep(3 * time.Second)

	// Create dialer for Event Hubs Emulator reader
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		SASLMechanism: plain.Mechanism{
			Username: "$ConnectionString",
			Password: eventHubsEmulatorConnectionString,
		},
	}

	// Setup Event Hubs Emulator reader to consume messages forwarded by the bridge
	// Use FirstOffset with a unique consumer group to read all messages
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{eventHubsEmulatorBroker},
		Topic:       eventHubsEmulatorTopic,
		GroupID:     fmt.Sprintf("test-mqtt-to-eventhubs-read-group-%d", testID),
		Dialer:      dialer,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
	})
	defer kafkaReader.Close()

	// Setup MQTT publisher to publish message
	mqttPublisher := setupMQTTPublisher(t)
	defer mqttPublisher.Disconnect(250)

	// Publish message to MQTT - the bridge should forward it to Event Hubs Emulator
	token := mqttPublisher.Publish(mqttTopic, 1, false, []byte(testMessage))
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to publish message to MQTT: %v", token.Error())
	}

	t.Logf("Published message to MQTT topic %s: %s", mqttTopic, testMessage)

	// Wait for the message to appear on Event Hubs Emulator (forwarded by the bridge)
	// Loop through messages until we find our specific test message
	// The readCtx timeout ensures we don't wait forever - ReadMessage will return
	// context.DeadlineExceeded when the timeout is reached
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()

	for {
		msg, err := kafkaReader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("Failed to read message from Event Hubs Emulator: %v", err)
		}

		received := string(msg.Value)
		if received == testMessage {
			t.Logf("Successfully received bridged message on Event Hubs Emulator: %s", received)
			t.Log("Successfully tested MQTT to Event Hubs Emulator bridge flow")
			return
		}
		// Skip messages from previous test runs
		t.Logf("Skipping old message: %s", received)
	}
}
