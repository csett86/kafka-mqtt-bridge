// Package integration provides integration tests for the kafka-mqtt-bridge.
// This file contains tests for Azure Event Hubs Emulator support.
// The tests use the Azure Event Hubs Emulator which is started via docker-compose.
package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// Azure Event Hubs Emulator configuration
// The emulator is started as part of the docker-compose.integration.yml
var (
	// Event Hubs Emulator Kafka endpoint (port 9092 - the emulator advertises this internally)
	eventHubsEmulatorBroker = getEnv("TEST_EVENTHUBS_EMULATOR_BROKER", "localhost:9092")
	// Event Hubs Emulator connection string for SASL authentication
	eventHubsEmulatorConnectionString = getEnv("TEST_EVENTHUBS_EMULATOR_CONNECTION_STRING",
		"Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")
	// Event Hub name (defined in test/eventhubs-emulator-config.json)
	eventHubsEmulatorTopic = getEnv("TEST_EVENTHUBS_EMULATOR_TOPIC", "eh1")
)

// getEventHubsDialer returns a dialer configured for Event Hubs Emulator with SASL
func getEventHubsDialer() *kafka.Dialer {
	mechanism := plain.Mechanism{
		Username: "$ConnectionString",
		Password: eventHubsEmulatorConnectionString,
	}

	return &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
	}
}

// checkEventHubsEmulatorConnection checks if the Event Hubs Emulator is available
func checkEventHubsEmulatorConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dialer := getEventHubsDialer()

	conn, err := dialer.DialContext(ctx, "tcp", eventHubsEmulatorBroker)
	if err != nil {
		return fmt.Errorf("failed to connect to Event Hubs Emulator: %w", err)
	}
	defer conn.Close()
	return nil
}

// TestEventHubsEmulatorConnection tests basic connectivity to Azure Event Hubs Emulator
// using the Kafka protocol with SASL/PLAIN authentication.
func TestEventHubsEmulatorConnection(t *testing.T) {
	if err := checkEventHubsEmulatorConnection(); err != nil {
		t.Skipf("Skipping Event Hubs Emulator test: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dialer := getEventHubsDialer()

	// Connect to Event Hubs Emulator
	conn, err := dialer.DialContext(ctx, "tcp", eventHubsEmulatorBroker)
	if err != nil {
		t.Fatalf("Failed to connect to Event Hubs Emulator: %v", err)
	}
	defer conn.Close()

	t.Log("Successfully connected to Event Hubs Emulator with SASL authentication")

	// Verify connection by reading partitions
	partitions, err := conn.ReadPartitions(eventHubsEmulatorTopic)
	if err != nil {
		t.Fatalf("Failed to read partitions: %v", err)
	}

	t.Logf("Found %d partitions for topic %s", len(partitions), eventHubsEmulatorTopic)
	t.Log("Event Hubs Emulator connection test passed")
}

// TestEventHubsEmulatorProduceConsume tests message round-trip through Event Hubs Emulator
func TestEventHubsEmulatorProduceConsume(t *testing.T) {
	if err := checkEventHubsEmulatorConnection(); err != nil {
		t.Skipf("Skipping Event Hubs Emulator test: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	testID := time.Now().UnixNano()
	testIDStr := strconv.FormatInt(testID, 10)
	testMessage := "eventhubs-emulator-test-message-" + testIDStr

	dialer := getEventHubsDialer()

	// Connect to leader for partition 0
	leaderConn, err := dialer.DialLeader(ctx, "tcp", eventHubsEmulatorBroker, eventHubsEmulatorTopic, 0)
	if err != nil {
		t.Fatalf("Failed to connect to Event Hubs Emulator leader: %v", err)
	}
	defer leaderConn.Close()

	// Write test message
	leaderConn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = leaderConn.WriteMessages(kafka.Message{
		Key:   []byte("test-key-" + testIDStr),
		Value: []byte(testMessage),
	})
	if err != nil {
		t.Fatalf("Failed to write message to Event Hubs Emulator: %v", err)
	}

	t.Logf("Published message to Event Hubs Emulator topic %s: %s", eventHubsEmulatorTopic, testMessage)

	// Create reader for Event Hubs Emulator with a unique group ID and start from end
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{eventHubsEmulatorBroker},
		Topic:       eventHubsEmulatorTopic,
		Partition:   0,
		Dialer:      dialer,
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     1 * time.Second,
	})
	defer reader.Close()

	// Seek to the beginning to read all messages, then find our message
	reader.SetOffset(kafka.FirstOffset)

	// Read messages until we find ours or timeout
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()

	var received string
	for {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("Failed to read message from Event Hubs Emulator: %v", err)
		}

		received = string(msg.Value)
		if received == testMessage {
			t.Logf("Successfully received message from Event Hubs Emulator: %s", received)
			break
		}
		t.Logf("Skipping older message: %s", received)
	}

	t.Log("Successfully tested Event Hubs Emulator produce/consume round-trip")
}

// TestKafkaToMQTTBridgeWithEventHubsEmulator tests the bridge component with Event Hubs Emulator
// as the Kafka backend using the built binary.
func TestKafkaToMQTTBridgeWithEventHubsEmulator(t *testing.T) {
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
	testIDStr := strconv.FormatInt(testID, 10)
	mqttTopic := "mqtt/eventhubs/bridge/test/" + testIDStr
	testMessage := "eventhubs-bridge-test-message-" + testIDStr

	projectRoot := getProjectRoot()

	// Build explicit configuration values
	kafkaGroupID := "test-eventhubs-bridge-group-" + testIDStr
	mqttClientID := "test-eventhubs-bridge-" + testIDStr
	mqttPortStr := strconv.Itoa(mqttPort)

	// Create a temporary config file for the bridge with Event Hubs Emulator configuration
	// Uses SASL/PLAIN authentication with $ConnectionString username
	configContent := `
kafka:
  broker: "` + eventHubsEmulatorBroker + `"
  group_id: "` + kafkaGroupID + `"
  sasl:
    enabled: true
    mechanism: "PLAIN"
    username: "$ConnectionString"
    password: "` + eventHubsEmulatorConnectionString + `"

mqtt:
  broker: "` + mqttBroker + `"
  port: ` + mqttPortStr + `
  client_id: "` + mqttClientID + `"

bridge:
  name: "test-eventhubs-bridge"
  log_level: "debug"
  buffer_size: 100
  kafka_to_mqtt:
    source_topic: "` + eventHubsEmulatorTopic + `"
    dest_topic: "` + mqttTopic + `"
`

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
	receivedMessages := make(chan string, 10)
	mqttClient := setupMQTTSubscriber(t, mqttTopic, receivedMessages)
	defer mqttClient.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

	// Publish message to Event Hubs Emulator using direct connection with SASL
	dialer := getEventHubsDialer()
	leaderConn, err := dialer.DialLeader(ctx, "tcp", eventHubsEmulatorBroker, eventHubsEmulatorTopic, 0)
	if err != nil {
		t.Fatalf("Failed to connect to Event Hubs Emulator: %v", err)
	}

	leaderConn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = leaderConn.WriteMessages(kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte(testMessage),
	})
	leaderConn.Close()

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

	// Wait for the message to appear on MQTT (forwarded by the bridge)
	select {
	case received := <-receivedMessages:
		if received != testMessage {
			t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
		} else {
			t.Logf("Successfully received Event Hubs Emulator bridged message on MQTT: %s", received)
		}
	case <-time.After(30 * time.Second):
		t.Error("Timeout waiting for Event Hubs Emulator bridged message on MQTT")
	}

	t.Log("Successfully tested Event Hubs Emulator to MQTT bridge flow")
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
	testIDStr := strconv.FormatInt(testID, 10)
	mqttTopic := "mqtt/to-eventhubs/test/" + testIDStr
	testMessage := "mqtt-to-eventhubs-test-message-" + testIDStr

	projectRoot := getProjectRoot()

	// Build explicit configuration values
	kafkaGroupID := "test-mqtt-to-eventhubs-group-" + testIDStr
	mqttClientID := "test-mqtt-to-eventhubs-" + testIDStr
	mqttPortStr := strconv.Itoa(mqttPort)

	// Create a temporary config file for the bridge (MQTT→Event Hubs Emulator)
	// Uses SASL/PLAIN authentication with $ConnectionString username
	configContent := `
kafka:
  broker: "` + eventHubsEmulatorBroker + `"
  group_id: "` + kafkaGroupID + `"
  sasl:
    enabled: true
    mechanism: "PLAIN"
    username: "$ConnectionString"
    password: "` + eventHubsEmulatorConnectionString + `"

mqtt:
  broker: "` + mqttBroker + `"
  port: ` + mqttPortStr + `
  client_id: "` + mqttClientID + `"

bridge:
  name: "test-mqtt-to-eventhubs"
  log_level: "debug"
  buffer_size: 100
  mqtt_to_kafka:
    source_topic: "` + mqttTopic + `"
    dest_topic: "` + eventHubsEmulatorTopic + `"
`

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

	// Setup Event Hubs Emulator reader to consume messages forwarded by the bridge (with SASL)
	dialer := getEventHubsDialer()
	kafkaReaderGroupID := "test-mqtt-to-eventhubs-read-group-" + testIDStr
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{eventHubsEmulatorBroker},
		Topic:       eventHubsEmulatorTopic,
		GroupID:     kafkaReaderGroupID,
		Dialer:      dialer,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     100 * time.Millisecond,
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
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()

	msg, err := kafkaReader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("Failed to read message from Event Hubs Emulator: %v", err)
	}

	received := string(msg.Value)
	if received != testMessage {
		t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
	} else {
		t.Logf("Successfully received bridged message on Event Hubs Emulator: %s", received)
	}

	t.Log("Successfully tested MQTT to Event Hubs Emulator bridge flow")
}
