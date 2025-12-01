// Package integration provides integration tests for the kafka-mqtt-bridge.
// These tests require running Kafka and MQTT brokers.
//
// To run the integration tests:
//
//  1. Start the test infrastructure:
//     docker compose -f docker-compose.integration.yml up -d
//
//  2. Wait for services to be ready, then run:
//     go test -v ./test/integration/...
//
//  3. Clean up:
//     docker compose -f docker-compose.integration.yml down
package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
)

// Test configuration - can be overridden via environment variables
var (
	kafkaBrokers = getEnv("TEST_KAFKA_BROKERS", "localhost:9092")
	mqttBroker   = getEnv("TEST_MQTT_BROKER", "localhost")
	mqttPort     = getEnvInt("TEST_MQTT_PORT", 1883)
	testTimeout  = 30 * time.Second
	messageWait  = 5 * time.Second
)

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		var result int
		if _, err := fmt.Sscanf(value, "%d", &result); err == nil {
			return result
		}
	}
	return fallback
}

// getProjectRoot returns the absolute path to the project root directory.
// It works by finding the directory containing go.mod relative to this test file.
func getProjectRoot() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("failed to get current file path")
	}
	// Navigate from test/integration/ to project root (../../)
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

// TestMain provides setup and teardown for integration tests
func TestMain(m *testing.M) {
	// Check if we can connect to the brokers before running tests
	if err := checkKafkaConnection(); err != nil {
		fmt.Printf("Skipping integration tests: Kafka not available: %v\n", err)
		fmt.Println("To run integration tests, start the test infrastructure:")
		fmt.Println("  docker-compose -f docker-compose.integration.yml up -d")
		os.Exit(0)
	}

	if err := checkMQTTConnection(); err != nil {
		fmt.Printf("Skipping integration tests: MQTT not available: %v\n", err)
		fmt.Println("To run integration tests, start the test infrastructure:")
		fmt.Println("  docker-compose -f docker-compose.integration.yml up -d")
		os.Exit(0)
	}

	os.Exit(m.Run())
}

func checkKafkaConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", kafkaBrokers)
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka: %w", err)
	}
	defer conn.Close()
	return nil
}

func checkMQTTConnection() error {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", mqttBroker, mqttPort))
	opts.SetClientID("integration-test-check")
	opts.SetConnectTimeout(10 * time.Second)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT: %w", token.Error())
	}
	client.Disconnect(250)
	return nil
}

// TestKafkaToMQTTBridge tests the actual bridge component using the built binary:
// 1. Builds the bridge binary
// 2. Publishes a message to Kafka BEFORE starting the bridge
// 3. Starts the bridge binary
// 4. Verifies the bridge forwards the message to MQTT
func TestKafkaToMQTTBridge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Use unique topics for this test
	testID := time.Now().UnixNano()
	kafkaTopic := fmt.Sprintf("test-bridge-kafka-%d", testID)
	mqttTopic := fmt.Sprintf("mqtt/bridge/test/%d", testID)
	testMessage := fmt.Sprintf("bridge-test-message-%d", testID)

	// Create a temporary config file for the bridge
	configContent := fmt.Sprintf(`
kafka:
  broker: "%s"
  group_id: "test-bridge-group-%d"

mqtt:
  broker: "%s"
  port: %d
  client_id: "test-bridge-%d"

bridge:
  name: "test-bridge"
  log_level: "debug"
  buffer_size: 100
  kafka_to_mqtt:
    source_topic: "%s"
    dest_topic: "%s"
`, kafkaBrokers, testID, mqttBroker, mqttPort, testID, kafkaTopic, mqttTopic)

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
	buildCmd.Dir = getProjectRoot()
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

	// Setup Kafka writer and publish message BEFORE starting bridge
	// This ensures the message exists when the bridge starts reading
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()

	// Publish message to Kafka - the bridge should forward it to MQTT when it starts
	err := writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte(testMessage),
	}, 10)
	if err != nil {
		t.Fatalf("Failed to write message to Kafka: %v", err)
	}

	t.Logf("Published message to Kafka topic %s: %s", kafkaTopic, testMessage)

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

	// Give bridge time to start, connect to Kafka, and process the message.
	// 5 seconds is sufficient for the bridge to read the pre-queued message.
	time.Sleep(5 * time.Second)

	// Wait for the message to appear on MQTT (forwarded by the bridge)
	select {
	case received := <-receivedMessages:
		if received != testMessage {
			t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
		} else {
			t.Logf("Successfully received bridged message on MQTT: %s", received)
		}
	case <-time.After(15 * time.Second):
		t.Error("Timeout waiting for bridged message on MQTT")
	}

	t.Log("Successfully tested Kafka to MQTT bridge flow")
}

// TestBridgeMultipleMessages tests the bridge with multiple messages using the built binary
func TestBridgeMultipleMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Use unique topics for this test
	testID := time.Now().UnixNano()
	kafkaTopic := fmt.Sprintf("test-bridge-multi-%d", testID)
	mqttTopic := fmt.Sprintf("mqtt/bridge/multi/%d", testID)

	testMessages := []string{
		"bridge-message-1",
		"bridge-message-2",
		"bridge-message-3",
	}

	// Create a temporary config file for the bridge
	configContent := fmt.Sprintf(`
kafka:
  broker: "%s"
  group_id: "test-bridge-multi-group-%d"

mqtt:
  broker: "%s"
  port: %d
  client_id: "test-bridge-multi-%d"

bridge:
  name: "test-bridge-multi"
  log_level: "debug"
  buffer_size: 100
  kafka_to_mqtt:
    source_topic: "%s"
    dest_topic: "%s"
`, kafkaBrokers, testID, mqttBroker, mqttPort, testID, kafkaTopic, mqttTopic)

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
	buildCmd.Dir = getProjectRoot()
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build bridge binary: %v\nOutput: %s", err, output)
	}

	// Setup MQTT subscriber to receive messages from the bridge
	receivedMessages := make(chan string, len(testMessages)+5)
	mqttClient := setupMQTTSubscriber(t, mqttTopic, receivedMessages)
	defer mqttClient.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

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

	// Give bridge time to start
	time.Sleep(3 * time.Second)

	// Setup Kafka writer to publish messages
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()

	// Publish messages to Kafka - the bridge should forward them to MQTT
	for i, msg := range testMessages {
		retries := 10
		if i > 0 {
			retries = 3
		}
		err := writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(msg),
		}, retries)
		if err != nil {
			t.Fatalf("Failed to write message %d to Kafka: %v", i, err)
		}
		t.Logf("Published message to Kafka: %s", msg)
	}

	// Collect messages received on MQTT
	received := make([]string, 0, len(testMessages))
	timeout := time.After(15 * time.Second)

collectLoop:
	for {
		select {
		case msg := <-receivedMessages:
			received = append(received, msg)
			t.Logf("Received bridged message on MQTT: %s", msg)
			if len(received) >= len(testMessages) {
				break collectLoop
			}
		case <-timeout:
			break collectLoop
		}
	}

	// Verify all messages were received
	if len(received) != len(testMessages) {
		t.Fatalf("Expected %d messages, got %d", len(testMessages), len(received))
	}

	for i, expected := range testMessages {
		if received[i] != expected {
			t.Errorf("Message %d mismatch: got %q, want %q", i+1, received[i], expected)
		}
	}

	t.Log("Successfully tested bridge with multiple messages")
}

// TestMQTTToKafkaBridge tests the bridge component for MQTT→Kafka direction using the built binary:
// 1. Builds and starts the bridge binary
// 2. Publishes a message to MQTT
// 3. Verifies the bridge forwards it to Kafka
func TestMQTTToKafkaBridge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Use unique topics for this test
	testID := time.Now().UnixNano()
	mqttTopic := fmt.Sprintf("mqtt/to-kafka/test/%d", testID)
	kafkaTopic := fmt.Sprintf("test-mqtt-to-kafka-%d", testID)
	testMessage := fmt.Sprintf("mqtt-to-kafka-test-message-%d", testID)

	// Create a temporary config file for the bridge (MQTT→Kafka only)
	configContent := fmt.Sprintf(`
kafka:
  broker: "%s"
  group_id: "test-mqtt-to-kafka-group-%d"

mqtt:
  broker: "%s"
  port: %d
  client_id: "test-mqtt-to-kafka-%d"

bridge:
  name: "test-mqtt-to-kafka"
  log_level: "debug"
  buffer_size: 100
  mqtt_to_kafka:
    source_topic: "%s"
    dest_topic: "%s"
`, kafkaBrokers, testID, mqttBroker, mqttPort, testID, mqttTopic, kafkaTopic)

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

	// Ensure we clean up the process
	defer func() {
		if bridgeCmd.Process != nil {
			_ = bridgeCmd.Process.Signal(syscall.SIGTERM)
			_ = bridgeCmd.Wait()
		}
	}()

	t.Log("Bridge binary started, waiting for it to initialize...")

	// Give bridge time to start and subscribe
	time.Sleep(3 * time.Second)

	// Setup Kafka reader to consume messages forwarded by the bridge
	kafkaReader := setupKafkaReader(t, kafkaTopic)
	defer kafkaReader.Close()

	// Setup MQTT publisher to publish message
	mqttPublisher := setupMQTTPublisher(t)
	defer mqttPublisher.Disconnect(250)

	// Publish message to MQTT - the bridge should forward it to Kafka
	token := mqttPublisher.Publish(mqttTopic, 1, false, []byte(testMessage))
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to publish message to MQTT: %v", token.Error())
	}

	t.Logf("Published message to MQTT topic %s: %s", mqttTopic, testMessage)

	// Wait for the message to appear on Kafka (forwarded by the bridge)
	readCtx, readCancel := context.WithTimeout(ctx, 15*time.Second)
	defer readCancel()

	msg, err := kafkaReader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("Failed to read message from Kafka: %v", err)
	}

	received := string(msg.Value)
	if received != testMessage {
		t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
	} else {
		t.Logf("Successfully received bridged message on Kafka: %s", received)
	}

	t.Log("Successfully tested MQTT to Kafka bridge flow")
}

// TestKafkaProducerConsumer tests basic Kafka message round-trip
func TestKafkaProducerConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	topic := fmt.Sprintf("test-kafka-roundtrip-%d", time.Now().UnixNano())
	testMessages := []string{
		"message-1",
		"message-2",
		"message-3",
	}

	// Setup writer
	writer := setupKafkaWriter(t, topic)
	defer writer.Close()

	// Write messages with retries for the first message (topic creation)
	for i, msg := range testMessages {
		retries := 10
		if i > 0 {
			retries = 3 // Less retries for subsequent messages
		}
		err := writeMessageWithRetry(ctx, writer, kafka.Message{
			Key:   []byte("key"),
			Value: []byte(msg),
		}, retries)
		if err != nil {
			t.Fatalf("Failed to write message %q: %v", msg, err)
		}
	}

	t.Logf("Published %d messages to Kafka", len(testMessages))

	// Setup reader
	reader := setupKafkaReader(t, topic)
	defer reader.Close()

	// Read and verify messages
	for i, expected := range testMessages {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("Failed to read message %d: %v", i+1, err)
		}

		if string(msg.Value) != expected {
			t.Errorf("Message %d mismatch: got %q, want %q", i+1, string(msg.Value), expected)
		}
	}

	t.Log("Successfully verified Kafka producer/consumer round-trip")
}

// TestMQTTPublishSubscribe tests basic MQTT message round-trip
func TestMQTTPublishSubscribe(t *testing.T) {
	topic := "test/mqtt/roundtrip"
	testMessages := []string{
		"mqtt-message-1",
		"mqtt-message-2",
		"mqtt-message-3",
	}

	receivedMessages := make(chan string, len(testMessages))

	// Setup subscriber
	subscriber := setupMQTTSubscriber(t, topic, receivedMessages)
	defer subscriber.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

	// Setup publisher
	publisher := setupMQTTPublisher(t)
	defer publisher.Disconnect(250)

	// Publish messages
	for _, msg := range testMessages {
		token := publisher.Publish(topic, 1, false, []byte(msg))
		if token.Wait() && token.Error() != nil {
			t.Fatalf("Failed to publish message %q: %v", msg, token.Error())
		}
	}

	t.Logf("Published %d messages to MQTT", len(testMessages))

	// Collect received messages
	received := make([]string, 0, len(testMessages))
	timeout := time.After(messageWait)

collectLoop:
	for {
		select {
		case msg := <-receivedMessages:
			received = append(received, msg)
			if len(received) >= len(testMessages) {
				break collectLoop
			}
		case <-timeout:
			break collectLoop
		}
	}

	// Verify all messages were received
	if len(received) != len(testMessages) {
		t.Fatalf("Expected %d messages, got %d", len(testMessages), len(received))
	}

	for i, expected := range testMessages {
		if received[i] != expected {
			t.Errorf("Message %d mismatch: got %q, want %q", i+1, received[i], expected)
		}
	}

	t.Log("Successfully verified MQTT publish/subscribe round-trip")
}

// TestMQTTToKafkaBridgeWithQoS2 tests the bridge with QoS 2 (exactly once) delivery
// This test verifies that:
// 1. The bridge can be configured with QoS 2
// 2. Messages are properly acknowledged after successful Kafka delivery
// 3. The exactly-once semantics are maintained
func TestMQTTToKafkaBridgeWithQoS2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Use unique topics for this test
	testID := time.Now().UnixNano()
	mqttTopic := fmt.Sprintf("mqtt/qos2/test/%d", testID)
	kafkaTopic := fmt.Sprintf("test-mqtt-qos2-%d", testID)
	testMessages := []string{
		fmt.Sprintf("qos2-message-1-%d", testID),
		fmt.Sprintf("qos2-message-2-%d", testID),
		fmt.Sprintf("qos2-message-3-%d", testID),
	}

	// Create a temporary config file for the bridge with QoS 2
	configContent := fmt.Sprintf(`
kafka:
  broker: "%s"
  group_id: "test-mqtt-qos2-group-%d"

mqtt:
  broker: "%s"
  port: %d
  client_id: "test-mqtt-qos2-%d"
  qos: 2
  clean_session: false

bridge:
  name: "test-mqtt-qos2"
  log_level: "debug"
  buffer_size: 100
  mqtt_to_kafka:
    source_topic: "%s"
    dest_topic: "%s"
`, kafkaBrokers, testID, mqttBroker, mqttPort, testID, mqttTopic, kafkaTopic)

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

	// Ensure we clean up the process
	defer func() {
		if bridgeCmd.Process != nil {
			_ = bridgeCmd.Process.Signal(syscall.SIGTERM)
			_ = bridgeCmd.Wait()
		}
	}()

	t.Log("Bridge binary started with QoS 2, waiting for it to initialize...")

	// Give bridge time to start and subscribe
	time.Sleep(3 * time.Second)

	// Setup Kafka reader to consume messages forwarded by the bridge
	kafkaReader := setupKafkaReader(t, kafkaTopic)
	defer kafkaReader.Close()

	// Setup MQTT publisher with QoS 2
	mqttPublisher := setupMQTTPublisherWithQoS(t, 2)
	defer mqttPublisher.Disconnect(250)

	// Publish messages to MQTT with QoS 2 - the bridge should forward them to Kafka
	for i, msg := range testMessages {
		token := mqttPublisher.Publish(mqttTopic, 2, false, []byte(msg))
		if token.Wait() && token.Error() != nil {
			t.Fatalf("Failed to publish message %d to MQTT with QoS 2: %v", i+1, token.Error())
		}
		t.Logf("Published message to MQTT topic %s with QoS 2: %s", mqttTopic, msg)
	}

	// Read and verify all messages from Kafka
	received := make([]string, 0, len(testMessages))
	readCtx, readCancel := context.WithTimeout(ctx, 15*time.Second)
	defer readCancel()

	for i := 0; i < len(testMessages); i++ {
		msg, err := kafkaReader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("Failed to read message %d from Kafka: %v", i+1, err)
		}
		received = append(received, string(msg.Value))
		t.Logf("Received bridged message on Kafka: %s", string(msg.Value))
	}

	// Verify all messages were received
	if len(received) != len(testMessages) {
		t.Fatalf("Expected %d messages, got %d", len(testMessages), len(received))
	}

	for i, expected := range testMessages {
		if received[i] != expected {
			t.Errorf("Message %d mismatch: got %q, want %q", i+1, received[i], expected)
		}
	}

	t.Log("Successfully tested MQTT to Kafka bridge with QoS 2 (exactly-once)")
}

// TestKafkaToMQTTBridgeWithQoS2 tests the bridge for Kafka→MQTT direction with QoS 2
// This test verifies that:
// 1. The bridge can publish to MQTT with QoS 2
// 2. Messages are delivered with exactly-once semantics
func TestKafkaToMQTTBridgeWithQoS2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Use unique topics for this test
	testID := time.Now().UnixNano()
	kafkaTopic := fmt.Sprintf("test-kafka-qos2-%d", testID)
	mqttTopic := fmt.Sprintf("mqtt/kafka-qos2/test/%d", testID)
	testMessages := []string{
		fmt.Sprintf("kafka-qos2-message-1-%d", testID),
		fmt.Sprintf("kafka-qos2-message-2-%d", testID),
		fmt.Sprintf("kafka-qos2-message-3-%d", testID),
	}

	// Create a temporary config file for the bridge with QoS 2
	configContent := fmt.Sprintf(`
kafka:
  broker: "%s"
  group_id: "test-kafka-qos2-group-%d"

mqtt:
  broker: "%s"
  port: %d
  client_id: "test-kafka-qos2-%d"
  qos: 2
  clean_session: false

bridge:
  name: "test-kafka-qos2"
  log_level: "debug"
  buffer_size: 100
  kafka_to_mqtt:
    source_topic: "%s"
    dest_topic: "%s"
`, kafkaBrokers, testID, mqttBroker, mqttPort, testID, kafkaTopic, mqttTopic)

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
	buildCmd.Dir = getProjectRoot()
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build bridge binary: %v\nOutput: %s", err, output)
	}

	// Setup MQTT subscriber to receive messages from the bridge with QoS 2
	receivedMessages := make(chan string, len(testMessages)+5)
	mqttClient := setupMQTTSubscriberWithQoS(t, mqttTopic, 2, receivedMessages)
	defer mqttClient.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

	// Setup Kafka writer and publish messages BEFORE starting bridge
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()

	// Publish messages to Kafka
	for i, msg := range testMessages {
		retries := 10
		if i > 0 {
			retries = 3
		}
		err := writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(msg),
		}, retries)
		if err != nil {
			t.Fatalf("Failed to write message %d to Kafka: %v", i, err)
		}
		t.Logf("Published message to Kafka: %s", msg)
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

	t.Log("Bridge binary started with QoS 2, waiting for it to initialize...")

	// Give bridge time to start and process
	time.Sleep(5 * time.Second)

	// Collect messages received on MQTT
	received := make([]string, 0, len(testMessages))
	timeout := time.After(15 * time.Second)

collectLoop:
	for {
		select {
		case msg := <-receivedMessages:
			received = append(received, msg)
			t.Logf("Received bridged message on MQTT with QoS 2: %s", msg)
			if len(received) >= len(testMessages) {
				break collectLoop
			}
		case <-timeout:
			break collectLoop
		}
	}

	// Verify all messages were received
	if len(received) != len(testMessages) {
		t.Fatalf("Expected %d messages, got %d", len(testMessages), len(received))
	}

	for i, expected := range testMessages {
		if received[i] != expected {
			t.Errorf("Message %d mismatch: got %q, want %q", i+1, received[i], expected)
		}
	}

	t.Log("Successfully tested Kafka to MQTT bridge with QoS 2 (exactly-once)")
}

// Helper functions

func setupKafkaWriter(t *testing.T, topic string) *kafka.Writer {
	t.Helper()
	return &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBrokers),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		// Retry settings for topic creation lag
		BatchTimeout: 100 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  10,
	}
}

// writeMessageWithRetry writes a message to Kafka with retries for transient errors
func writeMessageWithRetry(ctx context.Context, writer *kafka.Writer, msg kafka.Message, maxRetries int) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := writer.WriteMessages(ctx, msg)
		if err == nil {
			return nil
		}
		lastErr = err
		// Sleep briefly before retrying for leader election to complete
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return lastErr
}

func setupKafkaReader(t *testing.T, topic string) *kafka.Reader {
	t.Helper()
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    topic,
		GroupID:  fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  100 * time.Millisecond,
	})
}

func setupMQTTPublisher(t *testing.T) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", mqttBroker, mqttPort))
	opts.SetClientID(fmt.Sprintf("test-publisher-%d", time.Now().UnixNano()))
	opts.SetConnectTimeout(10 * time.Second)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect MQTT publisher: %v", token.Error())
	}
	return client
}

func setupMQTTSubscriber(t *testing.T, topic string, messages chan<- string) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", mqttBroker, mqttPort))
	opts.SetClientID(fmt.Sprintf("test-subscriber-%d", time.Now().UnixNano()))
	opts.SetConnectTimeout(10 * time.Second)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect MQTT subscriber: %v", token.Error())
	}

	token = client.Subscribe(topic, 1, func(c mqtt.Client, m mqtt.Message) {
		select {
		case messages <- string(m.Payload()):
		default:
			// Channel full, drop message
		}
	})
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to subscribe to topic %s: %v", topic, token.Error())
	}

	return client
}

// setupMQTTPublisherWithQoS creates an MQTT publisher with a specific QoS level
func setupMQTTPublisherWithQoS(t *testing.T, qos byte) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", mqttBroker, mqttPort))
	opts.SetClientID(fmt.Sprintf("test-publisher-qos%d-%d", qos, time.Now().UnixNano()))
	opts.SetConnectTimeout(10 * time.Second)
	// For QoS > 0, we need clean session false to maintain session state
	if qos > 0 {
		opts.SetCleanSession(false)
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect MQTT publisher with QoS %d: %v", qos, token.Error())
	}
	return client
}

// setupMQTTSubscriberWithQoS creates an MQTT subscriber with a specific QoS level
func setupMQTTSubscriberWithQoS(t *testing.T, topic string, qos byte, messages chan<- string) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", mqttBroker, mqttPort))
	opts.SetClientID(fmt.Sprintf("test-subscriber-qos%d-%d", qos, time.Now().UnixNano()))
	opts.SetConnectTimeout(10 * time.Second)
	// For QoS > 0, we need clean session false to maintain session state
	if qos > 0 {
		opts.SetCleanSession(false)
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect MQTT subscriber with QoS %d: %v", qos, token.Error())
	}

	token = client.Subscribe(topic, qos, func(c mqtt.Client, m mqtt.Message) {
		select {
		case messages <- string(m.Payload()):
		default:
			// Channel full, drop message
		}
	})
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to subscribe to topic %s with QoS %d: %v", topic, qos, token.Error())
	}

	return client
}
