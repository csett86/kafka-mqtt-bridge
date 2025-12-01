// Package integration provides integration tests for the kafka-mqtt-bridge.
// This file contains tests for Azure Event Hubs support with SAS token authentication.
package integration

import (
	"context"
	"crypto/tls"
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

// Azure Event Hubs test configuration - must be set via environment variables
// These tests are skipped if the environment variables are not set
var (
	// Azure Event Hubs namespace endpoint (e.g., "your-namespace.servicebus.windows.net:9093")
	azureEventHubsBroker = os.Getenv("TEST_AZURE_EVENTHUBS_BROKER")
	// Azure Event Hubs connection string for SAS authentication
	azureEventHubsConnectionString = os.Getenv("TEST_AZURE_EVENTHUBS_CONNECTION_STRING")
	// Azure Event Hubs topic (Event Hub name)
	azureEventHubsTopic = os.Getenv("TEST_AZURE_EVENTHUBS_TOPIC")
)

// TestAzureEventHubsSASConnection tests basic connectivity to Azure Event Hubs
// using SAS token authentication over the Kafka protocol.
// This test requires the following environment variables:
//   - TEST_AZURE_EVENTHUBS_BROKER: Event Hubs namespace endpoint (e.g., "namespace.servicebus.windows.net:9093")
//   - TEST_AZURE_EVENTHUBS_CONNECTION_STRING: Full connection string with SAS key
//   - TEST_AZURE_EVENTHUBS_TOPIC: Event Hub name to use for testing
func TestAzureEventHubsSASConnection(t *testing.T) {
	if azureEventHubsBroker == "" || azureEventHubsConnectionString == "" || azureEventHubsTopic == "" {
		t.Skip("Skipping Azure Event Hubs test: TEST_AZURE_EVENTHUBS_BROKER, TEST_AZURE_EVENTHUBS_CONNECTION_STRING, and TEST_AZURE_EVENTHUBS_TOPIC environment variables must be set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create dialer with SASL PLAIN authentication (Azure Event Hubs uses $ConnectionString as username)
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		SASLMechanism: plain.Mechanism{
			Username: "$ConnectionString",
			Password: azureEventHubsConnectionString,
		},
	}

	// Connect to Azure Event Hubs
	conn, err := dialer.DialContext(ctx, "tcp", azureEventHubsBroker)
	if err != nil {
		t.Fatalf("Failed to connect to Azure Event Hubs: %v", err)
	}
	defer conn.Close()

	t.Log("Successfully connected to Azure Event Hubs with SAS authentication")

	// Verify connection by reading broker metadata
	brokers, err := conn.Brokers()
	if err != nil {
		t.Fatalf("Failed to get broker metadata: %v", err)
	}

	t.Logf("Connected to Azure Event Hubs, found %d broker(s)", len(brokers))
	for _, b := range brokers {
		t.Logf("  Broker: %s:%d", b.Host, b.Port)
	}
}

// TestAzureEventHubsProduceConsume tests message round-trip through Azure Event Hubs
func TestAzureEventHubsProduceConsume(t *testing.T) {
	if azureEventHubsBroker == "" || azureEventHubsConnectionString == "" || azureEventHubsTopic == "" {
		t.Skip("Skipping Azure Event Hubs test: TEST_AZURE_EVENTHUBS_BROKER, TEST_AZURE_EVENTHUBS_CONNECTION_STRING, and TEST_AZURE_EVENTHUBS_TOPIC environment variables must be set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	testID := time.Now().UnixNano()
	testIDStr := strconv.FormatInt(testID, 10)
	testMessage := "azure-eventhubs-test-message-" + testIDStr

	// Create transport with SASL PLAIN authentication
	transport := &kafka.Transport{
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		SASL: plain.Mechanism{
			Username: "$ConnectionString",
			Password: azureEventHubsConnectionString,
		},
	}

	// Create writer for Azure Event Hubs
	writer := &kafka.Writer{
		Addr:      kafka.TCP(azureEventHubsBroker),
		Topic:     azureEventHubsTopic,
		Balancer:  &kafka.LeastBytes{},
		Transport: transport,
		// Azure Event Hubs doesn't support auto topic creation
		AllowAutoTopicCreation: false,
		BatchTimeout:           100 * time.Millisecond,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
	}
	defer writer.Close()

	// Write test message
	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("test-key-" + testIDStr),
		Value: []byte(testMessage),
	})
	if err != nil {
		t.Fatalf("Failed to write message to Azure Event Hubs: %v", err)
	}

	t.Logf("Published message to Azure Event Hubs topic %s: %s", azureEventHubsTopic, testMessage)

	// Create dialer for reader
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		SASLMechanism: plain.Mechanism{
			Username: "$ConnectionString",
			Password: azureEventHubsConnectionString,
		},
	}

	// Create reader for Azure Event Hubs
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{azureEventHubsBroker},
		Topic:       azureEventHubsTopic,
		GroupID:     "test-azure-group-" + testIDStr,
		Dialer:      dialer,
		StartOffset: kafka.LastOffset, // Start from end to get our test message
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     100 * time.Millisecond,
	})
	defer reader.Close()

	// Read message with timeout
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()

	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("Failed to read message from Azure Event Hubs: %v", err)
	}

	received := string(msg.Value)
	if received != testMessage {
		t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
	} else {
		t.Logf("Successfully received message from Azure Event Hubs: %s", received)
	}

	t.Log("Successfully tested Azure Event Hubs produce/consume round-trip")
}

// TestKafkaToMQTTBridgeWithAzureEventHubs tests the bridge component with Azure Event Hubs
// as the Kafka backend using the built binary.
func TestKafkaToMQTTBridgeWithAzureEventHubs(t *testing.T) {
	if azureEventHubsBroker == "" || azureEventHubsConnectionString == "" || azureEventHubsTopic == "" {
		t.Skip("Skipping Azure Event Hubs bridge test: TEST_AZURE_EVENTHUBS_BROKER, TEST_AZURE_EVENTHUBS_CONNECTION_STRING, and TEST_AZURE_EVENTHUBS_TOPIC environment variables must be set")
	}

	// Check if MQTT broker is available
	if err := checkMQTTConnection(); err != nil {
		t.Skipf("Skipping Azure Event Hubs bridge test: MQTT not available: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Use unique IDs for this test
	testID := time.Now().UnixNano()
	testIDStr := strconv.FormatInt(testID, 10)
	mqttTopic := "mqtt/azure/bridge/test/" + testIDStr
	testMessage := "azure-bridge-test-message-" + testIDStr

	projectRoot := getProjectRoot()

	// Build explicit configuration values
	kafkaGroupID := "test-azure-bridge-group-" + testIDStr
	mqttClientID := "test-azure-bridge-" + testIDStr
	mqttPortStr := strconv.Itoa(mqttPort)

	// Create a temporary config file for the bridge with Azure Event Hubs configuration
	configContent := `
kafka:
  broker: "` + azureEventHubsBroker + `"
  group_id: "` + kafkaGroupID + `"
  sasl:
    enabled: true
    mechanism: "PLAIN"
    username: "$ConnectionString"
    password: "` + azureEventHubsConnectionString + `"
  tls:
    enabled: true
    insecure_skip_verify: false

mqtt:
  broker: "` + mqttBroker + `"
  port: ` + mqttPortStr + `
  client_id: "` + mqttClientID + `"

bridge:
  name: "test-azure-bridge"
  log_level: "debug"
  buffer_size: 100
  kafka_to_mqtt:
    source_topic: "` + azureEventHubsTopic + `"
    dest_topic: "` + mqttTopic + `"
`

	// Create temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0600); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Logf("Created Azure Event Hubs config file at: %s", configPath)

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

	// Create transport with SASL PLAIN authentication for writing test message
	transport := &kafka.Transport{
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		SASL: plain.Mechanism{
			Username: "$ConnectionString",
			Password: azureEventHubsConnectionString,
		},
	}

	// Setup Azure Event Hubs writer and publish message BEFORE starting bridge
	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP(azureEventHubsBroker),
		Topic:                  azureEventHubsTopic,
		Balancer:               &kafka.LeastBytes{},
		Transport:              transport,
		AllowAutoTopicCreation: false,
		BatchTimeout:           100 * time.Millisecond,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
	}
	defer kafkaWriter.Close()

	// Publish message to Azure Event Hubs
	err := kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte(testMessage),
	})
	if err != nil {
		t.Fatalf("Failed to write message to Azure Event Hubs: %v", err)
	}

	t.Logf("Published message to Azure Event Hubs topic %s: %s", azureEventHubsTopic, testMessage)

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

	t.Log("Azure Event Hubs bridge binary started, waiting for it to initialize...")

	// Wait for the message to appear on MQTT (forwarded by the bridge)
	select {
	case received := <-receivedMessages:
		if received != testMessage {
			t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
		} else {
			t.Logf("Successfully received Azure Event Hubs bridged message on MQTT: %s", received)
		}
	case <-time.After(30 * time.Second):
		t.Error("Timeout waiting for Azure Event Hubs bridged message on MQTT")
	}

	t.Log("Successfully tested Azure Event Hubs to MQTT bridge flow")
}

// TestMQTTToAzureEventHubsBridge tests the MQTT→Azure Event Hubs direction
func TestMQTTToAzureEventHubsBridge(t *testing.T) {
	if azureEventHubsBroker == "" || azureEventHubsConnectionString == "" || azureEventHubsTopic == "" {
		t.Skip("Skipping Azure Event Hubs bridge test: TEST_AZURE_EVENTHUBS_BROKER, TEST_AZURE_EVENTHUBS_CONNECTION_STRING, and TEST_AZURE_EVENTHUBS_TOPIC environment variables must be set")
	}

	// Check if MQTT broker is available
	if err := checkMQTTConnection(); err != nil {
		t.Skipf("Skipping Azure Event Hubs bridge test: MQTT not available: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Use unique IDs for this test
	testID := time.Now().UnixNano()
	testIDStr := strconv.FormatInt(testID, 10)
	mqttTopic := "mqtt/to-azure/test/" + testIDStr
	testMessage := "mqtt-to-azure-test-message-" + testIDStr

	projectRoot := getProjectRoot()

	// Build explicit configuration values
	kafkaGroupID := "test-mqtt-to-azure-group-" + testIDStr
	mqttClientID := "test-mqtt-to-azure-" + testIDStr
	mqttPortStr := strconv.Itoa(mqttPort)

	// Create a temporary config file for the bridge (MQTT→Azure Event Hubs)
	configContent := `
kafka:
  broker: "` + azureEventHubsBroker + `"
  group_id: "` + kafkaGroupID + `"
  sasl:
    enabled: true
    mechanism: "PLAIN"
    username: "$ConnectionString"
    password: "` + azureEventHubsConnectionString + `"
  tls:
    enabled: true
    insecure_skip_verify: false

mqtt:
  broker: "` + mqttBroker + `"
  port: ` + mqttPortStr + `
  client_id: "` + mqttClientID + `"

bridge:
  name: "test-mqtt-to-azure"
  log_level: "debug"
  buffer_size: 100
  mqtt_to_kafka:
    source_topic: "` + mqttTopic + `"
    dest_topic: "` + azureEventHubsTopic + `"
`

	// Create temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0600); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Logf("Created MQTT to Azure Event Hubs config file at: %s", configPath)

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

	t.Log("MQTT to Azure Event Hubs bridge binary started, waiting for it to initialize...")

	// Give bridge time to start and subscribe
	time.Sleep(3 * time.Second)

	// Create dialer for Azure Event Hubs reader
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		SASLMechanism: plain.Mechanism{
			Username: "$ConnectionString",
			Password: azureEventHubsConnectionString,
		},
	}

	// Setup Azure Event Hubs reader to consume messages forwarded by the bridge
	kafkaReaderGroupID := "test-mqtt-to-azure-read-group-" + testIDStr
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{azureEventHubsBroker},
		Topic:       azureEventHubsTopic,
		GroupID:     kafkaReaderGroupID,
		Dialer:      dialer,
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     100 * time.Millisecond,
	})
	defer kafkaReader.Close()

	// Setup MQTT publisher to publish message
	mqttPublisher := setupMQTTPublisher(t)
	defer mqttPublisher.Disconnect(250)

	// Publish message to MQTT - the bridge should forward it to Azure Event Hubs
	token := mqttPublisher.Publish(mqttTopic, 1, false, []byte(testMessage))
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to publish message to MQTT: %v", token.Error())
	}

	t.Logf("Published message to MQTT topic %s: %s", mqttTopic, testMessage)

	// Wait for the message to appear on Azure Event Hubs (forwarded by the bridge)
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()

	msg, err := kafkaReader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("Failed to read message from Azure Event Hubs: %v", err)
	}

	received := string(msg.Value)
	if received != testMessage {
		t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
	} else {
		t.Logf("Successfully received bridged message on Azure Event Hubs: %s", received)
	}

	t.Log("Successfully tested MQTT to Azure Event Hubs bridge flow")
}
