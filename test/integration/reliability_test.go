// Package integration provides integration tests for the kafka-mqtt-bridge.
// These tests validate connection recovery behavior.
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

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
)

// TestMQTTConnectionRecovery tests that the bridge recovers after MQTT broker restart
// This test:
// 1. Starts the bridge
// 2. Verifies message flow works
// 3. Restarts the MQTT broker (mosquitto container)
// 4. Verifies the bridge reconnects and resumes operation
func TestMQTTConnectionRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Use unique topics for this test
	testID := time.Now().UnixNano()
	mqttTopic := fmt.Sprintf("mqtt/recovery/test/%d", testID)
	kafkaTopic := fmt.Sprintf("test-mqtt-recovery-%d", testID)

	// Create a temporary config file for the bridge (MQTT→Kafka)
	configContent := fmt.Sprintf(`
kafka:
  broker: "%s"
  dest_topic: "%s"
  group_id: "test-mqtt-recovery-group-%d"

mqtt:
  broker: "%s"
  port: %d
  source_topic: "%s"
  client_id: "test-mqtt-recovery-%d"

bridge:
  name: "test-mqtt-recovery"
  log_level: "debug"
  buffer_size: 100
`, kafkaBrokers, kafkaTopic, testID, mqttBroker, mqttPort, mqttTopic, testID)

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

	// Phase 1: Verify initial message flow works
	t.Log("Phase 1: Verifying initial message flow...")
	testMessage1 := fmt.Sprintf("before-restart-message-%d", testID)

	mqttPublisher := setupMQTTPublisher(t)
	token := mqttPublisher.Publish(mqttTopic, 1, false, []byte(testMessage1))
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to publish message to MQTT: %v", token.Error())
	}
	mqttPublisher.Disconnect(250)

	t.Logf("Published message to MQTT topic %s: %s", mqttTopic, testMessage1)

	// Wait for the message to appear on Kafka
	readCtx1, readCancel1 := context.WithTimeout(ctx, 15*time.Second)
	msg1, err := kafkaReader.ReadMessage(readCtx1)
	readCancel1()
	if err != nil {
		t.Fatalf("Failed to read first message from Kafka: %v", err)
	}

	if string(msg1.Value) != testMessage1 {
		t.Errorf("First message mismatch: got %q, want %q", string(msg1.Value), testMessage1)
	} else {
		t.Logf("Successfully received first bridged message on Kafka: %s", string(msg1.Value))
	}

	// Phase 2: Restart MQTT broker
	t.Log("Phase 2: Restarting MQTT broker (mosquitto)...")
	restartCmd := exec.CommandContext(ctx, "docker", "compose", "-f", "docker-compose.integration.yml", "restart", "mosquitto")
	restartCmd.Dir = getProjectRoot()
	if output, err := restartCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to restart mosquitto: %v\nOutput: %s", err, output)
	}

	// Wait for MQTT broker to come back up
	t.Log("Waiting for MQTT broker to come back up...")
	if err := waitForMQTT(ctx, 30*time.Second); err != nil {
		t.Fatalf("MQTT broker did not come back up: %v", err)
	}

	// Give the bridge time to reconnect
	t.Log("Waiting for bridge to reconnect...")
	time.Sleep(5 * time.Second)

	// Phase 3: Verify message flow works after reconnection
	t.Log("Phase 3: Verifying message flow after reconnection...")
	testMessage2 := fmt.Sprintf("after-restart-message-%d", testID)

	mqttPublisher2 := setupMQTTPublisher(t)
	token = mqttPublisher2.Publish(mqttTopic, 1, false, []byte(testMessage2))
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to publish message to MQTT after restart: %v", token.Error())
	}
	mqttPublisher2.Disconnect(250)

	t.Logf("Published message to MQTT topic %s: %s", mqttTopic, testMessage2)

	// Wait for the message to appear on Kafka
	readCtx2, readCancel2 := context.WithTimeout(ctx, 15*time.Second)
	msg2, err := kafkaReader.ReadMessage(readCtx2)
	readCancel2()
	if err != nil {
		t.Fatalf("Failed to read second message from Kafka after MQTT restart: %v", err)
	}

	if string(msg2.Value) != testMessage2 {
		t.Errorf("Second message mismatch: got %q, want %q", string(msg2.Value), testMessage2)
	} else {
		t.Logf("Successfully received second bridged message on Kafka after recovery: %s", string(msg2.Value))
	}

	t.Log("Successfully tested MQTT connection recovery!")
}

// TestKafkaConnectionRecovery tests that the bridge recovers after Kafka broker restart
// This test:
// 1. Starts the bridge
// 2. Verifies message flow works (Kafka→MQTT)
// 3. Restarts the Kafka broker container
// 4. Verifies the bridge reconnects and resumes operation
func TestKafkaConnectionRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Use unique topics for this test
	testID := time.Now().UnixNano()
	kafkaTopic := fmt.Sprintf("test-kafka-recovery-%d", testID)
	mqttTopic := fmt.Sprintf("mqtt/kafka/recovery/%d", testID)

	// Create a temporary config file for the bridge (Kafka→MQTT)
	configContent := fmt.Sprintf(`
kafka:
  broker: "%s"
  source_topic: "%s"
  group_id: "test-kafka-recovery-group-%d"

mqtt:
  broker: "%s"
  port: %d
  dest_topic: "%s"
  client_id: "test-kafka-recovery-%d"

bridge:
  name: "test-kafka-recovery"
  log_level: "debug"
  buffer_size: 100
`, kafkaBrokers, kafkaTopic, testID, mqttBroker, mqttPort, mqttTopic, testID)

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
	receivedMessages := make(chan string, 10)
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

	// Phase 1: Verify initial message flow works
	t.Log("Phase 1: Verifying initial message flow...")
	testMessage1 := fmt.Sprintf("before-kafka-restart-message-%d", testID)

	err := writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte(testMessage1),
	}, 10)
	if err != nil {
		t.Fatalf("Failed to write first message to Kafka: %v", err)
	}

	t.Logf("Published message to Kafka topic %s: %s", kafkaTopic, testMessage1)

	// Wait for the message to appear on MQTT
	select {
	case received := <-receivedMessages:
		if received != testMessage1 {
			t.Errorf("First message mismatch: got %q, want %q", received, testMessage1)
		} else {
			t.Logf("Successfully received first bridged message on MQTT: %s", received)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Timeout waiting for first bridged message on MQTT")
	}

	// Phase 2: Restart Kafka broker
	t.Log("Phase 2: Restarting Kafka broker...")
	restartCmd := exec.CommandContext(ctx, "docker", "compose", "-f", "docker-compose.integration.yml", "restart", "kafka")
	restartCmd.Dir = getProjectRoot()
	if output, err := restartCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to restart kafka: %v\nOutput: %s", err, output)
	}

	// Wait for Kafka broker to come back up
	t.Log("Waiting for Kafka broker to come back up...")
	if err := waitForKafka(ctx, 60*time.Second); err != nil {
		t.Fatalf("Kafka broker did not come back up: %v", err)
	}

	// Give the bridge time to reconnect
	t.Log("Waiting for bridge to reconnect to Kafka...")
	time.Sleep(10 * time.Second)

	// Phase 3: Verify message flow works after reconnection
	t.Log("Phase 3: Verifying message flow after Kafka reconnection...")

	// Create a new Kafka writer after restart
	kafkaWriter2 := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter2.Close()

	testMessage2 := fmt.Sprintf("after-kafka-restart-message-%d", testID)

	err = writeMessageWithRetry(ctx, kafkaWriter2, kafka.Message{
		Key:   []byte("test-key-2"),
		Value: []byte(testMessage2),
	}, 15)
	if err != nil {
		t.Fatalf("Failed to write second message to Kafka after restart: %v", err)
	}

	t.Logf("Published message to Kafka topic %s: %s", kafkaTopic, testMessage2)

	// Wait for the message to appear on MQTT
	select {
	case received := <-receivedMessages:
		if received != testMessage2 {
			t.Errorf("Second message mismatch: got %q, want %q", received, testMessage2)
		} else {
			t.Logf("Successfully received second bridged message on MQTT after recovery: %s", received)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for second bridged message on MQTT after Kafka restart")
	}

	t.Log("Successfully tested Kafka connection recovery!")
}

// waitForMQTT waits for the MQTT broker to become available
func waitForMQTT(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		opts := mqtt.NewClientOptions()
		opts.AddBroker(fmt.Sprintf("tcp://%s:%d", mqttBroker, mqttPort))
		opts.SetClientID("health-check")
		opts.SetConnectTimeout(2 * time.Second)

		client := mqtt.NewClient(opts)
		token := client.Connect()
		if token.Wait() && token.Error() == nil {
			client.Disconnect(250)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	return fmt.Errorf("timeout waiting for MQTT broker")
}

// waitForKafka waits for the Kafka broker to become available
func waitForKafka(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		dialCtx, dialCancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err := kafka.DialContext(dialCtx, "tcp", kafkaBrokers)
		dialCancel()

		if err == nil {
			_ = conn.Close()
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	return fmt.Errorf("timeout waiting for Kafka broker")
}
