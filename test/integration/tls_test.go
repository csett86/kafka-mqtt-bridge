// Package integration provides integration tests for the kafka-mqtt-bridge.
// This file contains tests for TLS and authentication support.
package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

// TLS test configuration
var (
	mqttTLSPort = getEnvInt("TEST_MQTT_TLS_PORT", 8883)
)

// TestMQTTTLSConnection tests basic MQTT TLS connection with mutual TLS
func TestMQTTTLSConnection(t *testing.T) {
	// Get project root for certificate paths
	projectRoot := getProjectRoot()
	certsDir := filepath.Join(projectRoot, "test", "certs")

	// Check if certificates exist
	caFile := filepath.Join(certsDir, "ca.crt")
	certFile := filepath.Join(certsDir, "client.crt")
	keyFile := filepath.Join(certsDir, "client.key")

	for _, f := range []string{caFile, certFile, keyFile} {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			t.Skipf("Certificate file not found: %s", f)
		}
	}

	// Create TLS config
	tlsConfig, err := createClientTLSConfig(caFile, certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to create TLS config: %v", err)
	}

	// Connect to MQTT with TLS
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("ssl://%s:%d", mqttBroker, mqttTLSPort))
	opts.SetClientID(fmt.Sprintf("tls-test-client-%d", time.Now().UnixNano()))
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetTLSConfig(tlsConfig)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		t.Skipf("Skipping TLS test: MQTT TLS broker not available: %v", token.Error())
	}
	defer client.Disconnect(250)

	t.Log("Successfully connected to MQTT broker with TLS")

	// Test publish/subscribe over TLS
	topic := fmt.Sprintf("test/tls/connection/%d", time.Now().UnixNano())
	testMessage := "TLS test message"
	receivedMessages := make(chan string, 1)

	// Subscribe
	token = client.Subscribe(topic, 1, func(c mqtt.Client, m mqtt.Message) {
		select {
		case receivedMessages <- string(m.Payload()):
		default:
		}
	})
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to subscribe over TLS: %v", token.Error())
	}

	// Give subscription time to be ready
	time.Sleep(500 * time.Millisecond)

	// Publish
	token = client.Publish(topic, 1, false, []byte(testMessage))
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to publish over TLS: %v", token.Error())
	}

	// Wait for message
	select {
	case received := <-receivedMessages:
		if received != testMessage {
			t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
		} else {
			t.Logf("Successfully received message over TLS: %s", received)
		}
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for message over TLS")
	}
}

// TestKafkaToMQTTBridgeWithTLS tests the bridge component with TLS-enabled MQTT connection
// using the built binary. This test:
// 1. Builds and starts the bridge binary with TLS configuration
// 2. Publishes a message to Kafka
// 3. Verifies the bridge forwards it to MQTT over TLS
func TestKafkaToMQTTBridgeWithTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Get project root for certificate paths
	projectRoot := getProjectRoot()
	certsDir := filepath.Join(projectRoot, "test", "certs")

	// Check if certificates exist
	caFile := filepath.Join(certsDir, "ca.crt")
	certFile := filepath.Join(certsDir, "client.crt")
	keyFile := filepath.Join(certsDir, "client.key")

	for _, f := range []string{caFile, certFile, keyFile} {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			t.Skipf("Certificate file not found: %s", f)
		}
	}

	// Check if TLS MQTT broker is available
	tlsConfig, err := createClientTLSConfig(caFile, certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to create TLS config: %v", err)
	}

	if err := checkMQTTTLSConnection(tlsConfig); err != nil {
		t.Skipf("Skipping TLS bridge test: MQTT TLS broker not available: %v", err)
	}

	// Use unique topics for this test
	testID := time.Now().UnixNano()
	kafkaTopic := fmt.Sprintf("test-tls-bridge-kafka-%d", testID)
	mqttTopic := fmt.Sprintf("mqtt/tls/bridge/test/%d", testID)
	testMessage := fmt.Sprintf("tls-bridge-test-message-%d", testID)

	// Create a temporary config file for the bridge with TLS enabled
	configContent := fmt.Sprintf(`
kafka:
  broker: "%s"
  group_id: "test-tls-bridge-group-%d"

mqtt:
  broker: "%s"
  port: %d
  client_id: "test-tls-bridge-%d"
  tls:
    enabled: true
    ca_file: "%s"
    cert_file: "%s"
    key_file: "%s"

bridge:
  name: "test-tls-bridge"
  log_level: "debug"
  buffer_size: 100
  kafka_to_mqtt:
    source_topic: "%s"
    dest_topic: "%s"
`, kafkaBrokers, testID, mqttBroker, mqttTLSPort, testID, caFile, certFile, keyFile, kafkaTopic, mqttTopic)

	// Create temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Logf("Created TLS config file at: %s", configPath)

	// Build the bridge binary
	binaryPath := filepath.Join(tmpDir, "kafka-mqtt-bridge")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/bridge")
	buildCmd.Dir = projectRoot
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build bridge binary: %v\nOutput: %s", err, output)
	}

	t.Logf("Built bridge binary at: %s", binaryPath)

	// Setup MQTT subscriber to receive messages from the bridge (with TLS)
	receivedMessages := make(chan string, 10)
	mqttClient := setupMQTTSubscriberTLS(t, mqttTopic, receivedMessages, tlsConfig)
	defer mqttClient.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

	// Setup Kafka writer and publish message BEFORE starting bridge
	// This ensures the message exists when the bridge starts reading
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()

	// Publish message to Kafka - the bridge should forward it to MQTT when it starts
	err = writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
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

	t.Log("TLS-enabled bridge binary started, waiting for it to initialize...")

	// Wait for the message to appear on MQTT (forwarded by the bridge over TLS)
	select {
	case received := <-receivedMessages:
		if received != testMessage {
			t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
		} else {
			t.Logf("Successfully received TLS-bridged message on MQTT: %s", received)
		}
	case <-time.After(15 * time.Second):
		t.Error("Timeout waiting for TLS-bridged message on MQTT")
	}

	t.Log("Successfully tested Kafka to MQTT TLS bridge flow")
}

// TestMQTTToKafkaBridgeWithTLS tests the MQTT→Kafka direction with TLS
func TestMQTTToKafkaBridgeWithTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Get project root for certificate paths
	projectRoot := getProjectRoot()
	certsDir := filepath.Join(projectRoot, "test", "certs")

	// Check if certificates exist
	caFile := filepath.Join(certsDir, "ca.crt")
	certFile := filepath.Join(certsDir, "client.crt")
	keyFile := filepath.Join(certsDir, "client.key")

	for _, f := range []string{caFile, certFile, keyFile} {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			t.Skipf("Certificate file not found: %s", f)
		}
	}

	// Check if TLS MQTT broker is available
	tlsConfig, err := createClientTLSConfig(caFile, certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to create TLS config: %v", err)
	}

	if err := checkMQTTTLSConnection(tlsConfig); err != nil {
		t.Skipf("Skipping TLS bridge test: MQTT TLS broker not available: %v", err)
	}

	// Use unique topics for this test
	testID := time.Now().UnixNano()
	mqttTopic := fmt.Sprintf("mqtt/tls/to-kafka/test/%d", testID)
	kafkaTopic := fmt.Sprintf("test-tls-mqtt-to-kafka-%d", testID)
	testMessage := fmt.Sprintf("tls-mqtt-to-kafka-test-message-%d", testID)

	// Create a temporary config file for the bridge (MQTT→Kafka with TLS)
	configContent := fmt.Sprintf(`
kafka:
  broker: "%s"
  group_id: "test-tls-mqtt-to-kafka-group-%d"

mqtt:
  broker: "%s"
  port: %d
  client_id: "test-tls-mqtt-to-kafka-%d"
  tls:
    enabled: true
    ca_file: "%s"
    cert_file: "%s"
    key_file: "%s"

bridge:
  name: "test-tls-mqtt-to-kafka"
  log_level: "debug"
  buffer_size: 100
  mqtt_to_kafka:
    source_topic: "%s"
    dest_topic: "%s"
`, kafkaBrokers, testID, mqttBroker, mqttTLSPort, testID, caFile, certFile, keyFile, mqttTopic, kafkaTopic)

	// Create temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Logf("Created TLS config file at: %s", configPath)

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

	t.Log("TLS-enabled bridge binary started, waiting for it to initialize...")

	// Give bridge time to start and subscribe
	time.Sleep(3 * time.Second)

	// Setup Kafka reader to consume messages forwarded by the bridge
	kafkaReader := setupKafkaReader(t, kafkaTopic)
	defer kafkaReader.Close()

	// Setup MQTT publisher with TLS to publish message
	mqttPublisher := setupMQTTPublisherTLS(t, tlsConfig)
	defer mqttPublisher.Disconnect(250)

	// Publish message to MQTT over TLS - the bridge should forward it to Kafka
	token := mqttPublisher.Publish(mqttTopic, 1, false, []byte(testMessage))
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to publish message to MQTT over TLS: %v", token.Error())
	}

	t.Logf("Published message to MQTT topic %s over TLS: %s", mqttTopic, testMessage)

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
		t.Logf("Successfully received TLS-bridged message on Kafka: %s", received)
	}

	t.Log("Successfully tested MQTT TLS to Kafka bridge flow")
}

// Helper functions for TLS tests

func createClientTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	// Load CA certificate
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	// Load client certificate and key
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}

	return &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
	}, nil
}

func checkMQTTTLSConnection(tlsConfig *tls.Config) error {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("ssl://%s:%d", mqttBroker, mqttTLSPort))
	opts.SetClientID("tls-connection-check")
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetTLSConfig(tlsConfig)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT with TLS: %w", token.Error())
	}
	client.Disconnect(250)
	return nil
}

func setupMQTTPublisherTLS(t *testing.T, tlsConfig *tls.Config) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("ssl://%s:%d", mqttBroker, mqttTLSPort))
	opts.SetClientID(fmt.Sprintf("test-tls-publisher-%d", time.Now().UnixNano()))
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetTLSConfig(tlsConfig)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect TLS MQTT publisher: %v", token.Error())
	}
	return client
}

func setupMQTTSubscriberTLS(t *testing.T, topic string, messages chan<- string, tlsConfig *tls.Config) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("ssl://%s:%d", mqttBroker, mqttTLSPort))
	opts.SetClientID(fmt.Sprintf("test-tls-subscriber-%d", time.Now().UnixNano()))
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetTLSConfig(tlsConfig)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect TLS MQTT subscriber: %v", token.Error())
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
