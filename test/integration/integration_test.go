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
	"testing"
	"time"

	"github.com/csett86/kafka-mqtt-bridge/internal/bridge"
	"github.com/csett86/kafka-mqtt-bridge/pkg/config"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
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

// TestKafkaToMQTTBridge tests the actual bridge component:
// 1. Starts the bridge
// 2. Publishes a message to Kafka
// 3. Verifies the bridge forwards it to MQTT
func TestKafkaToMQTTBridge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Use unique topics for this test
	kafkaTopic := fmt.Sprintf("test-bridge-kafka-%d", time.Now().UnixNano())
	mqttTopic := fmt.Sprintf("mqtt/bridge/test/%d", time.Now().UnixNano())
	testMessage := fmt.Sprintf("bridge-test-message-%d", time.Now().UnixNano())

	// Create a test configuration for the bridge
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:     []string{kafkaBrokers},
			SourceTopic: kafkaTopic,
			GroupID:     fmt.Sprintf("test-bridge-group-%d", time.Now().UnixNano()),
		},
		MQTT: config.MQTTConfig{
			Broker:    mqttBroker,
			Port:      mqttPort,
			DestTopic: mqttTopic,
			ClientID:  fmt.Sprintf("test-bridge-%d", time.Now().UnixNano()),
		},
		Bridge: config.BridgeConfig{
			Name:       "test-bridge",
			LogLevel:   "debug",
			BufferSize: 100,
		},
	}

	// Setup MQTT subscriber to receive messages from the bridge
	receivedMessages := make(chan string, 10)
	mqttClient := setupMQTTSubscriber(t, mqttTopic, receivedMessages)
	defer mqttClient.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

	// Create a logger for the bridge
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	// Create and start the bridge
	b, err := bridge.New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create bridge: %v", err)
	}

	// Start bridge in background
	bridgeCtx, bridgeCancel := context.WithCancel(ctx)
	bridgeDone := make(chan struct{})
	go func() {
		defer close(bridgeDone)
		if err := b.Start(bridgeCtx); err != nil {
			t.Logf("Bridge stopped with error: %v", err)
		}
	}()

	// Give bridge time to start
	time.Sleep(1 * time.Second)

	// Setup Kafka writer to publish message
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()

	// Publish message to Kafka - the bridge should forward it to MQTT
	err = writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte(testMessage),
	}, 10)
	if err != nil {
		bridgeCancel()
		<-bridgeDone
		b.Stop()
		t.Fatalf("Failed to write message to Kafka: %v", err)
	}

	t.Logf("Published message to Kafka topic %s: %s", kafkaTopic, testMessage)

	// Wait for the message to appear on MQTT (forwarded by the bridge)
	select {
	case received := <-receivedMessages:
		if received != testMessage {
			t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
		} else {
			t.Logf("Successfully received bridged message on MQTT: %s", received)
		}
	case <-time.After(10 * time.Second):
		t.Error("Timeout waiting for bridged message on MQTT")
	}

	// Cleanup: stop the bridge
	bridgeCancel()
	<-bridgeDone
	b.Stop()

	t.Log("Successfully tested Kafka to MQTT bridge flow")
}

// TestBridgeMultipleMessages tests the bridge with multiple messages
func TestBridgeMultipleMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Use unique topics for this test
	kafkaTopic := fmt.Sprintf("test-bridge-multi-%d", time.Now().UnixNano())
	mqttTopic := fmt.Sprintf("mqtt/bridge/multi/%d", time.Now().UnixNano())

	testMessages := []string{
		"bridge-message-1",
		"bridge-message-2",
		"bridge-message-3",
	}

	// Create a test configuration for the bridge
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:     []string{kafkaBrokers},
			SourceTopic: kafkaTopic,
			GroupID:     fmt.Sprintf("test-bridge-multi-group-%d", time.Now().UnixNano()),
		},
		MQTT: config.MQTTConfig{
			Broker:    mqttBroker,
			Port:      mqttPort,
			DestTopic: mqttTopic,
			ClientID:  fmt.Sprintf("test-bridge-multi-%d", time.Now().UnixNano()),
		},
		Bridge: config.BridgeConfig{
			Name:       "test-bridge-multi",
			LogLevel:   "debug",
			BufferSize: 100,
		},
	}

	// Setup MQTT subscriber to receive messages from the bridge
	receivedMessages := make(chan string, len(testMessages)+5)
	mqttClient := setupMQTTSubscriber(t, mqttTopic, receivedMessages)
	defer mqttClient.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

	// Create a logger for the bridge
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	// Create and start the bridge
	b, err := bridge.New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create bridge: %v", err)
	}

	// Start bridge in background
	bridgeCtx, bridgeCancel := context.WithCancel(ctx)
	bridgeDone := make(chan struct{})
	go func() {
		defer close(bridgeDone)
		if err := b.Start(bridgeCtx); err != nil {
			t.Logf("Bridge stopped with error: %v", err)
		}
	}()

	// Give bridge time to start
	time.Sleep(1 * time.Second)

	// Setup Kafka writer to publish messages
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()

	// Publish messages to Kafka - the bridge should forward them to MQTT
	for i, msg := range testMessages {
		retries := 10
		if i > 0 {
			retries = 3
		}
		err = writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(msg),
		}, retries)
		if err != nil {
			bridgeCancel()
			<-bridgeDone
			b.Stop()
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

	// Cleanup: stop the bridge
	bridgeCancel()
	<-bridgeDone
	b.Stop()

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

// TestMQTTToKafkaBridge tests the bridge component for MQTT→Kafka direction:
// 1. Starts the bridge
// 2. Publishes a message to MQTT
// 3. Verifies the bridge forwards it to Kafka
func TestMQTTToKafkaBridge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Use unique topics for this test
	mqttTopic := fmt.Sprintf("mqtt/to-kafka/test/%d", time.Now().UnixNano())
	kafkaTopic := fmt.Sprintf("test-mqtt-to-kafka-%d", time.Now().UnixNano())
	testMessage := fmt.Sprintf("mqtt-to-kafka-test-message-%d", time.Now().UnixNano())

	// Create a test configuration for the bridge (MQTT→Kafka only)
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:   []string{kafkaBrokers},
			DestTopic: kafkaTopic,
			GroupID:   fmt.Sprintf("test-mqtt-to-kafka-group-%d", time.Now().UnixNano()),
		},
		MQTT: config.MQTTConfig{
			Broker:      mqttBroker,
			Port:        mqttPort,
			SourceTopic: mqttTopic,
			ClientID:    fmt.Sprintf("test-mqtt-to-kafka-%d", time.Now().UnixNano()),
		},
		Bridge: config.BridgeConfig{
			Name:       "test-mqtt-to-kafka",
			LogLevel:   "debug",
			BufferSize: 100,
		},
	}

	// Create a logger for the bridge
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync() //nolint:errcheck

	// Create and start the bridge
	b, err := bridge.New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create bridge: %v", err)
	}

	// Start bridge in background
	bridgeCtx, bridgeCancel := context.WithCancel(ctx)
	bridgeDone := make(chan struct{})
	go func() {
		defer close(bridgeDone)
		if err := b.Start(bridgeCtx); err != nil {
			t.Logf("Bridge stopped with error: %v", err)
		}
	}()

	// Give bridge time to start and subscribe
	time.Sleep(1 * time.Second)

	// Pre-create the Kafka topic by writing a dummy message (triggers topic creation)
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()
	err = writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
		Key:   []byte("init"),
		Value: []byte("init"),
	}, 10)
	if err != nil {
		bridgeCancel()
		<-bridgeDone
		b.Stop()
		t.Fatalf("Failed to pre-create Kafka topic: %v", err)
	}

	// Setup Kafka reader to consume messages forwarded by the bridge
	kafkaReader := setupKafkaReader(t, kafkaTopic)
	defer kafkaReader.Close()

	// Consume the init message
	initCtx, initCancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = kafkaReader.ReadMessage(initCtx)
	initCancel()
	if err != nil {
		bridgeCancel()
		<-bridgeDone
		b.Stop()
		t.Fatalf("Failed to consume init message: %v", err)
	}

	// Setup MQTT publisher to publish message
	mqttPublisher := setupMQTTPublisher(t)
	defer mqttPublisher.Disconnect(250)

	// Publish message to MQTT - the bridge should forward it to Kafka
	token := mqttPublisher.Publish(mqttTopic, 1, false, []byte(testMessage))
	if token.Wait() && token.Error() != nil {
		bridgeCancel()
		<-bridgeDone
		b.Stop()
		t.Fatalf("Failed to publish message to MQTT: %v", token.Error())
	}

	t.Logf("Published message to MQTT topic %s: %s", mqttTopic, testMessage)

	// Wait for the message to appear on Kafka (forwarded by the bridge)
	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel()

	msg, err := kafkaReader.ReadMessage(readCtx)
	if err != nil {
		bridgeCancel()
		<-bridgeDone
		b.Stop()
		t.Fatalf("Failed to read message from Kafka: %v", err)
	}

	received := string(msg.Value)
	if received != testMessage {
		t.Errorf("Message mismatch: got %q, want %q", received, testMessage)
	} else {
		t.Logf("Successfully received bridged message on Kafka: %s", received)
	}

	// Cleanup: stop the bridge
	bridgeCancel()
	<-bridgeDone
	b.Stop()

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
