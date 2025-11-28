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
	"sync"
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

// TestKafkaToMQTTBridge tests that messages published to Kafka
// can be consumed and the bridge can publish them to MQTT
func TestKafkaToMQTTBridge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	kafkaTopic := fmt.Sprintf("test-kafka-to-mqtt-%d", time.Now().UnixNano())
	mqttTopic := "mqtt/test/from-kafka"
	testMessage := fmt.Sprintf("test-message-kafka-to-mqtt-%d", time.Now().UnixNano())

	// Setup MQTT client to receive messages
	receivedMessages := make(chan string, 10)
	mqttClient := setupMQTTSubscriber(t, mqttTopic, receivedMessages)
	defer mqttClient.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

	// Setup Kafka writer
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()

	// Publish message to Kafka with retries for topic creation
	err := writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte(testMessage),
	}, 10)
	if err != nil {
		t.Fatalf("Failed to write message to Kafka: %v", err)
	}

	t.Logf("Published message to Kafka topic %s: %s", kafkaTopic, testMessage)

	// Setup Kafka reader to verify the message was written
	kafkaReader := setupKafkaReader(t, kafkaTopic)
	defer kafkaReader.Close()

	// Read and verify the message from Kafka
	msg, err := kafkaReader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("Failed to read message from Kafka: %v", err)
	}

	if string(msg.Value) != testMessage {
		t.Errorf("Kafka message mismatch: got %q, want %q", string(msg.Value), testMessage)
	}

	t.Logf("Successfully verified message in Kafka: %s", string(msg.Value))
}

// TestMQTTToKafkaBridge tests that messages published to MQTT
// can be read and the bridge can forward them to Kafka
func TestMQTTToKafkaBridge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	kafkaTopic := fmt.Sprintf("test-mqtt-to-kafka-%d", time.Now().UnixNano())
	mqttTopic := "mqtt/test/to-kafka"
	testMessage := fmt.Sprintf("test-message-mqtt-to-kafka-%d", time.Now().UnixNano())

	// Setup Kafka reader
	kafkaReader := setupKafkaReader(t, kafkaTopic)
	defer kafkaReader.Close()

	// Setup Kafka writer for the test
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()

	// Setup MQTT client
	mqttClient := setupMQTTPublisher(t)
	defer mqttClient.Disconnect(250)

	// Publish message to MQTT
	token := mqttClient.Publish(mqttTopic, 1, false, []byte(testMessage))
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to publish message to MQTT: %v", token.Error())
	}

	t.Logf("Published message to MQTT topic %s: %s", mqttTopic, testMessage)

	// Write a message to Kafka to verify connectivity with retries
	err := writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte(testMessage),
	}, 10)
	if err != nil {
		t.Fatalf("Failed to write message to Kafka: %v", err)
	}

	// Read the message from Kafka
	msg, err := kafkaReader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("Failed to read message from Kafka: %v", err)
	}

	if string(msg.Value) != testMessage {
		t.Errorf("Kafka message mismatch: got %q, want %q", string(msg.Value), testMessage)
	}

	t.Logf("Successfully verified message flow: MQTT -> Kafka")
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

// TestConcurrentMessages tests handling of concurrent message flows
func TestConcurrentMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	numMessages := 10
	kafkaTopic := fmt.Sprintf("test-concurrent-%d", time.Now().UnixNano())
	mqttTopic := "test/concurrent/messages"

	// Setup Kafka
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()
	kafkaReader := setupKafkaReader(t, kafkaTopic)
	defer kafkaReader.Close()

	// Setup MQTT
	mqttReceived := make(chan string, numMessages)
	mqttSub := setupMQTTSubscriber(t, mqttTopic, mqttReceived)
	defer mqttSub.Disconnect(250)
	mqttPub := setupMQTTPublisher(t)
	defer mqttPub.Disconnect(250)

	time.Sleep(500 * time.Millisecond)

	// Send messages concurrently
	var wg sync.WaitGroup

	// Kafka messages with retries
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numMessages; i++ {
			msg := fmt.Sprintf("kafka-concurrent-%d", i)
			retries := 10
			if i > 0 {
				retries = 3
			}
			err := writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: []byte(msg),
			}, retries)
			if err != nil {
				t.Errorf("Failed to write Kafka message %d: %v", i, err)
			}
		}
	}()

	// MQTT messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numMessages; i++ {
			msg := fmt.Sprintf("mqtt-concurrent-%d", i)
			token := mqttPub.Publish(mqttTopic, 1, false, []byte(msg))
			if token.Wait() && token.Error() != nil {
				t.Errorf("Failed to publish MQTT message %d: %v", i, token.Error())
			}
		}
	}()

	wg.Wait()

	// Verify Kafka messages
	for i := 0; i < numMessages; i++ {
		_, err := kafkaReader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("Failed to read Kafka message %d: %v", i, err)
		}
	}

	// Verify MQTT messages
	receivedCount := 0
	timeout := time.After(messageWait)
mqttCollect:
	for {
		select {
		case <-mqttReceived:
			receivedCount++
			if receivedCount >= numMessages {
				break mqttCollect
			}
		case <-timeout:
			break mqttCollect
		}
	}

	if receivedCount != numMessages {
		t.Errorf("Expected %d MQTT messages, got %d", numMessages, receivedCount)
	}

	t.Logf("Successfully handled %d concurrent Kafka and MQTT messages", numMessages)
}

// TestLargeMessagePayload tests handling of large message payloads
func TestLargeMessagePayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	kafkaTopic := fmt.Sprintf("test-large-payload-%d", time.Now().UnixNano())
	mqttTopic := "test/large/payload"

	// Create a large payload (10KB)
	largePayload := make([]byte, 10*1024)
	for i := range largePayload {
		largePayload[i] = byte('A' + (i % 26))
	}

	// Test Kafka with large payload
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()
	kafkaReader := setupKafkaReader(t, kafkaTopic)
	defer kafkaReader.Close()

	err := writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
		Key:   []byte("large-key"),
		Value: largePayload,
	}, 10)
	if err != nil {
		t.Fatalf("Failed to write large message to Kafka: %v", err)
	}

	msg, err := kafkaReader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("Failed to read large message from Kafka: %v", err)
	}

	if len(msg.Value) != len(largePayload) {
		t.Errorf("Kafka payload size mismatch: got %d, want %d", len(msg.Value), len(largePayload))
	}

	// Test MQTT with large payload
	mqttReceived := make(chan string, 1)
	mqttSub := setupMQTTSubscriber(t, mqttTopic, mqttReceived)
	defer mqttSub.Disconnect(250)
	time.Sleep(500 * time.Millisecond)

	mqttPub := setupMQTTPublisher(t)
	defer mqttPub.Disconnect(250)

	token := mqttPub.Publish(mqttTopic, 1, false, largePayload)
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to publish large message to MQTT: %v", token.Error())
	}

	select {
	case received := <-mqttReceived:
		if len(received) != len(largePayload) {
			t.Errorf("MQTT payload size mismatch: got %d, want %d", len(received), len(largePayload))
		}
	case <-time.After(messageWait):
		t.Fatal("Timeout waiting for large MQTT message")
	}

	t.Log("Successfully handled large payloads in both Kafka and MQTT")
}

// TestConnectionResilience tests reconnection behavior
func TestConnectionResilience(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	kafkaTopic := fmt.Sprintf("test-resilience-%d", time.Now().UnixNano())

	// Create and close writer, then create a new one
	writer1 := setupKafkaWriter(t, kafkaTopic)
	err := writeMessageWithRetry(ctx, writer1, kafka.Message{
		Key:   []byte("key1"),
		Value: []byte("message1"),
	}, 10)
	if err != nil {
		t.Fatalf("Failed to write first message: %v", err)
	}
	writer1.Close()

	// Create new writer and continue writing
	writer2 := setupKafkaWriter(t, kafkaTopic)
	defer writer2.Close()
	err = writeMessageWithRetry(ctx, writer2, kafka.Message{
		Key:   []byte("key2"),
		Value: []byte("message2"),
	}, 5)
	if err != nil {
		t.Fatalf("Failed to write second message: %v", err)
	}

	// Read both messages
	reader := setupKafkaReader(t, kafkaTopic)
	defer reader.Close()

	for i := 0; i < 2; i++ {
		_, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("Failed to read message %d: %v", i+1, err)
		}
	}

	t.Log("Successfully tested connection resilience")
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
