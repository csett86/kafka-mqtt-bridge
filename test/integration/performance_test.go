// Package integration provides integration tests for the kafka-mqtt-bridge.
// This file contains performance tests that analyze RAM usage and detect memory leaks.
package integration

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/csett86/kafka-mqtt-bridge/internal/bridge"
	"github.com/csett86/kafka-mqtt-bridge/pkg/config"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// MemoryStats holds memory usage statistics at a point in time
type MemoryStats struct {
	Alloc      uint64 // bytes allocated and still in use
	TotalAlloc uint64 // bytes allocated (even if freed)
	Sys        uint64 // bytes obtained from system
	HeapAlloc  uint64 // bytes allocated on heap
	HeapSys    uint64 // bytes obtained from system for heap
	HeapInuse  uint64 // bytes in in-use spans
	NumGC      uint32 // number of GC cycles
}

// getMemoryStats returns current memory statistics after forcing GC
func getMemoryStats() MemoryStats {
	// Force garbage collection to get accurate memory stats
	runtime.GC()
	runtime.GC() // Run twice to ensure collection

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return MemoryStats{
		Alloc:      m.Alloc,
		TotalAlloc: m.TotalAlloc,
		Sys:        m.Sys,
		HeapAlloc:  m.HeapAlloc,
		HeapSys:    m.HeapSys,
		HeapInuse:  m.HeapInuse,
		NumGC:      m.NumGC,
	}
}

// formatBytes formats bytes to human-readable format
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// TestPerformance1000Messages tests the bridge with 1000 small messages
// and monitors memory usage to detect potential memory leaks.
func TestPerformance1000Messages(t *testing.T) {
	const (
		messageCount   = 1000
		messageSize    = 100 // bytes per message
		receiveTimeout = 120 * time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Use unique topics for this test
	kafkaTopic := fmt.Sprintf("perf-test-1000-%d", time.Now().UnixNano())
	mqttTopic := fmt.Sprintf("mqtt/perf/1000/%d", time.Now().UnixNano())

	// Create a test configuration for the bridge
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:     []string{kafkaBrokers},
			SourceTopic: kafkaTopic,
			GroupID:     fmt.Sprintf("perf-test-1000-group-%d", time.Now().UnixNano()),
		},
		MQTT: config.MQTTConfig{
			Broker:    mqttBroker,
			Port:      mqttPort,
			DestTopic: mqttTopic,
			ClientID:  fmt.Sprintf("perf-test-1000-%d", time.Now().UnixNano()),
		},
		Bridge: config.BridgeConfig{
			Name:       "perf-test-1000",
			LogLevel:   "warn", // Reduce log noise during performance tests
			BufferSize: 1000,
		},
	}

	// Get initial memory stats
	initialStats := getMemoryStats()
	t.Logf("Initial memory - Alloc: %s, HeapAlloc: %s, Sys: %s",
		formatBytes(initialStats.Alloc),
		formatBytes(initialStats.HeapAlloc),
		formatBytes(initialStats.Sys))

	// Setup MQTT subscriber to receive messages from the bridge
	receivedCount := 0
	receivedDone := make(chan struct{})
	mqttClient := setupMQTTSubscriberWithCounter(t, mqttTopic, &receivedCount, messageCount, receivedDone)
	defer mqttClient.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

	// Create a logger for the bridge (suppress debug logs for performance)
	logConfig := zap.NewProductionConfig()
	logConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	logger, err := logConfig.Build()
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

	// Give bridge time to start
	time.Sleep(1 * time.Second)

	// Get memory stats after bridge startup
	afterStartupStats := getMemoryStats()
	t.Logf("After bridge startup - Alloc: %s, HeapAlloc: %s",
		formatBytes(afterStartupStats.Alloc),
		formatBytes(afterStartupStats.HeapAlloc))

	// Setup Kafka writer
	kafkaWriter := setupKafkaWriter(t, kafkaTopic)
	defer kafkaWriter.Close()

	// Generate test message
	testMessage := strings.Repeat("x", messageSize)

	// Publish messages to Kafka
	t.Logf("Publishing %d messages of %d bytes each...", messageCount, messageSize)
	startTime := time.Now()

	for i := 0; i < messageCount; i++ {
		retries := 3
		if i == 0 {
			retries = 10 // More retries for first message (topic creation)
		}
		err = writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("%s-%d", testMessage, i)),
		}, retries)
		if err != nil {
			bridgeCancel()
			<-bridgeDone
			b.Stop()
			t.Fatalf("Failed to write message %d to Kafka: %v", i, err)
		}

		// Log progress every 100 messages
		if (i+1)%100 == 0 {
			t.Logf("Published %d/%d messages", i+1, messageCount)
		}
	}

	publishDuration := time.Since(startTime)
	t.Logf("Published all %d messages in %v (%.2f msg/sec)",
		messageCount, publishDuration, float64(messageCount)/publishDuration.Seconds())

	// Get memory stats after publishing
	afterPublishStats := getMemoryStats()
	t.Logf("After publishing - Alloc: %s, HeapAlloc: %s",
		formatBytes(afterPublishStats.Alloc),
		formatBytes(afterPublishStats.HeapAlloc))

	// Wait for all messages to be received
	select {
	case <-receivedDone:
		t.Logf("Received all %d messages", messageCount)
	case <-time.After(receiveTimeout):
		t.Logf("Timeout waiting for messages. Received %d/%d", receivedCount, messageCount)
	}

	receiveDuration := time.Since(startTime)
	t.Logf("Total test duration: %v", receiveDuration)

	// Cleanup: stop the bridge
	bridgeCancel()
	<-bridgeDone
	b.Stop()

	// Wait for cleanup
	time.Sleep(500 * time.Millisecond)

	// Get final memory stats after cleanup
	finalStats := getMemoryStats()
	t.Logf("After cleanup - Alloc: %s, HeapAlloc: %s, NumGC: %d",
		formatBytes(finalStats.Alloc),
		formatBytes(finalStats.HeapAlloc),
		finalStats.NumGC)

	// Memory analysis
	memoryGrowth := int64(finalStats.HeapAlloc) - int64(afterStartupStats.HeapAlloc)
	t.Logf("Memory growth during test: %s", formatBytes(uint64(max(0, memoryGrowth))))

	// Check for potential memory leaks
	// After processing 1000 messages and cleanup, memory should not grow significantly
	// Allow for some growth due to GC timing, but flag significant issues
	expectedMaxGrowth := uint64(10 * 1024 * 1024) // 10MB threshold
	if finalStats.HeapAlloc > afterStartupStats.HeapAlloc+expectedMaxGrowth {
		t.Logf("WARNING: Significant memory growth detected: %s (threshold: %s)",
			formatBytes(uint64(memoryGrowth)), formatBytes(expectedMaxGrowth))
	} else {
		t.Logf("Memory usage within acceptable limits")
	}

	// Verify message count
	if receivedCount < messageCount {
		t.Errorf("Expected %d messages, received %d", messageCount, receivedCount)
	}

	t.Logf("Performance test (1000 messages) completed successfully")
}

// TestPerformance10LargeMessages tests the bridge with 10 x 1MB messages
// and monitors memory usage to detect potential memory leaks with large payloads.
func TestPerformance10LargeMessages(t *testing.T) {
	const (
		messageCount   = 10
		messageSize    = 1024 * 1024 // 1MB per message
		receiveTimeout = 120 * time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Use unique topics for this test
	kafkaTopic := fmt.Sprintf("perf-test-large-%d", time.Now().UnixNano())
	mqttTopic := fmt.Sprintf("mqtt/perf/large/%d", time.Now().UnixNano())

	// Create a test configuration for the bridge
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:     []string{kafkaBrokers},
			SourceTopic: kafkaTopic,
			GroupID:     fmt.Sprintf("perf-test-large-group-%d", time.Now().UnixNano()),
		},
		MQTT: config.MQTTConfig{
			Broker:    mqttBroker,
			Port:      mqttPort,
			DestTopic: mqttTopic,
			ClientID:  fmt.Sprintf("perf-test-large-%d", time.Now().UnixNano()),
		},
		Bridge: config.BridgeConfig{
			Name:       "perf-test-large",
			LogLevel:   "warn",
			BufferSize: 100,
		},
	}

	// Get initial memory stats
	initialStats := getMemoryStats()
	t.Logf("Initial memory - Alloc: %s, HeapAlloc: %s, Sys: %s",
		formatBytes(initialStats.Alloc),
		formatBytes(initialStats.HeapAlloc),
		formatBytes(initialStats.Sys))

	// Setup MQTT subscriber to receive messages from the bridge
	receivedCount := 0
	receivedSizes := make([]int, 0, messageCount)
	receivedDone := make(chan struct{})
	mqttClient := setupMQTTSubscriberWithSizeTracking(t, mqttTopic, &receivedCount, &receivedSizes, messageCount, receivedDone)
	defer mqttClient.Disconnect(250)

	// Give subscriber time to be ready
	time.Sleep(500 * time.Millisecond)

	// Create a logger for the bridge
	logConfig := zap.NewProductionConfig()
	logConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	logger, err := logConfig.Build()
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

	// Give bridge time to start
	time.Sleep(1 * time.Second)

	// Get memory stats after bridge startup
	afterStartupStats := getMemoryStats()
	t.Logf("After bridge startup - Alloc: %s, HeapAlloc: %s",
		formatBytes(afterStartupStats.Alloc),
		formatBytes(afterStartupStats.HeapAlloc))

	// Setup Kafka writer with larger batch size for large messages
	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBrokers),
		Topic:                  kafkaTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		BatchTimeout:           100 * time.Millisecond,
		WriteTimeout:           30 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		MaxAttempts:            10,
		BatchBytes:             2 * 1024 * 1024, // 2MB to accommodate 1MB messages
	}
	defer kafkaWriter.Close()

	// Generate large test message (1MB of data)
	largePayload := strings.Repeat("A", messageSize)

	// Track memory during message processing
	memorySnapshots := make([]MemoryStats, 0, messageCount+2)
	memorySnapshots = append(memorySnapshots, afterStartupStats)

	// Publish large messages to Kafka
	t.Logf("Publishing %d messages of %s each...", messageCount, formatBytes(uint64(messageSize)))
	startTime := time.Now()

	for i := 0; i < messageCount; i++ {
		// Create unique message with identifier
		msgPayload := fmt.Sprintf("%s-MSG%d", largePayload[:messageSize-10], i)

		retries := 5
		if i == 0 {
			retries = 15 // More retries for first message (topic creation)
		}
		err = writeMessageWithRetry(ctx, kafkaWriter, kafka.Message{
			Key:   []byte(fmt.Sprintf("large-key-%d", i)),
			Value: []byte(msgPayload),
		}, retries)
		if err != nil {
			bridgeCancel()
			<-bridgeDone
			b.Stop()
			t.Fatalf("Failed to write large message %d to Kafka: %v", i, err)
		}

		t.Logf("Published large message %d/%d", i+1, messageCount)

		// Take memory snapshot after each message
		snapshot := getMemoryStats()
		memorySnapshots = append(memorySnapshots, snapshot)
	}

	publishDuration := time.Since(startTime)
	t.Logf("Published all %d large messages in %v", messageCount, publishDuration)

	// Get memory stats after publishing
	afterPublishStats := getMemoryStats()
	t.Logf("After publishing - Alloc: %s, HeapAlloc: %s",
		formatBytes(afterPublishStats.Alloc),
		formatBytes(afterPublishStats.HeapAlloc))

	// Wait for all messages to be received
	select {
	case <-receivedDone:
		t.Logf("Received all %d large messages", messageCount)
	case <-time.After(receiveTimeout):
		t.Logf("Timeout waiting for large messages. Received %d/%d", receivedCount, messageCount)
	}

	receiveDuration := time.Since(startTime)
	t.Logf("Total test duration: %v", receiveDuration)

	// Cleanup: stop the bridge
	bridgeCancel()
	<-bridgeDone
	b.Stop()

	// Wait for cleanup and GC
	time.Sleep(1 * time.Second)

	// Get final memory stats after cleanup
	finalStats := getMemoryStats()
	t.Logf("After cleanup - Alloc: %s, HeapAlloc: %s, NumGC: %d",
		formatBytes(finalStats.Alloc),
		formatBytes(finalStats.HeapAlloc),
		finalStats.NumGC)

	// Memory analysis
	t.Log("\n=== Memory Analysis ===")
	t.Logf("Startup HeapAlloc:  %s", formatBytes(afterStartupStats.HeapAlloc))
	t.Logf("Peak HeapAlloc:     %s", formatBytes(afterPublishStats.HeapAlloc))
	t.Logf("Final HeapAlloc:    %s", formatBytes(finalStats.HeapAlloc))

	memoryGrowth := int64(finalStats.HeapAlloc) - int64(afterStartupStats.HeapAlloc)
	t.Logf("Net memory growth:  %s", formatBytes(uint64(max(0, memoryGrowth))))

	// Analyze memory snapshots for leak patterns
	t.Log("\n=== Memory Snapshot Analysis ===")
	for i, snap := range memorySnapshots {
		if i == 0 {
			t.Logf("Snapshot %d (startup): HeapAlloc=%s", i, formatBytes(snap.HeapAlloc))
		} else {
			growth := int64(snap.HeapAlloc) - int64(memorySnapshots[i-1].HeapAlloc)
			t.Logf("Snapshot %d (msg %d): HeapAlloc=%s, delta=%+d bytes", i, i, formatBytes(snap.HeapAlloc), growth)
		}
	}

	// Check for potential memory leaks
	// With 10 x 1MB messages, we shouldn't retain more than ~20MB after cleanup
	// (accounting for some buffers and GC timing)
	expectedMaxGrowth := uint64(30 * 1024 * 1024) // 30MB threshold for large messages
	if finalStats.HeapAlloc > afterStartupStats.HeapAlloc+expectedMaxGrowth {
		t.Logf("WARNING: Significant memory growth detected: %s (threshold: %s)",
			formatBytes(uint64(memoryGrowth)), formatBytes(expectedMaxGrowth))
	} else {
		t.Logf("Memory usage within acceptable limits")
	}

	// Verify message count and sizes
	if receivedCount < messageCount {
		t.Errorf("Expected %d large messages, received %d", messageCount, receivedCount)
	}

	// Verify received message sizes (check approximately 1MB, allowing for message ID suffix)
	for i, size := range receivedSizes {
		// Messages are approximately 1MB (minus a few bytes for the identifier suffix)
		minExpectedSize := messageSize - 20
		if size < minExpectedSize {
			t.Errorf("Message %d size too small: got %d, want at least %d", i, size, minExpectedSize)
		}
	}

	t.Logf("Performance test (10 x 1MB messages) completed successfully")
}

// setupMQTTSubscriberWithCounter creates an MQTT client that counts received messages
func setupMQTTSubscriberWithCounter(t *testing.T, topic string, count *int, target int, done chan struct{}) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", mqttBroker, mqttPort))
	opts.SetClientID(fmt.Sprintf("perf-subscriber-%d", time.Now().UnixNano()))
	opts.SetConnectTimeout(10 * time.Second)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect MQTT subscriber: %v", token.Error())
	}

	token = client.Subscribe(topic, 1, func(c mqtt.Client, m mqtt.Message) {
		*count++
		if *count >= target {
			select {
			case <-done:
				// Already closed
			default:
				close(done)
			}
		}
	})
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to subscribe to topic %s: %v", topic, token.Error())
	}

	return client
}

// setupMQTTSubscriberWithSizeTracking creates an MQTT client that tracks received message sizes
func setupMQTTSubscriberWithSizeTracking(t *testing.T, topic string, count *int, sizes *[]int, target int, done chan struct{}) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", mqttBroker, mqttPort))
	opts.SetClientID(fmt.Sprintf("perf-subscriber-large-%d", time.Now().UnixNano()))
	opts.SetConnectTimeout(10 * time.Second)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect MQTT subscriber: %v", token.Error())
	}

	token = client.Subscribe(topic, 1, func(c mqtt.Client, m mqtt.Message) {
		*sizes = append(*sizes, len(m.Payload()))
		*count++
		if *count >= target {
			select {
			case <-done:
				// Already closed
			default:
				close(done)
			}
		}
	})
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to subscribe to topic %s: %v", topic, token.Error())
	}

	return client
}
