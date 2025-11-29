package bridge

import (
	"context"
	"fmt"
	"sync"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/csett86/kafka-mqtt-bridge/internal/kafka"
	"github.com/csett86/kafka-mqtt-bridge/internal/mqtt"
	"github.com/csett86/kafka-mqtt-bridge/pkg/config"
	"go.uber.org/zap"
)

// Bridge manages the connection between Kafka and MQTT
type Bridge struct {
	kafkaClient *kafka.Client
	mqttClient  *mqtt.Client
	config      *config.Config
	logger      *zap.Logger
	done        chan struct{}
}

// New creates a new Bridge instance
func New(cfg *config.Config, logger *zap.Logger) (*Bridge, error) {
	// Initialize Kafka client with separate read/write topics
	kafkaClient, err := kafka.NewClient(
		cfg.Kafka.Brokers,
		cfg.Kafka.SourceTopic,
		cfg.Kafka.DestTopic,
		cfg.Kafka.GroupID,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Initialize MQTT client
	mqttClient, err := mqtt.NewClient(
		cfg.MQTT.Broker,
		cfg.MQTT.Port,
		cfg.MQTT.Username,
		cfg.MQTT.Password,
		cfg.MQTT.ClientID,
		logger,
	)
	if err != nil {
		kafkaClient.Close()
		return nil, fmt.Errorf("failed to create MQTT client: %w", err)
	}

	return &Bridge{
		kafkaClient: kafkaClient,
		mqttClient:  mqttClient,
		config:      cfg,
		logger:      logger,
		done:        make(chan struct{}),
	}, nil
}

// Start begins the bridge operation
func (b *Bridge) Start(ctx context.Context) error {
	b.logger.Info("Starting bridge", zap.String("name", b.config.Bridge.Name))

	var wg sync.WaitGroup

	// Start Kafka→MQTT bridging if source topic is configured
	if b.config.Kafka.SourceTopic != "" && b.config.MQTT.DestTopic != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.runKafkaToMQTT(ctx)
		}()
		b.logger.Info("Started Kafka→MQTT bridge",
			zap.String("kafkaTopic", b.config.Kafka.SourceTopic),
			zap.String("mqttTopic", b.config.MQTT.DestTopic))
	}

	// Start MQTT→Kafka bridging if source topic is configured
	if b.config.MQTT.SourceTopic != "" && b.config.Kafka.DestTopic != "" {
		if err := b.startMQTTToKafka(ctx); err != nil {
			return fmt.Errorf("failed to start MQTT→Kafka bridge: %w", err)
		}
		b.logger.Info("Started MQTT→Kafka bridge",
			zap.String("mqttTopic", b.config.MQTT.SourceTopic),
			zap.String("kafkaTopic", b.config.Kafka.DestTopic))
	}

	// Wait for context cancellation or done signal
	select {
	case <-ctx.Done():
	case <-b.done:
	}

	wg.Wait()
	return nil
}

// runKafkaToMQTT handles the Kafka→MQTT message bridging
func (b *Bridge) runKafkaToMQTT(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.done:
			return
		default:
			// Read from Kafka
			msg, err := b.kafkaClient.ReadMessage(ctx)
			if err != nil {
				// Check if context was cancelled
				if ctx.Err() != nil {
					return
				}
				b.logger.Error("Failed to read message from Kafka", zap.Error(err))
				continue
			}

			// Publish to MQTT
			if err := b.mqttClient.Publish(b.config.MQTT.DestTopic, msg.Value); err != nil {
				b.logger.Error("Failed to publish message to MQTT", zap.Error(err))
				continue
			}

			b.logger.Debug("Message bridged",
				zap.String("from", "kafka"),
				zap.String("to", "mqtt"),
				zap.Int("size", len(msg.Value)),
			)
		}
	}
}

// startMQTTToKafka sets up MQTT subscription and forwards messages to Kafka
func (b *Bridge) startMQTTToKafka(ctx context.Context) error {
	handler := func(client pahomqtt.Client, msg pahomqtt.Message) {
		// Forward message to Kafka
		if err := b.kafkaClient.WriteMessage(ctx, nil, msg.Payload()); err != nil {
			b.logger.Error("Failed to write message to Kafka", zap.Error(err))
			return
		}

		b.logger.Debug("Message bridged",
			zap.String("from", "mqtt"),
			zap.String("to", "kafka"),
			zap.Int("size", len(msg.Payload())),
		)
	}

	if err := b.mqttClient.Subscribe(b.config.MQTT.SourceTopic, handler); err != nil {
		return fmt.Errorf("failed to subscribe to MQTT topic: %w", err)
	}

	return nil
}

// Stop gracefully stops the bridge
func (b *Bridge) Stop() {
	b.logger.Info("Stopping bridge")
	close(b.done)

	if b.kafkaClient != nil {
		if err := b.kafkaClient.Close(); err != nil {
			b.logger.Error("Error closing Kafka client", zap.Error(err))
		}
	}

	if b.mqttClient != nil {
		b.mqttClient.Disconnect()
	}

	b.logger.Info("Bridge stopped")
}
