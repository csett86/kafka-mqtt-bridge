package bridge

import (
	"context"
	"fmt"

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
	// Initialize Kafka client
	kafkaClient, err := kafka.NewClient(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topic,
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

	// Message processing loop
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-b.done:
			return nil
		default:
			// Read from Kafka
			msg, err := b.kafkaClient.ReadMessage(ctx)
			if err != nil {
				b.logger.Error("Failed to read message from Kafka", zap.Error(err))
				continue
			}

			// Publish to MQTT
			if err := b.mqttClient.Publish(b.config.MQTT.Topic, msg.Value); err != nil {
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