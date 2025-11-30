package bridge

import (
	"context"
	"fmt"
	"sync"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/csett86/kafka-mqtt-bridge/internal/kafka"
	"github.com/csett86/kafka-mqtt-bridge/internal/mqtt"
	"github.com/csett86/kafka-mqtt-bridge/pkg/config"
	"github.com/csett86/kafka-mqtt-bridge/pkg/topicmapper"
	"go.uber.org/zap"
)

// Bridge manages the connection between Kafka and MQTT
type Bridge struct {
	kafkaClient       *kafka.Client
	mqttClient        *mqtt.Client
	config            *config.Config
	logger            *zap.Logger
	done              chan struct{}
	mqttToKafkaMapper *topicmapper.Mapper // Maps MQTT topics to Kafka topics
	kafkaToMQTTMapper *topicmapper.Mapper // Maps Kafka topics to MQTT topics
}

// New creates a new Bridge instance
func New(cfg *config.Config, logger *zap.Logger) (*Bridge, error) {
	// Determine if we need dynamic topic writing for Kafka
	// If topic_mappings are configured, we use dynamic topics
	kafkaDestTopic := cfg.Kafka.DestTopic
	if len(cfg.Kafka.TopicMappings) > 0 {
		kafkaDestTopic = "" // Use dynamic topic writing
	}

	// Initialize Kafka client with full configuration support (SASL/TLS)
	kafkaClient, err := kafka.NewClientWithConfig(kafka.ClientConfig{
		Broker:     cfg.Kafka.Broker,
		ReadTopic:  cfg.Kafka.SourceTopic,
		WriteTopic: kafkaDestTopic,
		GroupID:    cfg.Kafka.GroupID,
		SASL:       cfg.Kafka.SASL,
		TLS:        cfg.Kafka.TLS,
		Logger:     logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Prepare TLS configuration if enabled
	var tlsConfig *mqtt.TLSConfig
	if cfg.MQTT.TLS.Enabled {
		tlsConfig = &mqtt.TLSConfig{
			Enabled:            cfg.MQTT.TLS.Enabled,
			CAFile:             cfg.MQTT.TLS.CAFile,
			CertFile:           cfg.MQTT.TLS.CertFile,
			KeyFile:            cfg.MQTT.TLS.KeyFile,
			InsecureSkipVerify: cfg.MQTT.TLS.InsecureSkipVerify,
		}
	}

	// Initialize MQTT client with TLS support
	mqttClient, err := mqtt.NewClientWithTLS(
		cfg.MQTT.Broker,
		cfg.MQTT.Port,
		cfg.MQTT.Username,
		cfg.MQTT.Password,
		cfg.MQTT.ClientID,
		tlsConfig,
		logger,
	)
	if err != nil {
		kafkaClient.Close()
		return nil, fmt.Errorf("failed to create MQTT client: %w", err)
	}

	// Create topic mappers
	var mqttToKafkaMapper *topicmapper.Mapper
	if len(cfg.Kafka.TopicMappings) > 0 {
		mqttToKafkaMapper, err = topicmapper.New(cfg.Kafka.TopicMappings)
		if err != nil {
			kafkaClient.Close()
			mqttClient.Disconnect()
			return nil, fmt.Errorf("failed to create MQTT→Kafka topic mapper: %w", err)
		}
	}

	var kafkaToMQTTMapper *topicmapper.Mapper
	if len(cfg.MQTT.TopicMappings) > 0 {
		kafkaToMQTTMapper, err = topicmapper.New(cfg.MQTT.TopicMappings)
		if err != nil {
			kafkaClient.Close()
			mqttClient.Disconnect()
			return nil, fmt.Errorf("failed to create Kafka→MQTT topic mapper: %w", err)
		}
	}

	return &Bridge{
		kafkaClient:       kafkaClient,
		mqttClient:        mqttClient,
		config:            cfg,
		logger:            logger,
		done:              make(chan struct{}),
		mqttToKafkaMapper: mqttToKafkaMapper,
		kafkaToMQTTMapper: kafkaToMQTTMapper,
	}, nil
}

// Start begins the bridge operation
func (b *Bridge) Start(ctx context.Context) error {
	b.logger.Info("Starting bridge", zap.String("name", b.config.Bridge.Name))

	var wg sync.WaitGroup

	// Start Kafka→MQTT bridging if configured
	hasKafkaSource := b.config.Kafka.SourceTopic != ""
	hasMQTTMappings := b.kafkaToMQTTMapper != nil && b.kafkaToMQTTMapper.HasMappings()
	kafkaToMQTTEnabled := hasKafkaSource && (b.config.MQTT.DestTopic != "" || hasMQTTMappings)

	if kafkaToMQTTEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.runKafkaToMQTT(ctx)
		}()
		if hasMQTTMappings {
			b.logger.Info("Started Kafka→MQTT bridge with topic mappings",
				zap.String("kafkaTopic", b.config.Kafka.SourceTopic),
				zap.Int("mappings", len(b.config.MQTT.TopicMappings)))
		} else {
			b.logger.Info("Started Kafka→MQTT bridge",
				zap.String("kafkaTopic", b.config.Kafka.SourceTopic),
				zap.String("mqttTopic", b.config.MQTT.DestTopic))
		}
	}

	// Start MQTT→Kafka bridging if configured
	hasKafkaMappings := b.mqttToKafkaMapper != nil && b.mqttToKafkaMapper.HasMappings()
	mqttToKafkaEnabled := (b.config.MQTT.SourceTopic != "" && b.config.Kafka.DestTopic != "") || hasKafkaMappings

	if mqttToKafkaEnabled {
		if err := b.startMQTTToKafka(ctx); err != nil {
			return fmt.Errorf("failed to start MQTT→Kafka bridge: %w", err)
		}
		if hasKafkaMappings {
			b.logger.Info("Started MQTT→Kafka bridge with topic mappings",
				zap.Int("mappings", len(b.config.Kafka.TopicMappings)))
		} else {
			b.logger.Info("Started MQTT→Kafka bridge",
				zap.String("mqttTopic", b.config.MQTT.SourceTopic),
				zap.String("kafkaTopic", b.config.Kafka.DestTopic))
		}
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

			// Determine the target MQTT topic
			var mqttTopic string
			if b.kafkaToMQTTMapper != nil && b.kafkaToMQTTMapper.HasMappings() {
				// Use topic mapper to determine target topic
				mqttTopic = b.kafkaToMQTTMapper.Map(msg.Topic)
				if mqttTopic == "" {
					b.logger.Warn("No matching topic mapping for Kafka topic",
						zap.String("kafkaTopic", msg.Topic))
					continue
				}
			} else {
				// Use static destination topic
				mqttTopic = b.config.MQTT.DestTopic
			}

			// Publish to MQTT
			if err := b.mqttClient.Publish(mqttTopic, msg.Value); err != nil {
				b.logger.Error("Failed to publish message to MQTT", zap.Error(err))
				continue
			}

			b.logger.Debug("Message bridged",
				zap.String("from", "kafka"),
				zap.String("kafkaTopic", msg.Topic),
				zap.String("mqttTopic", mqttTopic),
				zap.Int("size", len(msg.Value)),
			)
		}
	}
}

// startMQTTToKafka sets up MQTT subscription and forwards messages to Kafka
func (b *Bridge) startMQTTToKafka(ctx context.Context) error {
	handler := func(client pahomqtt.Client, msg pahomqtt.Message) {
		// Determine the target Kafka topic
		var kafkaTopic string
		if b.mqttToKafkaMapper != nil && b.mqttToKafkaMapper.HasMappings() {
			// Use topic mapper to determine target topic
			kafkaTopic = b.mqttToKafkaMapper.Map(msg.Topic())
			if kafkaTopic == "" {
				b.logger.Warn("No matching topic mapping for MQTT topic",
					zap.String("mqttTopic", msg.Topic()))
				return
			}
			// Write to the dynamically determined Kafka topic
			if err := b.kafkaClient.WriteMessageToTopic(ctx, kafkaTopic, nil, msg.Payload()); err != nil {
				b.logger.Error("Failed to write message to Kafka", zap.Error(err))
				return
			}
		} else {
			kafkaTopic = b.config.Kafka.DestTopic
			// Write to the static destination topic
			if err := b.kafkaClient.WriteMessage(ctx, nil, msg.Payload()); err != nil {
				b.logger.Error("Failed to write message to Kafka", zap.Error(err))
				return
			}
		}

		b.logger.Debug("Message bridged",
			zap.String("from", "mqtt"),
			zap.String("mqttTopic", msg.Topic()),
			zap.String("kafkaTopic", kafkaTopic),
			zap.Int("size", len(msg.Payload())),
		)
	}

	// Subscribe to topics based on configuration
	if b.mqttToKafkaMapper != nil && b.mqttToKafkaMapper.HasMappings() {
		// Subscribe to all source patterns from topic mappings
		for _, pattern := range b.mqttToKafkaMapper.GetSubscriptionPatterns() {
			if err := b.mqttClient.Subscribe(pattern, handler); err != nil {
				return fmt.Errorf("failed to subscribe to MQTT topic pattern %s: %w", pattern, err)
			}
			b.logger.Debug("Subscribed to MQTT topic pattern", zap.String("pattern", pattern))
		}
	} else if b.config.MQTT.SourceTopic != "" {
		// Subscribe to static source topic
		if err := b.mqttClient.Subscribe(b.config.MQTT.SourceTopic, handler); err != nil {
			return fmt.Errorf("failed to subscribe to MQTT topic: %w", err)
		}
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
