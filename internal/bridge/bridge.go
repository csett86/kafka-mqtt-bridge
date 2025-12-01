package bridge

import (
	"context"
	"fmt"
	"sync"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/csett86/kafka-mqtt-bridge/internal/kafka"
	"github.com/csett86/kafka-mqtt-bridge/internal/mqtt"
	"github.com/csett86/kafka-mqtt-bridge/pkg/config"
	"go.uber.org/zap"
)

const (
	// commitTimeout is the timeout for committing offsets after successful MQTT delivery.
	// This timeout is used independently of the main context to ensure commits complete
	// even during graceful shutdown.
	commitTimeout = 30 * time.Second
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
	// Prepare Kafka SASL configuration if enabled
	var saslConfig *kafka.SASLConfig
	if cfg.Kafka.SASL.Enabled {
		saslConfig = &kafka.SASLConfig{
			Enabled:   cfg.Kafka.SASL.Enabled,
			Mechanism: cfg.Kafka.SASL.Mechanism,
			Username:  cfg.Kafka.SASL.Username,
			Password:  cfg.Kafka.SASL.Password,
		}
	}

	// Prepare Kafka TLS configuration if enabled
	var kafkaTLSConfig *kafka.TLSConfig
	if cfg.Kafka.TLS.Enabled {
		kafkaTLSConfig = &kafka.TLSConfig{
			Enabled:            cfg.Kafka.TLS.Enabled,
			CAFile:             cfg.Kafka.TLS.CAFile,
			CertFile:           cfg.Kafka.TLS.CertFile,
			KeyFile:            cfg.Kafka.TLS.KeyFile,
			InsecureSkipVerify: cfg.Kafka.TLS.InsecureSkipVerify,
		}
	}

	// Determine Kafka topics from bridge config
	var kafkaReadTopic, kafkaWriteTopic string
	if cfg.Bridge.KafkaToMQTT != nil {
		kafkaReadTopic = cfg.Bridge.KafkaToMQTT.SourceTopic
	}
	if cfg.Bridge.MQTTToKafka != nil {
		kafkaWriteTopic = cfg.Bridge.MQTTToKafka.DestTopic
	}

	// Initialize Kafka client with separate read/write topics and auth config
	kafkaClient, err := kafka.NewClientWithConfig(kafka.ClientConfig{
		Broker:     cfg.Kafka.Broker,
		ReadTopic:  kafkaReadTopic,
		WriteTopic: kafkaWriteTopic,
		GroupID:    cfg.Kafka.GroupID,
		SASL:       saslConfig,
		TLS:        kafkaTLSConfig,
	}, logger)
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

	// Determine CleanSession value (default: false when QoS > 0, true otherwise)
	cleanSession := cfg.MQTT.QoS == 0
	if cfg.MQTT.CleanSession != nil {
		cleanSession = *cfg.MQTT.CleanSession
	}

	// Initialize MQTT client with QoS and CleanSession support
	mqttClient, err := mqtt.NewClientWithConfig(mqtt.ClientConfig{
		Broker:       cfg.MQTT.Broker,
		Port:         cfg.MQTT.Port,
		Username:     cfg.MQTT.Username,
		Password:     cfg.MQTT.Password,
		ClientID:     cfg.MQTT.ClientID,
		TLS:          tlsConfig,
		QoS:          byte(cfg.MQTT.QoS),
		CleanSession: cleanSession,
	}, logger)
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
	b.logger.Info("Starting bridge",
		zap.String("name", b.config.Bridge.Name),
		zap.Int("mqttQoS", b.config.MQTT.QoS),
	)

	var wg sync.WaitGroup

	// Start Kafka→MQTT bridging if configured
	kafkaToMQTTEnabled := b.config.Bridge.KafkaToMQTT != nil &&
		b.config.Bridge.KafkaToMQTT.SourceTopic != "" &&
		b.config.Bridge.KafkaToMQTT.DestTopic != ""

	if kafkaToMQTTEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.runKafkaToMQTT(ctx)
		}()
		b.logger.Info("Started Kafka→MQTT bridge",
			zap.String("kafkaTopic", b.config.Bridge.KafkaToMQTT.SourceTopic),
			zap.String("mqttTopic", b.config.Bridge.KafkaToMQTT.DestTopic),
			zap.Int("qos", b.config.MQTT.QoS))
	}

	// Start MQTT→Kafka bridging if configured
	mqttToKafkaEnabled := b.config.Bridge.MQTTToKafka != nil &&
		b.config.Bridge.MQTTToKafka.SourceTopic != "" &&
		b.config.Bridge.MQTTToKafka.DestTopic != ""

	if mqttToKafkaEnabled {
		if err := b.startMQTTToKafka(ctx); err != nil {
			return fmt.Errorf("failed to start MQTT→Kafka bridge: %w", err)
		}
		b.logger.Info("Started MQTT→Kafka bridge",
			zap.String("mqttTopic", b.config.Bridge.MQTTToKafka.SourceTopic),
			zap.String("kafkaTopic", b.config.Bridge.MQTTToKafka.DestTopic),
			zap.Int("qos", b.config.MQTT.QoS))
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
			// Fetch from Kafka (without committing offset)
			msg, err := b.kafkaClient.FetchMessage(ctx)
			if err != nil {
				// Check if context was cancelled
				if ctx.Err() != nil {
					return
				}
				b.logger.Error("Failed to fetch message from Kafka", zap.Error(err))
				continue
			}

			// Use static destination topic from bridge config
			mqttTopic := b.config.Bridge.KafkaToMQTT.DestTopic

			if err := b.mqttClient.Publish(mqttTopic, msg.Value); err != nil {
				b.logger.Error("Failed to publish message to MQTT", zap.Error(err))
				// Don't commit the offset - the message will be redelivered
				continue
			}

			// Commit the offset only after successful MQTT delivery.
			// Use a separate context with timeout to ensure commits complete even during
			// graceful shutdown when the main context is canceled.
			func() {
				commitCtx, commitCancel := context.WithTimeout(context.Background(), commitTimeout)
				defer commitCancel()
				if err := b.kafkaClient.CommitMessage(commitCtx, msg); err != nil {
					b.logger.Error("Failed to commit message after MQTT delivery", zap.Error(err))
					// Message was delivered to MQTT but commit failed
					// On restart, it may be redelivered but that's safer than losing messages
				}
			}()

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
		kafkaTopic := b.config.Bridge.MQTTToKafka.DestTopic
		if err := b.kafkaClient.WriteMessage(ctx, nil, msg.Payload()); err != nil {
			b.logger.Error("Failed to write message to Kafka", zap.Error(err))
			// Don't acknowledge the message - it will be redelivered for QoS > 0
			return
		}

		// Acknowledge the MQTT message after successful Kafka delivery
		// This is crucial for QoS 1 (at-least-once) and QoS 2 (exactly-once) semantics
		// For QoS 0 (at-most-once), this is a no-op
		msg.Ack()

		b.logger.Debug("Message bridged",
			zap.String("from", "mqtt"),
			zap.String("mqttTopic", msg.Topic()),
			zap.String("kafkaTopic", kafkaTopic),
			zap.Int("size", len(msg.Payload())),
			zap.Uint8("qos", msg.Qos()),
		)
	}

	mqttSourceTopic := b.config.Bridge.MQTTToKafka.SourceTopic
	if err := b.mqttClient.Subscribe(mqttSourceTopic, handler); err != nil {
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
