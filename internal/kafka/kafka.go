package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/csett86/kafka-mqtt-bridge/pkg/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"go.uber.org/zap"
)

const (
	// maxWriteRetries is the maximum number of retries for transient write errors
	maxWriteRetries = 10
	// retryBackoff is the delay between retries
	retryBackoff = 500 * time.Millisecond
)

// Client wraps the Kafka reader and writer
type Client struct {
	reader       *kafka.Reader
	writer       *kafka.Writer
	dynamicWrite bool // If true, topic is specified per message
	brokers      []string
	logger       *zap.Logger
	dialer       *kafka.Dialer
}

// ClientConfig holds configuration for creating a Kafka client
type ClientConfig struct {
	Brokers    []string
	ReadTopic  string
	WriteTopic string
	GroupID    string
	SASL       config.SASLConfig
	TLS        config.TLSConfig
	Logger     *zap.Logger
}

// createDialer creates a Kafka dialer with optional SASL and TLS configuration
func createDialer(saslCfg config.SASLConfig, tlsCfg config.TLSConfig) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// Configure TLS if enabled
	if tlsCfg.Enabled {
		dialer.TLS = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	// Configure SASL authentication if enabled
	if saslCfg.Enabled {
		mechanism, err := createSASLMechanism(saslCfg)
		if err != nil {
			return nil, err
		}
		dialer.SASLMechanism = mechanism
	}

	return dialer, nil
}

// createSASLMechanism creates the appropriate SASL mechanism based on configuration
func createSASLMechanism(cfg config.SASLConfig) (sasl.Mechanism, error) {
	mechanism := strings.ToUpper(cfg.Mechanism)
	switch mechanism {
	case "PLAIN", "":
		// Default to PLAIN mechanism (used by Azure Event Hubs)
		return plain.Mechanism{
			Username: cfg.Username,
			Password: cfg.Password,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s (supported: PLAIN)", cfg.Mechanism)
	}
}

// NewClient creates a new Kafka client with separate read and write topics.
//
// Deprecated: NewClient is deprecated. Use NewClientWithConfig for full configuration support.
func NewClient(brokers []string, readTopic string, writeTopic string, groupID string, logger *zap.Logger) (*Client, error) {
	return NewClientWithConfig(ClientConfig{
		Brokers:    brokers,
		ReadTopic:  readTopic,
		WriteTopic: writeTopic,
		GroupID:    groupID,
		Logger:     logger,
	})
}

// NewClientWithConfig creates a new Kafka client with full configuration support
// including SASL authentication and TLS encryption for Azure Event Hubs compatibility
func NewClientWithConfig(cfg ClientConfig) (*Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers provided")
	}

	// Create dialer with SASL/TLS configuration
	dialer, err := createDialer(cfg.SASL, cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka dialer: %w", err)
	}

	var reader *kafka.Reader
	if cfg.ReadTopic != "" {
		readerConfig := kafka.ReaderConfig{
			Brokers:     cfg.Brokers,
			Topic:       cfg.ReadTopic,
			GroupID:     cfg.GroupID,
			StartOffset: kafka.FirstOffset, // Start from beginning for new consumer groups
			Dialer:      dialer,
		}
		reader = kafka.NewReader(readerConfig)
	}

	dynamicWrite := cfg.WriteTopic == ""
	transport, err := createTransport(cfg.SASL, cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka transport: %w", err)
	}

	var writer *kafka.Writer
	if !dynamicWrite {
		writer = &kafka.Writer{
			Addr:                   kafka.TCP(cfg.Brokers...),
			Topic:                  cfg.WriteTopic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
			Transport:              transport,
		}
	} else {
		// Create a writer without a fixed topic for dynamic topic mapping
		writer = &kafka.Writer{
			Addr:                   kafka.TCP(cfg.Brokers...),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
			Transport:              transport,
		}
	}

	return &Client{
		reader:       reader,
		writer:       writer,
		dynamicWrite: dynamicWrite,
		brokers:      cfg.Brokers,
		logger:       cfg.Logger,
		dialer:       dialer,
	}, nil
}

// createTransport creates a Kafka transport with optional SASL and TLS configuration
func createTransport(saslCfg config.SASLConfig, tlsCfg config.TLSConfig) (*kafka.Transport, error) {
	transport := &kafka.Transport{}

	// Configure TLS if enabled
	if tlsCfg.Enabled {
		transport.TLS = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	// Configure SASL authentication if enabled
	if saslCfg.Enabled {
		mechanism, err := createSASLMechanism(saslCfg)
		if err != nil {
			return nil, err
		}
		transport.SASL = mechanism
	}

	return transport, nil
}

// ReadMessage reads a single message from Kafka
func (c *Client) ReadMessage(ctx context.Context) (*kafka.Message, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}
	return &msg, nil
}

// WriteMessage writes a single message to Kafka with retry logic for transient errors
func (c *Client) WriteMessage(ctx context.Context, key []byte, value []byte) error {
	return c.WriteMessageToTopic(ctx, "", key, value)
}

// WriteMessageToTopic writes a single message to a specific Kafka topic with retry logic
func (c *Client) WriteMessageToTopic(ctx context.Context, topic string, key []byte, value []byte) error {
	var lastErr error
	for i := 0; i < maxWriteRetries; i++ {
		msg := kafka.Message{
			Key:   key,
			Value: value,
		}
		// Only set topic if specified (for dynamic topic mapping)
		if topic != "" {
			msg.Topic = topic
		}
		
		err := c.writer.WriteMessages(ctx, msg)
		if err == nil {
			return nil
		}
		lastErr = err

		// Check if context was cancelled
		if ctx.Err() != nil {
			return fmt.Errorf("failed to write message: %w", ctx.Err())
		}

		// Log retry attempt
		c.logger.Debug("Retrying Kafka write",
			zap.Int("attempt", i+1),
			zap.Int("maxRetries", maxWriteRetries),
			zap.Error(err))

		// Wait before retrying
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed to write message: %w", ctx.Err())
		case <-time.After(retryBackoff):
		}
	}
	return fmt.Errorf("failed to write message after %d retries: %w", maxWriteRetries, lastErr)
}

// Close closes the Kafka client connections
func (c *Client) Close() error {
	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			c.logger.Error("Failed to close Kafka reader", zap.Error(err))
		}
	}
	if c.writer != nil {
		if err := c.writer.Close(); err != nil {
			c.logger.Error("Failed to close Kafka writer", zap.Error(err))
		}
	}
	return nil
}
