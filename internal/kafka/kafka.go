package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

const (
	// maxWriteRetries is the maximum number of retries for transient write errors
	maxWriteRetries = 10
	// retryBackoff is the initial delay between retries
	retryBackoff = 500 * time.Millisecond
	// maxRetryBackoff is the maximum delay between retries
	maxRetryBackoff = 30 * time.Second
	// readRetryDelay is the delay before retrying a failed read
	readRetryDelay = 1 * time.Second
)

// Client wraps the Kafka reader and writer with connection recovery support
type Client struct {
	reader    *kafka.Reader
	writer    *kafka.Writer
	broker    string
	readTopic string
	groupID   string
	logger    *zap.Logger
}

// SASLConfig contains SASL authentication settings for Kafka
type SASLConfig struct {
	Enabled   bool
	Mechanism string // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	Username  string
	Password  string
}

// TLSConfig contains TLS settings for Kafka connection
type TLSConfig struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
}

// ClientConfig contains all configuration for creating a Kafka client
type ClientConfig struct {
	Broker     string
	ReadTopic  string
	WriteTopic string
	GroupID    string
	SASL       *SASLConfig
	TLS        *TLSConfig
}

// NewClient creates a new Kafka client with separate read and write topics
// and connection recovery support
func NewClient(broker string, readTopic string, writeTopic string, groupID string, logger *zap.Logger) (*Client, error) {
	return NewClientWithConfig(ClientConfig{
		Broker:     broker,
		ReadTopic:  readTopic,
		WriteTopic: writeTopic,
		GroupID:    groupID,
	}, logger)
}

// NewClientWithConfig creates a new Kafka client with full configuration support
// including SASL authentication and TLS for Azure Event Hubs compatibility
func NewClientWithConfig(cfg ClientConfig, logger *zap.Logger) (*Client, error) {
	if cfg.Broker == "" {
		return nil, fmt.Errorf("no kafka broker provided")
	}

	// Create dialer with SASL and TLS if configured
	dialer, err := createDialer(cfg.SASL, cfg.TLS, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialer: %w", err)
	}

	// Create transport for writer with SASL and TLS
	var transport *kafka.Transport
	if dialer != nil {
		transport = &kafka.Transport{
			TLS:  dialer.TLS,
			SASL: dialer.SASLMechanism,
		}
	}

	var reader *kafka.Reader
	if cfg.ReadTopic != "" {
		readerConfig := kafka.ReaderConfig{
			Brokers:        []string{cfg.Broker},
			Topic:          cfg.ReadTopic,
			GroupID:        cfg.GroupID,
			StartOffset:    kafka.FirstOffset, // Start from beginning for new consumer groups
			CommitInterval: 0,                 // Disable auto-commit; we commit manually after successful processing
			// Connection recovery settings
			MaxAttempts:       0, // Unlimited retries for connection failures
			ReadBackoffMin:    100 * time.Millisecond,
			ReadBackoffMax:    1 * time.Second,
			HeartbeatInterval: 3 * time.Second,
			SessionTimeout:    30 * time.Second,
			RebalanceTimeout:  30 * time.Second,
		}
		if dialer != nil {
			readerConfig.Dialer = dialer
		}
		reader = kafka.NewReader(readerConfig)
	}

	var writer *kafka.Writer
	if cfg.WriteTopic != "" {
		writer = &kafka.Writer{
			Addr:                   kafka.TCP(cfg.Broker),
			Topic:                  cfg.WriteTopic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
			// Connection recovery settings
			MaxAttempts:     maxWriteRetries,
			WriteBackoffMin: 100 * time.Millisecond,
			WriteBackoffMax: 1 * time.Second,
			BatchTimeout:    100 * time.Millisecond,
			RequiredAcks:    kafka.RequireOne,
		}
		if transport != nil {
			writer.Transport = transport
		}
	}

	return &Client{
		reader:    reader,
		writer:    writer,
		broker:    cfg.Broker,
		readTopic: cfg.ReadTopic,
		groupID:   cfg.GroupID,
		logger:    logger,
	}, nil
}

// createDialer creates a Kafka dialer with optional SASL and TLS configuration
func createDialer(saslCfg *SASLConfig, tlsCfg *TLSConfig, logger *zap.Logger) (*kafka.Dialer, error) {
	if (saslCfg == nil || !saslCfg.Enabled) && (tlsCfg == nil || !tlsCfg.Enabled) {
		return nil, nil
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// Configure TLS
	if tlsCfg != nil && tlsCfg.Enabled {
		tlsConfig, err := createTLSConfig(tlsCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		dialer.TLS = tlsConfig
		logger.Info("Kafka TLS enabled",
			zap.Bool("insecureSkipVerify", tlsCfg.InsecureSkipVerify),
			zap.Bool("clientCertEnabled", tlsCfg.CertFile != ""),
		)
	}

	// Configure SASL
	if saslCfg != nil && saslCfg.Enabled {
		mechanism, err := createSASLMechanism(saslCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}
		dialer.SASLMechanism = mechanism
		logger.Info("Kafka SASL enabled",
			zap.String("mechanism", saslCfg.Mechanism),
			zap.String("username", saslCfg.Username),
		)
	}

	return dialer, nil
}

// createTLSConfig creates a TLS configuration from TLSConfig
func createTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}

	// Load CA certificate if provided
	if cfg.CAFile != "" {
		caCert, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Load client certificate and key for mutual TLS
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// createSASLMechanism creates a SASL mechanism from SASLConfig
func createSASLMechanism(cfg *SASLConfig) (sasl.Mechanism, error) {
	mechanism := strings.ToUpper(cfg.Mechanism)

	switch mechanism {
	case "PLAIN":
		return plain.Mechanism{
			Username: cfg.Username,
			Password: cfg.Password,
		}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, cfg.Username, cfg.Password)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, cfg.Username, cfg.Password)
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %q (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", cfg.Mechanism)
	}
}

// isTransientError checks if the error is a transient connection error that should be retried
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	// Check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Check for connection refused errors
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

// FetchMessage fetches a single message from Kafka without committing the offset.
// The caller must call CommitMessage after successful processing.
func (c *Client) FetchMessage(ctx context.Context) (*kafka.Message, error) {
	backoff := readRetryDelay

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err == nil {
			return &msg, nil
		}

		// Check if context was cancelled
		if ctx.Err() != nil {
			return nil, fmt.Errorf("failed to fetch message: %w", ctx.Err())
		}

		// Check if it's a transient error that should be retried
		if isTransientError(err) {
			c.logger.Warn("Kafka fetch failed, will retry",
				zap.Error(err),
				zap.Duration("backoff", backoff),
			)

			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("failed to fetch message: %w", ctx.Err())
			case <-time.After(backoff):
			}

			// Exponential backoff with cap
			backoff = backoff * 2
			if backoff > maxRetryBackoff {
				backoff = maxRetryBackoff
			}
			continue
		}

		return nil, fmt.Errorf("failed to fetch message: %w", err)
	}
}

// CommitMessage commits the offset for a message after successful processing
func (c *Client) CommitMessage(ctx context.Context, msg *kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, *msg); err != nil {
		return fmt.Errorf("failed to commit message: %w", err)
	}
	return nil
}

// ReadMessage reads a single message from Kafka with automatic reconnection
// Note: This method auto-commits the offset. Use FetchMessage + CommitMessage
// for manual offset control.
func (c *Client) ReadMessage(ctx context.Context) (*kafka.Message, error) {
	backoff := readRetryDelay

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err == nil {
			return &msg, nil
		}

		// Check if context was cancelled
		if ctx.Err() != nil {
			return nil, fmt.Errorf("failed to read message: %w", ctx.Err())
		}

		// Check if it's a transient error that should be retried
		if isTransientError(err) {
			c.logger.Warn("Kafka read failed, will retry",
				zap.Error(err),
				zap.Duration("backoff", backoff),
			)

			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("failed to read message: %w", ctx.Err())
			case <-time.After(backoff):
			}

			// Exponential backoff with cap
			backoff = backoff * 2
			if backoff > maxRetryBackoff {
				backoff = maxRetryBackoff
			}
			continue
		}

		return nil, fmt.Errorf("failed to read message: %w", err)
	}
}

// WriteMessage writes a single message to Kafka with retry logic for transient errors
func (c *Client) WriteMessage(ctx context.Context, key []byte, value []byte) error {
	return c.WriteMessageToTopic(ctx, "", key, value)
}

// WriteMessageToTopic writes a single message to a specific Kafka topic with retry logic
// and exponential backoff for connection recovery
func (c *Client) WriteMessageToTopic(ctx context.Context, topic string, key []byte, value []byte) error {
	var lastErr error
	backoff := retryBackoff

	for i := 0; i < maxWriteRetries; i++ {
		msg := kafka.Message{
			Key:   key,
			Value: value,
		}
		if topic != "" {
			msg.Topic = topic
		}

		err := c.writer.WriteMessages(ctx, msg)
		if err == nil {
			return nil
		}
		lastErr = err

		if ctx.Err() != nil {
			return fmt.Errorf("failed to write message: %w", ctx.Err())
		}

		// Log retry attempt
		c.logger.Warn("Kafka write failed, retrying",
			zap.Int("attempt", i+1),
			zap.Int("maxRetries", maxWriteRetries),
			zap.Duration("backoff", backoff),
			zap.Error(err))

		select {
		case <-ctx.Done():
			return fmt.Errorf("failed to write message: %w", ctx.Err())
		case <-time.After(backoff):
		}

		// Exponential backoff with cap
		backoff = backoff * 2
		if backoff > maxRetryBackoff {
			backoff = maxRetryBackoff
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
