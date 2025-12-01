package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
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
	reader       *kafka.Reader
	writer       *kafka.Writer
	dynamicWrite bool // If true, topic is specified per message
	broker       string
	readTopic    string
	groupID      string
	logger       *zap.Logger
}

// NewClient creates a new Kafka client with separate read and write topics
// and connection recovery support
func NewClient(broker string, readTopic string, writeTopic string, groupID string, logger *zap.Logger) (*Client, error) {
	if broker == "" {
		return nil, fmt.Errorf("no kafka broker provided")
	}

	var reader *kafka.Reader
	if readTopic != "" {
		reader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{broker},
			Topic:          readTopic,
			GroupID:        groupID,
			StartOffset:    kafka.FirstOffset, // Start from beginning for new consumer groups
			CommitInterval: 0,                 // Disable auto-commit; we commit manually after successful processing
			// Connection recovery settings
			MaxAttempts:       0, // Unlimited retries for connection failures
			ReadBackoffMin:    100 * time.Millisecond,
			ReadBackoffMax:    1 * time.Second,
			HeartbeatInterval: 3 * time.Second,
			SessionTimeout:    30 * time.Second,
			RebalanceTimeout:  30 * time.Second,
		})
	}

	dynamicWrite := writeTopic == ""
	var writer *kafka.Writer
	if !dynamicWrite {
		writer = &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			Topic:                  writeTopic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
			// Connection recovery settings
			MaxAttempts:     maxWriteRetries,
			WriteBackoffMin: 100 * time.Millisecond,
			WriteBackoffMax: 1 * time.Second,
			BatchTimeout:    100 * time.Millisecond,
			RequiredAcks:    kafka.RequireOne,
		}
	} else {
		// Create a writer without a fixed topic for dynamic topic mapping
		writer = &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
			// Connection recovery settings
			MaxAttempts:     maxWriteRetries,
			WriteBackoffMin: 100 * time.Millisecond,
			WriteBackoffMax: 1 * time.Second,
			BatchTimeout:    100 * time.Millisecond,
			RequiredAcks:    kafka.RequireOne,
		}
	}

	return &Client{
		reader:       reader,
		writer:       writer,
		dynamicWrite: dynamicWrite,
		broker:       broker,
		readTopic:    readTopic,
		groupID:      groupID,
		logger:       logger,
	}, nil
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
