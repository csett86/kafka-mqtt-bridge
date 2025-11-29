package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
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
	reader *kafka.Reader
	writer *kafka.Writer
	logger *zap.Logger
}

// NewClient creates a new Kafka client with separate read and write topics
func NewClient(brokers []string, readTopic string, writeTopic string, groupID string, logger *zap.Logger) (*Client, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers provided")
	}

	var reader *kafka.Reader
	if readTopic != "" {
		reader = kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   readTopic,
			GroupID: groupID,
		})
	}

	var writer *kafka.Writer
	if writeTopic != "" {
		writer = &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  writeTopic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
	}

	return &Client{
		reader: reader,
		writer: writer,
		logger: logger,
	}, nil
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
	var lastErr error
	for i := 0; i < maxWriteRetries; i++ {
		err := c.writer.WriteMessages(ctx, kafka.Message{
			Key:   key,
			Value: value,
		})
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
