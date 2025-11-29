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
	reader       *kafka.Reader
	writer       *kafka.Writer
	dynamicWrite bool // If true, topic is specified per message
	brokers      []string
	logger       *zap.Logger
}

// NewClient creates a new Kafka client with separate read and write topics
func NewClient(brokers []string, readTopic string, writeTopic string, groupID string, logger *zap.Logger) (*Client, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers provided")
	}

	var reader *kafka.Reader
	if readTopic != "" {
		reader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       readTopic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset, // Start from beginning for new consumer groups
		})
	}

	dynamicWrite := writeTopic == ""
	var writer *kafka.Writer
	if !dynamicWrite {
		writer = &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  writeTopic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
	} else {
		// Create a writer without a fixed topic for dynamic topic mapping
		writer = &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
	}

	return &Client{
		reader:       reader,
		writer:       writer,
		dynamicWrite: dynamicWrite,
		brokers:      brokers,
		logger:       logger,
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
