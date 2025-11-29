package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
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

// WriteMessage writes a single message to Kafka
func (c *Client) WriteMessage(ctx context.Context, key []byte, value []byte) error {
	err := c.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}
	return nil
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
