package mqtt

import (
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

// subscription represents a topic subscription with its handler
type subscription struct {
	topic   string
	handler mqtt.MessageHandler
}

// Client wraps the MQTT client with connection recovery support
type Client struct {
	client        mqtt.Client
	logger        *zap.Logger
	subscriptions []subscription
	subMu         sync.RWMutex
}

// NewClient creates a new MQTT client with automatic reconnection and subscription recovery
func NewClient(broker string, port int, username string, password string, clientID string, logger *zap.Logger) (*Client, error) {
	c := &Client{
		logger:        logger,
		subscriptions: make([]subscription, 0),
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID(clientID)

	if username != "" {
		opts.SetUsername(username)
		opts.SetPassword(password)
	}

	// Enable automatic reconnection with exponential backoff
	opts.SetAutoReconnect(true)
	opts.SetCleanSession(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(1 * time.Second)
	opts.SetMaxReconnectInterval(60 * time.Second)

	// Set connection lost handler
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		logger.Warn("MQTT connection lost, will attempt to reconnect",
			zap.Error(err),
		)
	})

	// Set reconnect handler to restore subscriptions
	opts.SetReconnectingHandler(func(client mqtt.Client, opts *mqtt.ClientOptions) {
		logger.Info("MQTT attempting to reconnect...")
	})

	// Set OnConnect handler to restore subscriptions after reconnection
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		logger.Info("MQTT connected/reconnected",
			zap.String("broker", broker),
			zap.Int("port", port),
		)
		// Restore subscriptions after reconnection
		c.restoreSubscriptions()
	})

	mqttClient := mqtt.NewClient(opts)
	c.client = mqttClient

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	logger.Info("Connected to MQTT broker",
		zap.String("broker", broker),
		zap.Int("port", port),
	)

	return c, nil
}

// restoreSubscriptions restores all subscriptions after a reconnection
func (c *Client) restoreSubscriptions() {
	c.subMu.RLock()
	subs := make([]subscription, len(c.subscriptions))
	copy(subs, c.subscriptions)
	c.subMu.RUnlock()

	for _, sub := range subs {
		token := c.client.Subscribe(sub.topic, 1, sub.handler)
		if token.Wait() && token.Error() != nil {
			c.logger.Error("Failed to restore subscription after reconnect",
				zap.String("topic", sub.topic),
				zap.Error(token.Error()),
			)
		} else {
			c.logger.Debug("Restored subscription after reconnect",
				zap.String("topic", sub.topic),
			)
		}
	}
}

// Publish publishes a message to an MQTT topic
func (c *Client) Publish(topic string, payload []byte) error {
	token := c.client.Publish(topic, 1, false, payload)
	if token.Wait(); token.Error() != nil {
		return fmt.Errorf("failed to publish message: %w", token.Error())
	}
	return nil
}

// Subscribe subscribes to an MQTT topic and stores the subscription for recovery
func (c *Client) Subscribe(topic string, callback mqtt.MessageHandler) error {
	token := c.client.Subscribe(topic, 1, callback)
	if token.Wait(); token.Error() != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", token.Error())
	}

	// Store subscription for recovery after reconnection
	c.subMu.Lock()
	c.subscriptions = append(c.subscriptions, subscription{
		topic:   topic,
		handler: callback,
	})
	c.subMu.Unlock()

	return nil
}

// IsConnected checks if the MQTT client is connected
func (c *Client) IsConnected() bool {
	return c.client.IsConnected()
}

// Disconnect closes the MQTT connection
func (c *Client) Disconnect() {
	if c.client.IsConnected() {
		c.client.Disconnect(250)
		c.logger.Info("Disconnected from MQTT broker")
	}
}
