package mqtt

import (
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

// Client wraps the MQTT client
type Client struct {
	client mqtt.Client
	logger *zap.Logger
}

// NewClient creates a new MQTT client
func NewClient(broker string, port int, username string, password string, clientID string, logger *zap.Logger) (*Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID(clientID)

	if username != "" {
		opts.SetUsername(username)
		opts.SetPassword(password)
	}

	opts.SetAutoReconnect(true)
	opts.SetCleanSession(true)

	mqttClient := mqtt.NewClient(opts)

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	logger.Info("Connected to MQTT broker",
		zap.String("broker", broker),
		zap.Int("port", port),
	)

	return &Client{
		client: mqttClient,
		logger: logger,
	}, nil
}

// Publish publishes a message to an MQTT topic
func (c *Client) Publish(topic string, payload []byte) error {
	token := c.client.Publish(topic, 1, false, payload)
	if token.Wait(); token.Error() != nil {
		return fmt.Errorf("failed to publish message: %w", token.Error())
	}
	return nil
}

// Subscribe subscribes to an MQTT topic
func (c *Client) Subscribe(topic string, callback mqtt.MessageHandler) error {
	token := c.client.Subscribe(topic, 1, callback)
	if token.Wait(); token.Error() != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", token.Error())
	}
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
