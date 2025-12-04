package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

const (
	// maxPublishRetries is the maximum number of retries for publish failures
	maxPublishRetries = 5
	// publishRetryBackoff is the initial delay between publish retries
	publishRetryBackoff = 500 * time.Millisecond
	// maxPublishRetryBackoff is the maximum delay between publish retries
	maxPublishRetryBackoff = 10 * time.Second
	// publishTimeout is the timeout for each publish attempt
	publishTimeout = 30 * time.Second
)

// TLSConfig holds TLS configuration for MQTT connection
type TLSConfig struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
}

// subscription represents a topic subscription with its handler and QoS
type subscription struct {
	topic   string
	handler mqtt.MessageHandler
	qos     byte
}

// ClientConfig holds extended configuration for MQTT client
type ClientConfig struct {
	Broker       string
	Port         int
	Username     string
	Password     string
	ClientID     string
	TLS          *TLSConfig
	QoS          byte // QoS level for MQTT operations (0, 1, or 2)
	CleanSession bool // CleanSession flag (should be false for QoS > 0)
}

// Client wraps the MQTT client with connection recovery support
type Client struct {
	client        mqtt.Client
	logger        *zap.Logger
	subscriptions []subscription
	subMu         sync.RWMutex
	qos           byte // QoS level for MQTT operations (0, 1, or 2)
}

// NewClient creates a new MQTT client with automatic reconnection and subscription recovery
func NewClient(broker string, port int, username string, password string, clientID string, logger *zap.Logger) (*Client, error) {
	return NewClientWithTLS(broker, port, username, password, clientID, nil, logger)
}

// NewClientWithTLS creates a new MQTT client with TLS configuration and automatic reconnection
// Uses default QoS of 1 (at-least-once) and CleanSession true
func NewClientWithTLS(broker string, port int, username string, password string, clientID string, tlsConfig *TLSConfig, logger *zap.Logger) (*Client, error) {
	return NewClientWithConfig(ClientConfig{
		Broker:       broker,
		Port:         port,
		Username:     username,
		Password:     password,
		ClientID:     clientID,
		TLS:          tlsConfig,
		QoS:          1,    // Default to at-least-once
		CleanSession: true, // Default for backward compatibility
	}, logger)
}

// NewClientWithConfig creates a new MQTT client with full configuration support
func NewClientWithConfig(cfg ClientConfig, logger *zap.Logger) (*Client, error) {
	// Validate QoS
	if cfg.QoS > 2 {
		return nil, fmt.Errorf("invalid QoS level %d: must be 0, 1, or 2", cfg.QoS)
	}

	c := &Client{
		logger:        logger,
		subscriptions: make([]subscription, 0),
		qos:           cfg.QoS,
	}

	opts := mqtt.NewClientOptions()

	// Determine protocol and set broker URL
	protocol := "tcp"
	if cfg.TLS != nil && cfg.TLS.Enabled {
		protocol = "ssl"
	}
	opts.AddBroker(fmt.Sprintf("%s://%s:%d", protocol, cfg.Broker, cfg.Port))
	opts.SetClientID(cfg.ClientID)

	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}

	if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsCfg, err := createTLSConfig(cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsCfg)
		logger.Info("TLS enabled for MQTT connection",
			zap.Bool("insecureSkipVerify", cfg.TLS.InsecureSkipVerify),
			zap.Bool("clientCertEnabled", cfg.TLS.CertFile != ""),
		)
	}

	opts.SetAutoReconnect(true)
	opts.SetCleanSession(cfg.CleanSession)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(1 * time.Second)
	opts.SetMaxReconnectInterval(60 * time.Second)

	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		logger.Warn("MQTT connection lost, will attempt to reconnect",
			zap.Error(err),
		)
	})

	opts.SetReconnectingHandler(func(client mqtt.Client, opts *mqtt.ClientOptions) {
		logger.Info("MQTT attempting to reconnect...")
	})

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		logger.Info("MQTT connected/reconnected",
			zap.String("broker", cfg.Broker),
			zap.Int("port", cfg.Port),
		)
		c.restoreSubscriptions()
	})

	mqttClient := mqtt.NewClient(opts)
	c.client = mqttClient

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	logger.Info("Connected to MQTT broker",
		zap.String("broker", cfg.Broker),
		zap.Int("port", cfg.Port),
		zap.Bool("tlsEnabled", cfg.TLS != nil && cfg.TLS.Enabled),
		zap.Uint8("qos", cfg.QoS),
		zap.Bool("cleanSession", cfg.CleanSession),
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
		token := c.client.Subscribe(sub.topic, sub.qos, sub.handler)
		if token.Wait() && token.Error() != nil {
			c.logger.Error("Failed to restore subscription after reconnect",
				zap.String("topic", sub.topic),
				zap.Uint8("qos", sub.qos),
				zap.Error(token.Error()),
			)
		} else {
			c.logger.Debug("Restored subscription after reconnect",
				zap.String("topic", sub.topic),
				zap.Uint8("qos", sub.qos),
			)
		}
	}
}

// createTLSConfig creates a tls.Config from TLSConfig
// If no CA file is specified, the system's trusted CA certificates are used
func createTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	// Load custom CA certificate if provided; otherwise use system CA certificates
	if cfg.CAFile != "" {
		caCert, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsCfg.RootCAs = caCertPool
	}

	// Load client certificate and key for mutual TLS
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// Publish publishes a message to an MQTT topic using the client's configured QoS level.
// It includes retry logic with exponential backoff for connection-related failures
// to ensure reliable message delivery during temporary network issues.
func (c *Client) Publish(topic string, payload []byte) error {
	var lastErr error
	backoff := publishRetryBackoff

	for i := 0; i < maxPublishRetries; i++ {
		token := c.client.Publish(topic, c.qos, false, payload)

		// Wait with timeout to avoid indefinite blocking
		if token.WaitTimeout(publishTimeout) {
			if token.Error() == nil {
				return nil
			}
			lastErr = token.Error()
		} else {
			lastErr = fmt.Errorf("publish timeout after %v", publishTimeout)
		}

		// Skip delay on the last attempt
		if i == maxPublishRetries-1 {
			break
		}

		// Log retry attempt with connection status
		c.logger.Warn("MQTT publish failed, retrying",
			zap.Int("attempt", i+1),
			zap.Int("maxRetries", maxPublishRetries),
			zap.Duration("backoff", backoff),
			zap.Bool("connected", c.client.IsConnected()),
			zap.Error(lastErr),
		)

		time.Sleep(backoff)

		// Exponential backoff with cap
		backoff = backoff * 2
		if backoff > maxPublishRetryBackoff {
			backoff = maxPublishRetryBackoff
		}
	}

	return fmt.Errorf("failed to publish message after %d retries: %w", maxPublishRetries, lastErr)
}

// Subscribe subscribes to an MQTT topic and stores the subscription for recovery
// Uses the client's configured QoS level
func (c *Client) Subscribe(topic string, callback mqtt.MessageHandler) error {
	token := c.client.Subscribe(topic, c.qos, callback)
	if token.Wait(); token.Error() != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", token.Error())
	}

	c.subMu.Lock()
	c.subscriptions = append(c.subscriptions, subscription{
		topic:   topic,
		handler: callback,
		qos:     c.qos,
	})
	c.subMu.Unlock()

	return nil
}

// QoS returns the QoS level configured for this client
func (c *Client) QoS() byte {
	return c.qos
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
