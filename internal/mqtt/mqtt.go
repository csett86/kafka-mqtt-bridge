package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

// TLSConfig holds TLS configuration for MQTT connection
type TLSConfig struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
}

// Client wraps the MQTT client
type Client struct {
	client mqtt.Client
	logger *zap.Logger
}

// NewClient creates a new MQTT client with optional TLS support
func NewClient(broker string, port int, username string, password string, clientID string, logger *zap.Logger) (*Client, error) {
	return NewClientWithTLS(broker, port, username, password, clientID, nil, logger)
}

// NewClientWithTLS creates a new MQTT client with TLS configuration
func NewClientWithTLS(broker string, port int, username string, password string, clientID string, tlsConfig *TLSConfig, logger *zap.Logger) (*Client, error) {
	opts := mqtt.NewClientOptions()

	// Determine protocol and set broker URL
	protocol := "tcp"
	if tlsConfig != nil && tlsConfig.Enabled {
		protocol = "ssl"
	}
	opts.AddBroker(fmt.Sprintf("%s://%s:%d", protocol, broker, port))
	opts.SetClientID(clientID)

	if username != "" {
		opts.SetUsername(username)
		opts.SetPassword(password)
	}

	if tlsConfig != nil && tlsConfig.Enabled {
		tlsCfg, err := createTLSConfig(tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsCfg)
		logger.Info("TLS enabled for MQTT connection",
			zap.Bool("insecureSkipVerify", tlsConfig.InsecureSkipVerify),
			zap.Bool("clientCertEnabled", tlsConfig.CertFile != ""),
		)
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
		zap.Bool("tlsEnabled", tlsConfig != nil && tlsConfig.Enabled),
	)

	return &Client{
		client: mqttClient,
		logger: logger,
	}, nil
}

// createTLSConfig creates a tls.Config from TLSConfig
func createTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

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
