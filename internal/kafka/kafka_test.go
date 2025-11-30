package kafka

import (
	"strings"
	"testing"

	"github.com/csett86/kafka-mqtt-bridge/pkg/config"
	"go.uber.org/zap"
)

func TestCreateSASLMechanism(t *testing.T) {
	tests := []struct {
		name        string
		cfg         config.SASLConfig
		wantErr     bool
		errContains string
	}{
		{
			name: "PLAIN mechanism",
			cfg: config.SASLConfig{
				Enabled:   true,
				Mechanism: "PLAIN",
				Username:  "$ConnectionString",
				Password:  "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test-key",
			},
			wantErr: false,
		},
		{
			name: "Empty mechanism defaults to PLAIN",
			cfg: config.SASLConfig{
				Enabled:   true,
				Mechanism: "",
				Username:  "user",
				Password:  "pass",
			},
			wantErr: false,
		},
		{
			name: "Case insensitive PLAIN",
			cfg: config.SASLConfig{
				Enabled:   true,
				Mechanism: "plain",
				Username:  "user",
				Password:  "pass",
			},
			wantErr: false,
		},
		{
			name: "Unsupported mechanism",
			cfg: config.SASLConfig{
				Enabled:   true,
				Mechanism: "SCRAM-SHA-256",
				Username:  "user",
				Password:  "pass",
			},
			wantErr:     true,
			errContains: "unsupported SASL mechanism",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mechanism, err := createSASLMechanism(tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Errorf("createSASLMechanism() expected error containing %q, got nil", tt.errContains)
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("createSASLMechanism() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("createSASLMechanism() unexpected error: %v", err)
				return
			}
			if mechanism == nil {
				t.Error("createSASLMechanism() returned nil mechanism")
			}
		})
	}
}

func TestCreateDialer(t *testing.T) {
	tests := []struct {
		name    string
		saslCfg config.SASLConfig
		tlsCfg  config.TLSConfig
		wantTLS bool
	}{
		{
			name:    "No SASL, No TLS",
			saslCfg: config.SASLConfig{Enabled: false},
			tlsCfg:  config.TLSConfig{Enabled: false},
			wantTLS: false,
		},
		{
			name:    "TLS enabled only",
			saslCfg: config.SASLConfig{Enabled: false},
			tlsCfg:  config.TLSConfig{Enabled: true},
			wantTLS: true,
		},
		{
			name: "SASL and TLS enabled (Azure Event Hubs config)",
			saslCfg: config.SASLConfig{
				Enabled:   true,
				Mechanism: "PLAIN",
				Username:  "$ConnectionString",
				Password:  "test-connection-string",
			},
			tlsCfg:  config.TLSConfig{Enabled: true},
			wantTLS: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialer, err := createDialer(tt.saslCfg, tt.tlsCfg)
			if err != nil {
				t.Fatalf("createDialer() unexpected error: %v", err)
			}
			if dialer == nil {
				t.Fatal("createDialer() returned nil dialer")
			}
			if tt.wantTLS && dialer.TLS == nil {
				t.Error("createDialer() TLS is nil when it should be configured")
			}
			if !tt.wantTLS && dialer.TLS != nil {
				t.Error("createDialer() TLS is configured when it should be nil")
			}
			if tt.saslCfg.Enabled && dialer.SASLMechanism == nil {
				t.Error("createDialer() SASL mechanism is nil when it should be configured")
			}
			if !tt.saslCfg.Enabled && dialer.SASLMechanism != nil {
				t.Error("createDialer() SASL mechanism is configured when it should be nil")
			}
		})
	}
}

func TestNewClientWithConfig(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	tests := []struct {
		name    string
		cfg     ClientConfig
		wantErr bool
	}{
		{
			name: "Valid config with SASL and TLS",
			cfg: ClientConfig{
				Broker:     "test.servicebus.windows.net:9093",
				ReadTopic:  "test-topic",
				WriteTopic: "test-topic",
				GroupID:    "$Default",
				SASL: config.SASLConfig{
					Enabled:   true,
					Mechanism: "PLAIN",
					Username:  "$ConnectionString",
					Password:  "test-connection-string",
				},
				TLS:    config.TLSConfig{Enabled: true},
				Logger: logger,
			},
			wantErr: false,
		},
		{
			name: "Valid config without SASL",
			cfg: ClientConfig{
				Broker:     "localhost:9092",
				ReadTopic:  "test-topic",
				WriteTopic: "test-topic",
				GroupID:    "test-group",
				Logger:     logger,
			},
			wantErr: false,
		},
		{
			name: "Empty broker",
			cfg: ClientConfig{
				Broker: "",
				Logger: logger,
			},
			wantErr: true,
		},
		{
			name: "Unsupported SASL mechanism",
			cfg: ClientConfig{
				Broker: "localhost:9092",
				SASL: config.SASLConfig{
					Enabled:   true,
					Mechanism: "UNSUPPORTED",
				},
				Logger: logger,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClientWithConfig(tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Error("NewClientWithConfig() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("NewClientWithConfig() unexpected error: %v", err)
				return
			}
			if client == nil {
				t.Error("NewClientWithConfig() returned nil client")
				return
			}
			// Clean up
			client.Close()
		})
	}
}
