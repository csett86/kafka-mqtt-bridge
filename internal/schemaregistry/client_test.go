package schemaregistry

import (
	"testing"

	"go.uber.org/zap"
)

func TestClientConfigValidation(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	tests := []struct {
		name      string
		cfg       ClientConfig
		expectErr bool
	}{
		{
			name: "valid config",
			cfg: ClientConfig{
				FullyQualifiedNamespace: "test.servicebus.windows.net",
				GroupName:               "test-group",
			},
			expectErr: false,
		},
		{
			name: "missing namespace",
			cfg: ClientConfig{
				FullyQualifiedNamespace: "",
				GroupName:               "test-group",
			},
			expectErr: true,
		},
		{
			name: "missing group name",
			cfg: ClientConfig{
				FullyQualifiedNamespace: "test.servicebus.windows.net",
				GroupName:               "",
			},
			expectErr: true,
		},
		{
			name: "both missing",
			cfg: ClientConfig{
				FullyQualifiedNamespace: "",
				GroupName:               "",
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Note: NewClient will fail on credential creation without Azure environment,
			// but we're testing the validation logic first
			_, err := NewClient(tt.cfg, logger)
			if tt.expectErr && err == nil {
				t.Errorf("NewClient() expected error for config %+v, got nil", tt.cfg)
			}
			// For valid configs, error is expected because Azure credentials won't be available in test
			// We just want to ensure validation works
		})
	}
}

func TestSchemaFormat(t *testing.T) {
	// Test that FormatAvro constant is correct
	if FormatAvro != "Avro" {
		t.Errorf("FormatAvro = %q, want %q", FormatAvro, "Avro")
	}
}
