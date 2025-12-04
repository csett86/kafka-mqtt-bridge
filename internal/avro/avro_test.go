package avro

import (
	"context"
	"encoding/binary"
	"testing"
)

func TestIsAvroMessage(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "valid avro message",
			data:     []byte{0x00, 0x00, 0x00, 0x00, 0x04, 't', 'e', 's', 't'},
			expected: true,
		},
		{
			name:     "invalid format version",
			data:     []byte{0x01, 0x00, 0x00, 0x00, 0x04, 't', 'e', 's', 't'},
			expected: false,
		},
		{
			name:     "too short",
			data:     []byte{0x00, 0x00, 0x00},
			expected: false,
		},
		{
			name:     "empty",
			data:     []byte{},
			expected: false,
		},
		{
			name:     "nil",
			data:     nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsAvroMessage(tt.data)
			if result != tt.expected {
				t.Errorf("IsAvroMessage(%v) = %v, want %v", tt.data, result, tt.expected)
			}
		})
	}
}

func TestExtractSchemaID(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		expected  string
		expectErr bool
	}{
		{
			name: "valid message with schema ID",
			data: func() []byte {
				schemaID := "test-schema-id"
				msg := make([]byte, 1+4+len(schemaID))
				msg[0] = formatVersion
				binary.BigEndian.PutUint32(msg[1:5], uint32(len(schemaID)))
				copy(msg[5:], schemaID)
				return msg
			}(),
			expected:  "test-schema-id",
			expectErr: false,
		},
		{
			name:      "invalid format version",
			data:      []byte{0x01, 0x00, 0x00, 0x00, 0x04, 't', 'e', 's', 't'},
			expected:  "",
			expectErr: true,
		},
		{
			name:      "too short for header",
			data:      []byte{0x00, 0x00, 0x00},
			expected:  "",
			expectErr: true,
		},
		{
			name: "schema ID length exceeds message",
			data: func() []byte {
				msg := make([]byte, 5)
				msg[0] = formatVersion
				binary.BigEndian.PutUint32(msg[1:5], 100) // claims 100 bytes but message is too short
				return msg
			}(),
			expected:  "",
			expectErr: true,
		},
		{
			name:      "empty message",
			data:      []byte{},
			expected:  "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ExtractSchemaID(tt.data)
			if tt.expectErr {
				if err == nil {
					t.Errorf("ExtractSchemaID() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("ExtractSchemaID() unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("ExtractSchemaID() = %q, want %q", result, tt.expected)
				}
			}
		})
	}
}

func TestNewSerializerValidation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		cfg       SerializerConfig
		expectErr bool
	}{
		{
			name: "missing schema registry client",
			cfg: SerializerConfig{
				SchemaRegistryClient: nil,
				SchemaName:           "test",
			},
			expectErr: true,
		},
		{
			name: "missing schema name",
			cfg: SerializerConfig{
				SchemaRegistryClient: nil, // Would be set in real test
				SchemaName:           "",
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSerializer(ctx, tt.cfg, nil)
			if tt.expectErr && err == nil {
				t.Errorf("NewSerializer() expected error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("NewSerializer() unexpected error: %v", err)
			}
		})
	}
}

func TestNewDeserializerValidation(t *testing.T) {
	tests := []struct {
		name      string
		cfg       DeserializerConfig
		expectErr bool
	}{
		{
			name: "missing schema registry client",
			cfg: DeserializerConfig{
				SchemaRegistryClient: nil,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDeserializer(tt.cfg, nil)
			if tt.expectErr && err == nil {
				t.Errorf("NewDeserializer() expected error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("NewDeserializer() unexpected error: %v", err)
			}
		})
	}
}
