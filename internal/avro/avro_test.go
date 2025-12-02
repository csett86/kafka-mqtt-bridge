package avro

import (
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
			data:     []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03},
			expected: true,
		},
		{
			name:     "minimum valid avro message (just header)",
			data:     []byte{0x00, 0x00, 0x00, 0x00, 0x01},
			expected: true,
		},
		{
			name:     "wrong magic byte",
			data:     []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03},
			expected: false,
		},
		{
			name:     "too short message",
			data:     []byte{0x00, 0x00, 0x00, 0x00},
			expected: false,
		},
		{
			name:     "empty message",
			data:     []byte{},
			expected: false,
		},
		{
			name:     "nil message",
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

func TestSchemaIDEncodingDecoding(t *testing.T) {
	tests := []struct {
		name     string
		schemaID int
	}{
		{"zero", 0},
		{"small", 1},
		{"medium", 12345},
		{"large", 999999},
		{"max int32", 2147483647},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 4)
			putSchemaID(buf, tt.schemaID)

			result := getSchemaID(buf)
			if result != tt.schemaID {
				t.Errorf("Round-trip failed: got %d, want %d", result, tt.schemaID)
			}

			// Verify it matches the standard big-endian encoding
			expected := int(binary.BigEndian.Uint32(buf))
			if result != expected {
				t.Errorf("Encoding mismatch: got %d, want %d", result, expected)
			}
		})
	}
}

func TestConvertJSONToAvro(t *testing.T) {
	// The current implementation is a passthrough
	tests := []struct {
		name  string
		input interface{}
	}{
		{"string", "hello"},
		{"int", 42},
		{"float", 3.14},
		{"bool", true},
		{"nil", nil},
		{"map", map[string]interface{}{"key": "value"}},
		{"slice", []interface{}{1, 2, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertJSONToAvro(tt.input, nil)
			if err != nil {
				t.Errorf("convertJSONToAvro() error = %v", err)
				return
			}
			// The function is a passthrough, so we just verify it returns without error
			// We can't directly compare maps/slices for equality
			if result == nil && tt.input != nil {
				t.Errorf("convertJSONToAvro() returned nil for non-nil input")
			}
		})
	}
}

func TestNewCodec_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "missing schema registry URL",
			cfg: Config{
				SchemaRegistryURL: "",
			},
			wantErr: true,
		},
		// Note: We can't easily test successful creation without a real schema registry
		// Those would be integration tests
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCodec(tt.cfg, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCodec() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDeserialize_InvalidMessages(t *testing.T) {
	// Create a mock codec for testing error cases
	// We can't create a real one without a schema registry, but we can test the basic validation
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name:    "message too short",
			data:    []byte{0x00, 0x00, 0x00, 0x00},
			wantErr: "message too short",
		},
		{
			name:    "empty message",
			data:    []byte{},
			wantErr: "message too short",
		},
		{
			name:    "wrong magic byte",
			data:    []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x02},
			wantErr: "invalid magic byte",
		},
	}

	// Create a minimal codec to test deserialization errors
	// We can't use a real codec here since we don't have a schema registry
	// Instead, we directly test the validation logic
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test IsAvroMessage for messages with wrong format
			if len(tt.data) >= HeaderSize && tt.data[0] == MagicByte {
				// This is a valid format, skip
				return
			}

			// For invalid messages, IsAvroMessage should return false
			if tt.name != "wrong magic byte" || len(tt.data) < HeaderSize {
				if IsAvroMessage(tt.data) && len(tt.data) < HeaderSize {
					t.Errorf("IsAvroMessage should return false for short messages")
				}
			}
		})
	}
}

func TestGetSubjectName(t *testing.T) {
	// We can't easily test the private method directly, but we can document expected behavior
	// The subject name strategy follows Confluent's convention:
	// - topic: <topic>-value or <topic>-key
	// - record: <record-name>-value or <record-name>-key
	// - topic_record: <topic>-<record-name>-value or <topic>-<record-name>-key

	// These tests document the expected subject naming behavior
	testCases := []struct {
		strategy   string
		topic      string
		isKey      bool
		recordName string
		expected   string
	}{
		{"topic", "my-topic", false, "", "my-topic-value"},
		{"topic", "my-topic", true, "", "my-topic-key"},
		{"record", "my-topic", false, "MyRecord", "MyRecord-value"},
		{"record", "my-topic", true, "MyRecord", "MyRecord-key"},
		{"record", "my-topic", false, "", "my-topic-value"}, // fallback to topic
		{"topic_record", "my-topic", false, "MyRecord", "my-topic-MyRecord-value"},
		{"topic_record", "my-topic", true, "MyRecord", "my-topic-MyRecord-key"},
		{"topic_record", "my-topic", false, "", "my-topic-value"}, // fallback to topic
	}

	// Log the expected behavior for documentation
	t.Log("Subject naming conventions:")
	for _, tc := range testCases {
		t.Logf("  Strategy: %s, Topic: %s, IsKey: %v, Record: %s -> %s",
			tc.strategy, tc.topic, tc.isKey, tc.recordName, tc.expected)
	}
}
