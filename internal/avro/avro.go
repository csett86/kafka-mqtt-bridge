// Package avro provides Avro serialization and deserialization with Azure Schema Registry integration.
// It supports encoding and decoding messages using Avro schemas stored in Azure Event Hubs Schema Registry.
package avro

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/csett86/kafka-mqtt-bridge/internal/schemaregistry"
	"github.com/linkedin/goavro/v2"
	"go.uber.org/zap"
)

const (
	// Azure Schema Registry message format version
	// Format: [format version (1 byte)][4-byte schema ID length][schema ID string][Avro payload]
	formatVersion byte = 0x00
)

// Serializer provides Avro serialization with Schema Registry integration
type Serializer struct {
	client     *schemaregistry.Client
	schemaName string
	schema     *schemaregistry.Schema
	codec      *goavro.Codec
	logger     *zap.Logger
	mu         sync.RWMutex
}

// SerializerConfig contains configuration for the Avro serializer
type SerializerConfig struct {
	// SchemaRegistryClient is the Schema Registry client
	SchemaRegistryClient *schemaregistry.Client
	// SchemaName is the name of the schema to use for serialization
	SchemaName string
	// SchemaContent is the Avro schema JSON (optional - if not provided, will be fetched from registry)
	SchemaContent string
	// AutoRegisterSchema enables automatic schema registration (default: false)
	AutoRegisterSchema bool
}

// NewSerializer creates a new Avro serializer
func NewSerializer(ctx context.Context, cfg SerializerConfig, logger *zap.Logger) (*Serializer, error) {
	if cfg.SchemaRegistryClient == nil {
		return nil, fmt.Errorf("SchemaRegistryClient is required")
	}
	if cfg.SchemaName == "" {
		return nil, fmt.Errorf("SchemaName is required")
	}

	s := &Serializer{
		client:     cfg.SchemaRegistryClient,
		schemaName: cfg.SchemaName,
		logger:     logger,
	}

	// If schema content is provided, register or get schema
	if cfg.SchemaContent != "" {
		var schema *schemaregistry.Schema
		var err error

		if cfg.AutoRegisterSchema {
			schema, err = cfg.SchemaRegistryClient.RegisterSchema(ctx, cfg.SchemaName, cfg.SchemaContent, schemaregistry.FormatAvro)
		} else {
			// Just create the codec locally, schema must already exist in registry
			schema, err = cfg.SchemaRegistryClient.GetLatestSchema(ctx, cfg.SchemaName)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get/register schema: %w", err)
		}

		codec, err := goavro.NewCodec(schema.Content)
		if err != nil {
			return nil, fmt.Errorf("failed to create Avro codec: %w", err)
		}

		s.schema = schema
		s.codec = codec
	}

	logger.Info("Avro serializer created",
		zap.String("schemaName", cfg.SchemaName),
		zap.Bool("autoRegister", cfg.AutoRegisterSchema),
	)

	return s, nil
}

// Serialize serializes data to Avro binary format with Schema Registry header.
// The data should be a map or struct that matches the Avro schema.
// Returns bytes in format: [format version][schema ID length (4 bytes)][schema ID][Avro payload]
func (s *Serializer) Serialize(ctx context.Context, data interface{}) ([]byte, error) {
	s.mu.RLock()
	schema := s.schema
	codec := s.codec
	s.mu.RUnlock()

	// If no schema is set, fetch it
	if schema == nil || codec == nil {
		s.mu.Lock()
		if s.schema == nil {
			var err error
			s.schema, err = s.client.GetLatestSchema(ctx, s.schemaName)
			if err != nil {
				s.mu.Unlock()
				return nil, fmt.Errorf("failed to get schema: %w", err)
			}
			s.codec, err = goavro.NewCodec(s.schema.Content)
			if err != nil {
				s.mu.Unlock()
				return nil, fmt.Errorf("failed to create Avro codec: %w", err)
			}
		}
		schema = s.schema
		codec = s.codec
		s.mu.Unlock()
	}

	// Encode data to Avro binary
	avroBytes, err := codec.BinaryFromNative(nil, data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode data to Avro: %w", err)
	}

	// Build message with Schema Registry header
	// Format: [format version (1 byte)][schema ID length (4 bytes big-endian)][schema ID][Avro payload]
	schemaIDBytes := []byte(schema.ID)
	schemaIDLen := len(schemaIDBytes)

	result := make([]byte, 1+4+schemaIDLen+len(avroBytes))
	result[0] = formatVersion
	binary.BigEndian.PutUint32(result[1:5], uint32(schemaIDLen))
	copy(result[5:5+schemaIDLen], schemaIDBytes)
	copy(result[5+schemaIDLen:], avroBytes)

	s.logger.Debug("Message serialized",
		zap.String("schemaName", s.schemaName),
		zap.String("schemaID", schema.ID),
		zap.Int("payloadSize", len(avroBytes)),
	)

	return result, nil
}

// SerializeJSON serializes JSON data to Avro binary format with Schema Registry header.
// The input should be a JSON string or []byte that matches the Avro schema.
func (s *Serializer) SerializeJSON(ctx context.Context, jsonData []byte) ([]byte, error) {
	s.mu.RLock()
	schema := s.schema
	codec := s.codec
	s.mu.RUnlock()

	// If no schema is set, fetch it
	if schema == nil || codec == nil {
		s.mu.Lock()
		if s.schema == nil {
			var err error
			s.schema, err = s.client.GetLatestSchema(ctx, s.schemaName)
			if err != nil {
				s.mu.Unlock()
				return nil, fmt.Errorf("failed to get schema: %w", err)
			}
			s.codec, err = goavro.NewCodec(s.schema.Content)
			if err != nil {
				s.mu.Unlock()
				return nil, fmt.Errorf("failed to create Avro codec: %w", err)
			}
		}
		schema = s.schema
		codec = s.codec
		s.mu.Unlock()
	}

	// Convert JSON to native Go types
	native, _, err := codec.NativeFromTextual(jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Encode to Avro binary
	avroBytes, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("failed to encode to Avro: %w", err)
	}

	// Build message with Schema Registry header
	schemaIDBytes := []byte(schema.ID)
	schemaIDLen := len(schemaIDBytes)

	result := make([]byte, 1+4+schemaIDLen+len(avroBytes))
	result[0] = formatVersion
	binary.BigEndian.PutUint32(result[1:5], uint32(schemaIDLen))
	copy(result[5:5+schemaIDLen], schemaIDBytes)
	copy(result[5+schemaIDLen:], avroBytes)

	s.logger.Debug("JSON message serialized to Avro",
		zap.String("schemaName", s.schemaName),
		zap.String("schemaID", schema.ID),
		zap.Int("payloadSize", len(avroBytes)),
	)

	return result, nil
}

// GetSchemaID returns the current schema ID
func (s *Serializer) GetSchemaID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.schema == nil {
		return ""
	}
	return s.schema.ID
}

// Deserializer provides Avro deserialization with Schema Registry integration
type Deserializer struct {
	client  *schemaregistry.Client
	codecs  map[string]*goavro.Codec
	schemas map[string]*schemaregistry.Schema
	logger  *zap.Logger
	mu      sync.RWMutex
}

// DeserializerConfig contains configuration for the Avro deserializer
type DeserializerConfig struct {
	// SchemaRegistryClient is the Schema Registry client
	SchemaRegistryClient *schemaregistry.Client
}

// NewDeserializer creates a new Avro deserializer
func NewDeserializer(cfg DeserializerConfig, logger *zap.Logger) (*Deserializer, error) {
	if cfg.SchemaRegistryClient == nil {
		return nil, fmt.Errorf("SchemaRegistryClient is required")
	}

	d := &Deserializer{
		client:  cfg.SchemaRegistryClient,
		codecs:  make(map[string]*goavro.Codec),
		schemas: make(map[string]*schemaregistry.Schema),
		logger:  logger,
	}

	logger.Info("Avro deserializer created")

	return d, nil
}

// DeserializeResult contains the result of deserialization
type DeserializeResult struct {
	// Data is the deserialized data as native Go types
	Data interface{}
	// SchemaID is the schema ID from the message
	SchemaID string
	// Schema is the schema used for deserialization
	Schema *schemaregistry.Schema
}

// Deserialize deserializes Avro binary data with Schema Registry header.
// Returns the deserialized data as native Go types (maps, slices, primitives).
func (d *Deserializer) Deserialize(ctx context.Context, data []byte) (*DeserializeResult, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("message too short: expected at least 5 bytes, got %d", len(data))
	}

	// Check format version
	if data[0] != formatVersion {
		return nil, fmt.Errorf("unsupported message format version: %d", data[0])
	}

	// Parse schema ID length
	schemaIDLen := binary.BigEndian.Uint32(data[1:5])
	if len(data) < int(5+schemaIDLen) {
		return nil, fmt.Errorf("message too short for schema ID: expected %d bytes, got %d", 5+schemaIDLen, len(data))
	}

	// Extract schema ID
	schemaID := string(data[5 : 5+schemaIDLen])

	// Get or create codec for this schema
	codec, schema, err := d.getCodec(ctx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get codec for schema %s: %w", schemaID, err)
	}

	// Decode Avro payload
	avroPayload := data[5+schemaIDLen:]
	native, _, err := codec.NativeFromBinary(avroPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro payload: %w", err)
	}

	d.logger.Debug("Message deserialized",
		zap.String("schemaID", schemaID),
		zap.Int("payloadSize", len(avroPayload)),
	)

	return &DeserializeResult{
		Data:     native,
		SchemaID: schemaID,
		Schema:   schema,
	}, nil
}

// DeserializeToJSON deserializes Avro binary data to JSON.
func (d *Deserializer) DeserializeToJSON(ctx context.Context, data []byte) ([]byte, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("message too short: expected at least 5 bytes, got %d", len(data))
	}

	// Check format version
	if data[0] != formatVersion {
		return nil, fmt.Errorf("unsupported message format version: %d", data[0])
	}

	// Parse schema ID length
	schemaIDLen := binary.BigEndian.Uint32(data[1:5])
	if len(data) < int(5+schemaIDLen) {
		return nil, fmt.Errorf("message too short for schema ID: expected %d bytes, got %d", 5+schemaIDLen, len(data))
	}

	// Extract schema ID
	schemaID := string(data[5 : 5+schemaIDLen])

	// Get or create codec for this schema
	codec, _, err := d.getCodec(ctx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get codec for schema %s: %w", schemaID, err)
	}

	// Decode Avro payload to JSON
	avroPayload := data[5+schemaIDLen:]
	native, _, err := codec.NativeFromBinary(avroPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro payload: %w", err)
	}

	jsonBytes, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to JSON: %w", err)
	}

	d.logger.Debug("Message deserialized to JSON",
		zap.String("schemaID", schemaID),
		zap.Int("jsonSize", len(jsonBytes)),
	)

	return jsonBytes, nil
}

// getCodec gets or creates a codec for the given schema ID
func (d *Deserializer) getCodec(ctx context.Context, schemaID string) (*goavro.Codec, *schemaregistry.Schema, error) {
	d.mu.RLock()
	codec, ok := d.codecs[schemaID]
	schema := d.schemas[schemaID]
	d.mu.RUnlock()

	if ok {
		return codec, schema, nil
	}

	// Fetch schema from registry
	d.mu.Lock()
	defer d.mu.Unlock()

	// Double-check after acquiring write lock
	if codec, ok = d.codecs[schemaID]; ok {
		return codec, d.schemas[schemaID], nil
	}

	schema, err := d.client.GetSchemaByID(ctx, schemaID)
	if err != nil {
		return nil, nil, err
	}

	codec, err = goavro.NewCodec(schema.Content)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create codec: %w", err)
	}

	d.codecs[schemaID] = codec
	d.schemas[schemaID] = schema

	return codec, schema, nil
}

// IsAvroMessage checks if the message is an Avro-encoded message with Schema Registry header
func IsAvroMessage(data []byte) bool {
	if len(data) < 5 {
		return false
	}
	return data[0] == formatVersion
}

// ExtractSchemaID extracts the schema ID from an Avro-encoded message
func ExtractSchemaID(data []byte) (string, error) {
	if len(data) < 5 {
		return "", fmt.Errorf("message too short: expected at least 5 bytes, got %d", len(data))
	}

	if data[0] != formatVersion {
		return "", fmt.Errorf("unsupported message format version: %d", data[0])
	}

	schemaIDLen := binary.BigEndian.Uint32(data[1:5])
	if len(data) < int(5+schemaIDLen) {
		return "", fmt.Errorf("message too short for schema ID")
	}

	return string(data[5 : 5+schemaIDLen]), nil
}
