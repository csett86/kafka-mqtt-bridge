// Package avro provides Avro serialization and deserialization with Azure Schema Registry integration.
// It supports encoding and decoding messages using Avro schemas stored in Azure Event Hubs Schema Registry.
package avro

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/csett86/kafka-mqtt-bridge/internal/schemaregistry"
	"github.com/hamba/avro/v2"
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
	avroSchema avro.Schema
	logger     *zap.Logger
	mu         sync.RWMutex
}

// SerializerConfig contains configuration for the Avro serializer
type SerializerConfig struct {
	// SchemaRegistryClient is the Schema Registry client
	SchemaRegistryClient *schemaregistry.Client
	// SchemaName is the name of the schema to use for serialization
	SchemaName string
}

// NewSerializer creates a new Avro serializer
func NewSerializer(ctx context.Context, cfg SerializerConfig, logger *zap.Logger) (*Serializer, error) {
	if cfg.SchemaRegistryClient == nil {
		return nil, fmt.Errorf("SchemaRegistryClient is required")
	}
	if cfg.SchemaName == "" {
		return nil, fmt.Errorf("SchemaName is required")
	}

	// Fetch the schema during initialization
	schema, err := cfg.SchemaRegistryClient.GetLatestSchema(ctx, cfg.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Parse the Avro schema
	avroSchema, err := avro.Parse(schema.Content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Avro schema: %w", err)
	}

	s := &Serializer{
		client:     cfg.SchemaRegistryClient,
		schemaName: cfg.SchemaName,
		schema:     schema,
		avroSchema: avroSchema,
		logger:     logger,
	}

	logger.Info("Avro serializer created",
		zap.String("schemaName", cfg.SchemaName),
		zap.String("schemaID", schema.ID),
		zap.Int("version", schema.Version),
	)

	return s, nil
}

// Serialize serializes data to Avro binary format with Schema Registry header.
// The data should be a map or struct that matches the Avro schema.
// Returns bytes in format: [format version][schema ID length (4 bytes)][schema ID][Avro payload]
func (s *Serializer) Serialize(ctx context.Context, data interface{}) ([]byte, error) {
	s.mu.RLock()
	schema := s.schema
	avroSchema := s.avroSchema
	s.mu.RUnlock()

	// Encode data to Avro binary
	avroBytes, err := avro.Marshal(avroSchema, data)
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
	avroSchema := s.avroSchema
	s.mu.RUnlock()

	// Parse JSON to a generic map
	var data interface{}
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Encode to Avro binary
	avroBytes, err := avro.Marshal(avroSchema, data)
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
	return s.schema.ID
}

// Deserializer provides Avro deserialization with Schema Registry integration
type Deserializer struct {
	client     *schemaregistry.Client
	schemas    map[string]avro.Schema
	regSchemas map[string]*schemaregistry.Schema
	logger     *zap.Logger
	mu         sync.RWMutex
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
		client:     cfg.SchemaRegistryClient,
		schemas:    make(map[string]avro.Schema),
		regSchemas: make(map[string]*schemaregistry.Schema),
		logger:     logger,
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

	// Get or create schema for this schema ID
	avroSchema, regSchema, err := d.getSchema(ctx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for schema %s: %w", schemaID, err)
	}

	// Decode Avro payload
	avroPayload := data[5+schemaIDLen:]
	var native interface{}
	if err := avro.Unmarshal(avroSchema, avroPayload, &native); err != nil {
		return nil, fmt.Errorf("failed to decode Avro payload: %w", err)
	}

	d.logger.Debug("Message deserialized",
		zap.String("schemaID", schemaID),
		zap.Int("payloadSize", len(avroPayload)),
	)

	return &DeserializeResult{
		Data:     native,
		SchemaID: schemaID,
		Schema:   regSchema,
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

	// Get or create schema for this schema ID
	avroSchema, _, err := d.getSchema(ctx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for schema %s: %w", schemaID, err)
	}

	// Decode Avro payload to native Go types
	avroPayload := data[5+schemaIDLen:]
	var native interface{}
	if err := avro.Unmarshal(avroSchema, avroPayload, &native); err != nil {
		return nil, fmt.Errorf("failed to decode Avro payload: %w", err)
	}

	// Convert to JSON
	jsonBytes, err := json.Marshal(native)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to JSON: %w", err)
	}

	d.logger.Debug("Message deserialized to JSON",
		zap.String("schemaID", schemaID),
		zap.Int("jsonSize", len(jsonBytes)),
	)

	return jsonBytes, nil
}

// getSchema gets or creates a schema for the given schema ID
func (d *Deserializer) getSchema(ctx context.Context, schemaID string) (avro.Schema, *schemaregistry.Schema, error) {
	d.mu.RLock()
	avroSchema, ok := d.schemas[schemaID]
	regSchema := d.regSchemas[schemaID]
	d.mu.RUnlock()

	if ok {
		return avroSchema, regSchema, nil
	}

	// Fetch schema from registry
	d.mu.Lock()
	defer d.mu.Unlock()

	// Double-check after acquiring write lock
	if avroSchema, ok = d.schemas[schemaID]; ok {
		return avroSchema, d.regSchemas[schemaID], nil
	}

	regSchema, err := d.client.GetSchemaByID(ctx, schemaID)
	if err != nil {
		return nil, nil, err
	}

	avroSchema, err = avro.Parse(regSchema.Content)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse schema: %w", err)
	}

	d.schemas[schemaID] = avroSchema
	d.regSchemas[schemaID] = regSchema

	return avroSchema, regSchema, nil
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
