// Package avro provides Avro serialization and deserialization support for Kafka messages.
// It integrates with Confluent Schema Registry to fetch and cache schemas.
package avro

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"go.uber.org/zap"
)

const (
	// MagicByte is the Confluent wire format magic byte
	// See: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
	MagicByte = 0x00
	// HeaderSize is the size of the Confluent wire format header (1 magic byte + 4 bytes schema ID)
	HeaderSize = 5
)

// Config contains Avro codec configuration
type Config struct {
	// SchemaRegistryURL is the URL of the Confluent Schema Registry
	SchemaRegistryURL string
	// SchemaRegistryUsername for basic auth (optional)
	SchemaRegistryUsername string
	// SchemaRegistryPassword for basic auth (optional)
	SchemaRegistryPassword string
	// SubjectNameStrategy determines how subject names are derived
	// Options: "topic" (default), "record", "topic_record"
	SubjectNameStrategy string
}

// Codec handles Avro serialization and deserialization with Schema Registry integration
type Codec struct {
	client       *srclient.SchemaRegistryClient
	logger       *zap.Logger
	codecCache   map[int]*goavro.Codec // Cache of codecs by schema ID
	codecMu      sync.RWMutex
	schemaCache  map[string]int // Cache of schema IDs by subject
	schemaMu     sync.RWMutex
	subjectStrat string
}

// NewCodec creates a new Avro codec with Schema Registry integration
func NewCodec(cfg Config, logger *zap.Logger) (*Codec, error) {
	if cfg.SchemaRegistryURL == "" {
		return nil, fmt.Errorf("schema registry URL is required")
	}

	client := srclient.CreateSchemaRegistryClient(cfg.SchemaRegistryURL)
	if cfg.SchemaRegistryUsername != "" && cfg.SchemaRegistryPassword != "" {
		client.SetCredentials(cfg.SchemaRegistryUsername, cfg.SchemaRegistryPassword)
	}

	subjectStrat := cfg.SubjectNameStrategy
	if subjectStrat == "" {
		subjectStrat = "topic"
	}

	logger.Info("Avro codec initialized",
		zap.String("schemaRegistryURL", cfg.SchemaRegistryURL),
		zap.String("subjectNameStrategy", subjectStrat),
	)

	return &Codec{
		client:       client,
		logger:       logger,
		codecCache:   make(map[int]*goavro.Codec),
		schemaCache:  make(map[string]int),
		subjectStrat: subjectStrat,
	}, nil
}

// getSubjectName returns the subject name based on the configured strategy
func (c *Codec) getSubjectName(topic string, isKey bool, recordName string) string {
	suffix := "-value"
	if isKey {
		suffix = "-key"
	}

	switch c.subjectStrat {
	case "record":
		if recordName != "" {
			return recordName + suffix
		}
		return topic + suffix
	case "topic_record":
		if recordName != "" {
			return topic + "-" + recordName + suffix
		}
		return topic + suffix
	default: // "topic" strategy
		return topic + suffix
	}
}

// getCodecByID retrieves or creates a codec for the given schema ID
func (c *Codec) getCodecByID(schemaID int) (*goavro.Codec, error) {
	// Check cache first
	c.codecMu.RLock()
	codec, exists := c.codecCache[schemaID]
	c.codecMu.RUnlock()
	if exists {
		return codec, nil
	}

	// Fetch schema from registry
	schema, err := c.client.GetSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema ID %d: %w", schemaID, err)
	}

	// Create codec
	codec, err = goavro.NewCodec(schema.Schema())
	if err != nil {
		return nil, fmt.Errorf("failed to create codec for schema ID %d: %w", schemaID, err)
	}

	// Cache the codec
	c.codecMu.Lock()
	c.codecCache[schemaID] = codec
	c.codecMu.Unlock()

	c.logger.Debug("Cached Avro codec",
		zap.Int("schemaID", schemaID),
	)

	return codec, nil
}

// getSchemaID retrieves or looks up the schema ID for a topic
func (c *Codec) getSchemaID(topic string, isKey bool) (int, *goavro.Codec, error) {
	subject := c.getSubjectName(topic, isKey, "")

	// Check cache first
	c.schemaMu.RLock()
	schemaID, exists := c.schemaCache[subject]
	c.schemaMu.RUnlock()

	if exists {
		codec, err := c.getCodecByID(schemaID)
		if err != nil {
			return 0, nil, err
		}
		return schemaID, codec, nil
	}

	// Fetch latest schema from registry
	schema, err := c.client.GetLatestSchema(subject)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get latest schema for subject %q: %w", subject, err)
	}

	schemaID = schema.ID()

	// Create codec
	codec, err := goavro.NewCodec(schema.Schema())
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create codec for subject %q: %w", subject, err)
	}

	// Cache the schema ID and codec
	c.schemaMu.Lock()
	c.schemaCache[subject] = schemaID
	c.schemaMu.Unlock()

	c.codecMu.Lock()
	c.codecCache[schemaID] = codec
	c.codecMu.Unlock()

	c.logger.Debug("Cached schema and codec",
		zap.String("subject", subject),
		zap.Int("schemaID", schemaID),
	)

	return schemaID, codec, nil
}

// Serialize encodes a message using Avro and adds the Confluent wire format header.
// The input should be JSON-encoded data that matches the Avro schema.
func (c *Codec) Serialize(topic string, isKey bool, data []byte) ([]byte, error) {
	schemaID, codec, err := c.getSchemaID(topic, isKey)
	if err != nil {
		return nil, err
	}

	// Parse JSON input to native Go type
	var native interface{}
	if err := json.Unmarshal(data, &native); err != nil {
		return nil, fmt.Errorf("failed to parse JSON input: %w", err)
	}

	// Convert JSON to Avro native format (handles type conversions)
	native, err = convertJSONToAvro(native, codec)
	if err != nil {
		return nil, fmt.Errorf("failed to convert JSON to Avro format: %w", err)
	}

	// Encode to Avro binary
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("failed to encode Avro: %w", err)
	}

	// Add Confluent wire format header
	result := make([]byte, HeaderSize+len(binary))
	result[0] = MagicByte
	putSchemaID(result[1:HeaderSize], schemaID)
	copy(result[HeaderSize:], binary)

	return result, nil
}

// Deserialize decodes an Avro message with Confluent wire format header.
// Returns JSON-encoded data.
func (c *Codec) Deserialize(data []byte) ([]byte, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("message too short: expected at least %d bytes, got %d", HeaderSize, len(data))
	}

	// Check magic byte
	if data[0] != MagicByte {
		return nil, fmt.Errorf("invalid magic byte: expected %#x, got %#x", MagicByte, data[0])
	}

	// Extract schema ID
	schemaID := getSchemaID(data[1:HeaderSize])

	// Get codec for schema
	codec, err := c.getCodecByID(schemaID)
	if err != nil {
		return nil, err
	}

	// Decode Avro binary to native
	native, _, err := codec.NativeFromBinary(data[HeaderSize:])
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro: %w", err)
	}

	// Convert to JSON
	jsonData, err := json.Marshal(native)
	if err != nil {
		return nil, fmt.Errorf("failed to encode JSON: %w", err)
	}

	return jsonData, nil
}

// DeserializeWithSchemaID decodes an Avro message and returns both the data and schema ID
func (c *Codec) DeserializeWithSchemaID(data []byte) ([]byte, int, error) {
	if len(data) < HeaderSize {
		return nil, 0, fmt.Errorf("message too short: expected at least %d bytes, got %d", HeaderSize, len(data))
	}

	// Check magic byte
	if data[0] != MagicByte {
		return nil, 0, fmt.Errorf("invalid magic byte: expected %#x, got %#x", MagicByte, data[0])
	}

	// Extract schema ID
	schemaID := getSchemaID(data[1:HeaderSize])

	// Get codec for schema
	codec, err := c.getCodecByID(schemaID)
	if err != nil {
		return nil, 0, err
	}

	// Decode Avro binary to native
	native, _, err := codec.NativeFromBinary(data[HeaderSize:])
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode Avro: %w", err)
	}

	// Convert to JSON
	jsonData, err := json.Marshal(native)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to encode JSON: %w", err)
	}

	return jsonData, schemaID, nil
}

// IsAvroMessage checks if a message has the Confluent Avro wire format header
func IsAvroMessage(data []byte) bool {
	if len(data) < HeaderSize {
		return false
	}
	return data[0] == MagicByte
}

// putSchemaID encodes a schema ID as big-endian into 4 bytes
func putSchemaID(b []byte, id int) {
	binary.BigEndian.PutUint32(b, uint32(id))
}

// getSchemaID decodes a schema ID from 4 big-endian bytes
func getSchemaID(b []byte) int {
	return int(binary.BigEndian.Uint32(b))
}

// convertJSONToAvro handles type conversions between JSON and Avro native formats
// This is needed because JSON doesn't distinguish between int, long, float, double, etc.
func convertJSONToAvro(native interface{}, codec *goavro.Codec) (interface{}, error) {
	// goavro's TextualFromNative and then NativeFromTextual handles type conversion better
	// but for direct BinaryFromNative, we let goavro handle the conversion internally
	// as it's more lenient with numeric types
	return native, nil
}
