// Package schemaregistry provides a client for Azure Event Hubs Schema Registry.
// It supports retrieving and caching Avro schemas.
package schemaregistry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"go.uber.org/zap"
)

const (
	// Azure Schema Registry API version
	apiVersion = "2022-10"

	// Schema formats supported by Azure Schema Registry
	FormatAvro = "Avro"

	// Default timeout for API requests
	defaultTimeout = 30 * time.Second

	// Schema Registry scope for Azure authentication
	schemaRegistryScope = "https://eventhubs.azure.net/.default"
)

// Schema represents a schema stored in the registry
type Schema struct {
	// ID is the unique identifier of the schema in the registry
	ID string
	// Content is the schema definition (e.g., Avro JSON schema)
	Content string
	// Format is the schema format (e.g., "Avro")
	Format string
	// GroupName is the schema group name
	GroupName string
	// SchemaName is the name of the schema
	SchemaName string
	// Version is the schema version
	Version int
}

// ClientConfig contains configuration for the Schema Registry client
type ClientConfig struct {
	// FullyQualifiedNamespace is the fully qualified namespace of the Schema Registry
	// e.g., "<namespace>.servicebus.windows.net"
	FullyQualifiedNamespace string
	// GroupName is the schema group name
	GroupName string
	// Credential is the Azure credential used for authentication
	// If nil, DefaultAzureCredential will be used
	Credential azcore.TokenCredential
	// CacheEnabled enables schema caching (default: true)
	CacheEnabled bool
	// RequestTimeout is the timeout for individual API requests (default: 30s)
	RequestTimeout time.Duration
}

// Client is a client for Azure Event Hubs Schema Registry
type Client struct {
	namespace      string
	groupName      string
	credential     azcore.TokenCredential
	httpClient     *http.Client
	logger         *zap.Logger
	requestTimeout time.Duration

	// Schema caching
	cacheEnabled bool
	schemaByID   map[string]*Schema
	cacheMu      sync.RWMutex
}

// NewClient creates a new Schema Registry client
func NewClient(cfg ClientConfig, logger *zap.Logger) (*Client, error) {
	if cfg.FullyQualifiedNamespace == "" {
		return nil, fmt.Errorf("FullyQualifiedNamespace is required")
	}
	if cfg.GroupName == "" {
		return nil, fmt.Errorf("GroupName is required")
	}

	credential := cfg.Credential
	if credential == nil {
		var err error
		credential, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create default Azure credential: %w", err)
		}
	}

	timeout := cfg.RequestTimeout
	if timeout == 0 {
		timeout = defaultTimeout
	}

	client := &Client{
		namespace:      cfg.FullyQualifiedNamespace,
		groupName:      cfg.GroupName,
		credential:     credential,
		httpClient:     &http.Client{Timeout: timeout},
		logger:         logger,
		requestTimeout: timeout,
		cacheEnabled:   cfg.CacheEnabled,
		schemaByID:     make(map[string]*Schema),
	}

	logger.Info("Schema Registry client created",
		zap.String("namespace", cfg.FullyQualifiedNamespace),
		zap.String("groupName", cfg.GroupName),
		zap.Bool("cacheEnabled", cfg.CacheEnabled),
	)

	return client, nil
}

// getAccessToken gets an access token for the Schema Registry API
func (c *Client) getAccessToken(ctx context.Context) (string, error) {
	token, err := c.credential.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{schemaRegistryScope},
	})
	if err != nil {
		return "", fmt.Errorf("failed to get access token: %w", err)
	}
	return token.Token, nil
}

// GetSchemaByID retrieves a schema by its ID
func (c *Client) GetSchemaByID(ctx context.Context, schemaID string) (*Schema, error) {
	// Check cache first
	if c.cacheEnabled {
		c.cacheMu.RLock()
		if schema, ok := c.schemaByID[schemaID]; ok {
			c.cacheMu.RUnlock()
			c.logger.Debug("Schema found in cache by ID", zap.String("id", schemaID))
			return schema, nil
		}
		c.cacheMu.RUnlock()
	}

	// GET /$schemagroups/$schemas/{schemaId}?api-version=2022-10
	url := fmt.Sprintf("https://%s/$schemagroups/$schemas/%s?api-version=%s",
		c.namespace, schemaID, apiVersion)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	token, err := c.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get schema: status %d, body: %s", resp.StatusCode, string(body))
	}

	// Parse response headers for metadata
	format := resp.Header.Get("Schema-Format")
	if format == "" {
		// Try to parse from Content-Type header
		contentType := resp.Header.Get("Content-Type")
		if strings.Contains(contentType, "Avro") {
			format = FormatAvro
		}
	}

	groupName := resp.Header.Get("Schema-Group-Name")
	schemaName := resp.Header.Get("Schema-Name")
	versionStr := resp.Header.Get("Schema-Version")
	version := 0
	if versionStr != "" {
		if v, err := strconv.Atoi(versionStr); err == nil {
			version = v
		}
	}

	schema := &Schema{
		ID:         schemaID,
		Content:    string(body),
		Format:     format,
		GroupName:  groupName,
		SchemaName: schemaName,
		Version:    version,
	}

	// Cache the schema
	if c.cacheEnabled {
		c.cacheMu.Lock()
		c.schemaByID[schemaID] = schema
		c.cacheMu.Unlock()
	}

	c.logger.Debug("Schema retrieved",
		zap.String("id", schemaID),
		zap.String("schemaName", schemaName),
		zap.Int("version", version),
	)

	return schema, nil
}

// GetSchemaByNameAndVersion retrieves a schema by its name and version
func (c *Client) GetSchemaByNameAndVersion(ctx context.Context, schemaName string, version int) (*Schema, error) {
	// GET /$schemagroups/{groupName}/schemas/{schemaName}/versions/{versionNumber}?api-version=2022-10
	url := fmt.Sprintf("https://%s/$schemagroups/%s/schemas/%s/versions/%d?api-version=%s",
		c.namespace, c.groupName, schemaName, version, apiVersion)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	token, err := c.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get schema: status %d, body: %s", resp.StatusCode, string(body))
	}

	// Parse schema ID from response headers
	schemaID := resp.Header.Get("Schema-Id")

	schema := &Schema{
		ID:         schemaID,
		Content:    string(body),
		Format:     FormatAvro,
		GroupName:  c.groupName,
		SchemaName: schemaName,
		Version:    version,
	}

	// Cache the schema
	if c.cacheEnabled {
		c.cacheMu.Lock()
		c.schemaByID[schemaID] = schema
		c.cacheMu.Unlock()
	}

	c.logger.Debug("Schema retrieved by name and version",
		zap.String("schemaName", schemaName),
		zap.Int("version", version),
		zap.String("id", schemaID),
	)

	return schema, nil
}

// GetLatestSchema retrieves the latest version of a schema by its name
func (c *Client) GetLatestSchema(ctx context.Context, schemaName string) (*Schema, error) {
	// GET /$schemagroups/{groupName}/schemas/{schemaName}?api-version=2022-10
	url := fmt.Sprintf("https://%s/$schemagroups/%s/schemas/%s?api-version=%s",
		c.namespace, c.groupName, schemaName, apiVersion)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	token, err := c.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get schema: status %d, body: %s", resp.StatusCode, string(body))
	}

	// Parse schema ID and version from response headers
	schemaID := resp.Header.Get("Schema-Id")
	versionStr := resp.Header.Get("Schema-Version")
	version := 0
	if versionStr != "" {
		if v, err := strconv.Atoi(versionStr); err == nil {
			version = v
		}
	}

	schema := &Schema{
		ID:         schemaID,
		Content:    string(body),
		Format:     FormatAvro,
		GroupName:  c.groupName,
		SchemaName: schemaName,
		Version:    version,
	}

	// Cache the schema
	if c.cacheEnabled {
		c.cacheMu.Lock()
		c.schemaByID[schemaID] = schema
		c.cacheMu.Unlock()
	}

	c.logger.Debug("Latest schema retrieved",
		zap.String("schemaName", schemaName),
		zap.Int("version", version),
		zap.String("id", schemaID),
	)

	return schema, nil
}

// ListSchemaVersions lists all versions of a schema
func (c *Client) ListSchemaVersions(ctx context.Context, schemaName string) ([]int, error) {
	// GET /$schemagroups/{groupName}/schemas/{schemaName}/versions?api-version=2022-10
	url := fmt.Sprintf("https://%s/$schemagroups/%s/schemas/%s/versions?api-version=%s",
		c.namespace, c.groupName, schemaName, apiVersion)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	token, err := c.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to list schema versions: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to list schema versions: status %d, body: %s", resp.StatusCode, string(body))
	}

	var versions struct {
		Versions []int `json:"schemaVersions"`
	}
	if err := json.Unmarshal(body, &versions); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return versions.Versions, nil
}

// ClearCache clears the schema cache
func (c *Client) ClearCache() {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.schemaByID = make(map[string]*Schema)
	c.logger.Debug("Schema cache cleared")
}
