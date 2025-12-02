package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigWithSchemaRegistry(t *testing.T) {
	// Create a temporary config file with schema registry settings
	configContent := `
kafka:
  broker: "localhost:9092"
  group_id: "test-group"

mqtt:
  broker: "localhost"
  port: 1883
  client_id: "test-client"

bridge:
  name: "test-bridge"
  log_level: "info"
  buffer_size: 100

schema_registry:
  enabled: true
  fully_qualified_namespace: "test.servicebus.windows.net"
  group_name: "test-group"
  schema_name: "test-schema"
  auto_register_schema: true
  cache_enabled: true
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify schema registry settings
	if !cfg.SchemaRegistry.Enabled {
		t.Error("SchemaRegistry.Enabled = false, want true")
	}
	if cfg.SchemaRegistry.FullyQualifiedNamespace != "test.servicebus.windows.net" {
		t.Errorf("SchemaRegistry.FullyQualifiedNamespace = %q, want %q",
			cfg.SchemaRegistry.FullyQualifiedNamespace, "test.servicebus.windows.net")
	}
	if cfg.SchemaRegistry.GroupName != "test-group" {
		t.Errorf("SchemaRegistry.GroupName = %q, want %q",
			cfg.SchemaRegistry.GroupName, "test-group")
	}
	if cfg.SchemaRegistry.SchemaName != "test-schema" {
		t.Errorf("SchemaRegistry.SchemaName = %q, want %q",
			cfg.SchemaRegistry.SchemaName, "test-schema")
	}
	if !cfg.SchemaRegistry.AutoRegisterSchema {
		t.Error("SchemaRegistry.AutoRegisterSchema = false, want true")
	}
	if cfg.SchemaRegistry.CacheEnabled == nil || !*cfg.SchemaRegistry.CacheEnabled {
		t.Error("SchemaRegistry.CacheEnabled should be true")
	}
}

func TestLoadConfigSchemaRegistryDefaults(t *testing.T) {
	// Create a minimal config file without schema registry settings
	configContent := `
kafka:
  broker: "localhost:9092"

mqtt:
  broker: "localhost"
  port: 1883

bridge:
  name: "test-bridge"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify schema registry defaults
	if cfg.SchemaRegistry.Enabled {
		t.Error("SchemaRegistry.Enabled should be false by default")
	}
	if cfg.SchemaRegistry.CacheEnabled == nil {
		t.Error("SchemaRegistry.CacheEnabled should have default value")
	} else if !*cfg.SchemaRegistry.CacheEnabled {
		t.Error("SchemaRegistry.CacheEnabled should default to true")
	}
}

func TestLoadConfigSchemaRegistryWithCredentials(t *testing.T) {
	// Create a config file with Azure credentials
	configContent := `
kafka:
  broker: "localhost:9092"

mqtt:
  broker: "localhost"

bridge:
  name: "test-bridge"

schema_registry:
  enabled: true
  fully_qualified_namespace: "test.servicebus.windows.net"
  group_name: "test-group"
  schema_name: "test-schema"
  tenant_id: "test-tenant-id"
  client_id: "test-client-id"
  client_secret: "test-client-secret"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify Azure credentials
	if cfg.SchemaRegistry.TenantID != "test-tenant-id" {
		t.Errorf("SchemaRegistry.TenantID = %q, want %q",
			cfg.SchemaRegistry.TenantID, "test-tenant-id")
	}
	if cfg.SchemaRegistry.ClientID != "test-client-id" {
		t.Errorf("SchemaRegistry.ClientID = %q, want %q",
			cfg.SchemaRegistry.ClientID, "test-client-id")
	}
	if cfg.SchemaRegistry.ClientSecret != "test-client-secret" {
		t.Errorf("SchemaRegistry.ClientSecret = %q, want %q",
			cfg.SchemaRegistry.ClientSecret, "test-client-secret")
	}
}

func TestLoadConfigSchemaRegistryWithSchemaContent(t *testing.T) {
	// Create a config file with inline schema content
	schemaContent := `{"type":"record","name":"Test","fields":[{"name":"id","type":"string"}]}`
	configContent := `
kafka:
  broker: "localhost:9092"

mqtt:
  broker: "localhost"

bridge:
  name: "test-bridge"

schema_registry:
  enabled: true
  fully_qualified_namespace: "test.servicebus.windows.net"
  group_name: "test-group"
  schema_name: "test-schema"
  schema_content: '` + schemaContent + `'
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify schema content
	if cfg.SchemaRegistry.SchemaContent != schemaContent {
		t.Errorf("SchemaRegistry.SchemaContent = %q, want %q",
			cfg.SchemaRegistry.SchemaContent, schemaContent)
	}
}
