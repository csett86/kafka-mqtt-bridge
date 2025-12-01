// Package topicmapper provides topic mapping with wildcard support for the Kafka-MQTT bridge.
// It supports MQTT-style wildcards (+ for single level, # for multi-level) and dynamic
// target topic generation using captured segments.
package topicmapper

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/csett86/kafka-mqtt-bridge/pkg/config"
)

// Mapper handles topic pattern matching and transformation
type Mapper struct {
	mappings []compiledMapping
}

// compiledMapping holds a pre-compiled mapping for efficient matching
type compiledMapping struct {
	sourcePattern  *regexp.Regexp
	targetTemplate string
	sourceLiteral  string // Original source pattern for MQTT subscriptions
}

// New creates a new Mapper from topic mappings
func New(mappings []config.TopicMapping) (*Mapper, error) {
	compiled := make([]compiledMapping, 0, len(mappings))
	for _, m := range mappings {
		pattern, err := patternToRegex(m.Source)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, compiledMapping{
			sourcePattern:  pattern,
			targetTemplate: m.Target,
			sourceLiteral:  m.Source,
		})
	}
	return &Mapper{mappings: compiled}, nil
}

// Map takes a source topic and returns the corresponding target topic.
// Returns empty string if no mapping matches.
func (m *Mapper) Map(sourceTopic string) string {
	for _, mapping := range m.mappings {
		if matches := mapping.sourcePattern.FindStringSubmatch(sourceTopic); matches != nil {
			return applyTemplate(mapping.targetTemplate, matches[1:])
		}
	}
	return ""
}

// GetSubscriptionPatterns returns the source patterns for MQTT subscriptions.
// These patterns use MQTT wildcard syntax.
func (m *Mapper) GetSubscriptionPatterns() []string {
	patterns := make([]string, len(m.mappings))
	for i, mapping := range m.mappings {
		patterns[i] = mapping.sourceLiteral
	}
	return patterns
}

// HasMappings returns true if there are any topic mappings configured
func (m *Mapper) HasMappings() bool {
	return len(m.mappings) > 0
}

// patternToRegex converts an MQTT-style wildcard pattern to a regex.
// Supports:
//   - + : matches exactly one topic level (no slashes)
//   - # : matches zero or more topic levels (including slashes), must be at end
//   - * : alternative single-level wildcard (same as +)
func patternToRegex(pattern string) (*regexp.Regexp, error) {
	// Replace wildcards with unique placeholders before escaping
	// This prevents QuoteMeta from affecting them
	const (
		plusPlaceholder = "\x00PLUS\x00"
		hashPlaceholder = "\x00HASH\x00"
		starPlaceholder = "\x00STAR\x00"
	)

	pattern = strings.ReplaceAll(pattern, "+", plusPlaceholder)
	pattern = strings.ReplaceAll(pattern, "#", hashPlaceholder)
	pattern = strings.ReplaceAll(pattern, "*", starPlaceholder)

	// Escape regex special characters
	escaped := regexp.QuoteMeta(pattern)

	// Replace placeholders with regex patterns
	// + -> single level capture group (no slashes)
	escaped = strings.ReplaceAll(escaped, plusPlaceholder, `([^/]+)`)
	// * -> single level capture group (alternative syntax)
	escaped = strings.ReplaceAll(escaped, starPlaceholder, `([^/]+)`)
	// # -> multi-level capture group (including slashes)
	escaped = strings.ReplaceAll(escaped, hashPlaceholder, `(.*)`)

	escaped = "^" + escaped + "$"

	return regexp.Compile(escaped)
}

// applyTemplate replaces {1}, {2}, etc. in the template with captured segments.
func applyTemplate(template string, captures []string) string {
	result := template
	for i, capture := range captures {
		placeholder := "{" + strconv.Itoa(i+1) + "}"
		result = strings.ReplaceAll(result, placeholder, capture)
	}
	return result
}
