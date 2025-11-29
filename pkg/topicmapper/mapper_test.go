package topicmapper

import (
	"testing"

	"github.com/csett86/kafka-mqtt-bridge/pkg/config"
)

func TestMapper_Map(t *testing.T) {
	tests := []struct {
		name        string
		mappings    []config.TopicMapping
		sourceTopic string
		want        string
	}{
		{
			name: "literal match",
			mappings: []config.TopicMapping{
				{Source: "sensors/temp", Target: "kafka/temp"},
			},
			sourceTopic: "sensors/temp",
			want:        "kafka/temp",
		},
		{
			name: "single level wildcard +",
			mappings: []config.TopicMapping{
				{Source: "sensors/+/data", Target: "kafka/sensors/{1}"},
			},
			sourceTopic: "sensors/temp/data",
			want:        "kafka/sensors/temp",
		},
		{
			name: "single level wildcard *",
			mappings: []config.TopicMapping{
				{Source: "sensors/*/data", Target: "kafka/sensors/{1}"},
			},
			sourceTopic: "sensors/humidity/data",
			want:        "kafka/sensors/humidity",
		},
		{
			name: "multi-level wildcard #",
			mappings: []config.TopicMapping{
				{Source: "devices/#", Target: "kafka/{1}"},
			},
			sourceTopic: "devices/floor1/room2/sensor",
			want:        "kafka/floor1/room2/sensor",
		},
		{
			name: "multiple wildcards",
			mappings: []config.TopicMapping{
				{Source: "sensors/+/+/data", Target: "kafka/{1}/{2}"},
			},
			sourceTopic: "sensors/building1/floor2/data",
			want:        "kafka/building1/floor2",
		},
		{
			name: "no match",
			mappings: []config.TopicMapping{
				{Source: "sensors/+/data", Target: "kafka/{1}"},
			},
			sourceTopic: "actuators/temp/data",
			want:        "",
		},
		{
			name: "first matching rule wins",
			mappings: []config.TopicMapping{
				{Source: "sensors/temp", Target: "exact-match"},
				{Source: "sensors/+", Target: "wildcard-match"},
			},
			sourceTopic: "sensors/temp",
			want:        "exact-match",
		},
		{
			name: "empty multi-level wildcard",
			mappings: []config.TopicMapping{
				{Source: "sensors/#", Target: "kafka/{1}"},
			},
			sourceTopic: "sensors/",
			want:        "kafka/",
		},
		{
			name: "mixed wildcards",
			mappings: []config.TopicMapping{
				{Source: "home/+/room/#", Target: "kafka/{1}/{2}"},
			},
			sourceTopic: "home/floor1/room/light/brightness",
			want:        "kafka/floor1/light/brightness",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper, err := New(tt.mappings)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			got := mapper.Map(tt.sourceTopic)
			if got != tt.want {
				t.Errorf("Map() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMapper_GetSubscriptionPatterns(t *testing.T) {
	mappings := []config.TopicMapping{
		{Source: "sensors/+/data", Target: "kafka/{1}"},
		{Source: "devices/#", Target: "kafka/{1}"},
	}

	mapper, err := New(mappings)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	patterns := mapper.GetSubscriptionPatterns()
	if len(patterns) != 2 {
		t.Fatalf("GetSubscriptionPatterns() returned %d patterns, want 2", len(patterns))
	}
	if patterns[0] != "sensors/+/data" {
		t.Errorf("patterns[0] = %q, want %q", patterns[0], "sensors/+/data")
	}
	if patterns[1] != "devices/#" {
		t.Errorf("patterns[1] = %q, want %q", patterns[1], "devices/#")
	}
}

func TestMapper_HasMappings(t *testing.T) {
	tests := []struct {
		name     string
		mappings []config.TopicMapping
		want     bool
	}{
		{
			name:     "no mappings",
			mappings: nil,
			want:     false,
		},
		{
			name:     "empty mappings",
			mappings: []config.TopicMapping{},
			want:     false,
		},
		{
			name: "has mappings",
			mappings: []config.TopicMapping{
				{Source: "test", Target: "target"},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper, err := New(tt.mappings)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if got := mapper.HasMappings(); got != tt.want {
				t.Errorf("HasMappings() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPatternToRegex(t *testing.T) {
	tests := []struct {
		pattern string
		matches []struct {
			topic   string
			matched bool
		}
	}{
		{
			pattern: "sensors/temp",
			matches: []struct {
				topic   string
				matched bool
			}{
				{"sensors/temp", true},
				{"sensors/humidity", false},
				{"sensors/temp/extra", false},
			},
		},
		{
			pattern: "sensors/+",
			matches: []struct {
				topic   string
				matched bool
			}{
				{"sensors/temp", true},
				{"sensors/humidity", true},
				{"sensors/temp/extra", false},
				{"actuators/temp", false},
			},
		},
		{
			pattern: "sensors/#",
			matches: []struct {
				topic   string
				matched bool
			}{
				{"sensors/temp", true},
				{"sensors/temp/extra", true},
				{"sensors/a/b/c/d", true},
				{"sensors/", true},
				{"actuators/temp", false},
			},
		},
		{
			pattern: "+/+/data",
			matches: []struct {
				topic   string
				matched bool
			}{
				{"sensors/temp/data", true},
				{"actuators/motor/data", true},
				{"sensors/data", false},
				{"sensors/temp/data/extra", false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			re, err := patternToRegex(tt.pattern)
			if err != nil {
				t.Fatalf("patternToRegex() error = %v", err)
			}
			for _, m := range tt.matches {
				matched := re.MatchString(m.topic)
				if matched != m.matched {
					t.Errorf("pattern %q matching %q: got %v, want %v", tt.pattern, m.topic, matched, m.matched)
				}
			}
		})
	}
}

func TestApplyTemplate(t *testing.T) {
	tests := []struct {
		name     string
		template string
		captures []string
		want     string
	}{
		{
			name:     "no placeholders",
			template: "static/topic",
			captures: []string{"ignored"},
			want:     "static/topic",
		},
		{
			name:     "single placeholder",
			template: "kafka/{1}",
			captures: []string{"value"},
			want:     "kafka/value",
		},
		{
			name:     "multiple placeholders",
			template: "kafka/{1}/{2}/data",
			captures: []string{"floor1", "room2"},
			want:     "kafka/floor1/room2/data",
		},
		{
			name:     "repeated placeholder",
			template: "{1}/{1}",
			captures: []string{"value"},
			want:     "value/value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyTemplate(tt.template, tt.captures)
			if got != tt.want {
				t.Errorf("applyTemplate() = %q, want %q", got, tt.want)
			}
		})
	}
}
