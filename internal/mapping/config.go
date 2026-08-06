package mapping

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadConfig reads and parses a mapping YAML file (see configs/example/mapping.yaml
// for a small example).
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("mapping: reading %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("mapping: parsing %s: %w", path, err)
	}
	for i, f := range cfg.Fields {
		if f.Field == "" {
			return nil, fmt.Errorf("mapping: rule %d has no field name", i)
		}
		sources := 0
		if f.Builtin != "" {
			sources++
		}
		if f.EventParam != "" {
			sources++
		}
		if f.EventParamPath != "" {
			sources++
		}
		if sources != 1 {
			return nil, fmt.Errorf("mapping: field %q must have exactly one source (builtin/event_param/event_param_path), got %d", f.Field, sources)
		}
	}
	return &cfg, nil
}
