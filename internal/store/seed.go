package store

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/example/divolte-rewrite/internal/avroenc"
	"github.com/example/divolte-rewrite/internal/mapping"
)

// rawAvscField mirrors one entry of a .avsc's top-level "fields" array,
// keeping "type" and "default" as raw JSON so arbitrary shapes round-trip
// without this package needing to model every possible Avro type.
type rawAvscField struct {
	Name    string          `json:"name"`
	Type    json.RawMessage `json:"type"`
	Default json.RawMessage `json:"default"`
}

type rawAvscSchema struct {
	Fields []rawAvscField `json:"fields"`
}

// SeedFromFiles populates an empty store from the original .avsc schema and
// mapping.yaml config - the same seed data used to bootstrap Phase 1. Only
// seeds fields with `has_default` json.RawMessage checked structurally
// (some .avsc entries have no "default" key at all, which is different
// from having `"default": null`).
//
// Where the mapping.yaml has more than one rule targeting the same Avro
// field (as the real production mapping does for the documented
// cart_sku/my_list_sku quirk - the store's schema enforces one rule per
// field), only the LAST rule for that field is kept, matching what
// actually gets evaluated today (mapping.Config.Evaluate applies rules in
// order and the last one wins) - so seeding preserves observed behavior
// exactly, even though the underlying quirk is no longer representable
// after seeding (which is a deliberate modernization: the admin UI's
// one-rule-per-field model makes this class of bug impossible going
// forward).
func (s *Store) SeedFromFiles(schemaPath, mappingPath string) error {
	empty, err := s.IsEmpty()
	if err != nil {
		return err
	}
	if !empty {
		return fmt.Errorf("store: SeedFromFiles called on a non-empty store")
	}

	rawSchema, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("store: reading schema %s: %w", schemaPath, err)
	}
	var parsed rawAvscSchema
	if err := json.Unmarshal([]byte(avroenc.StripLineComments(string(rawSchema))), &parsed); err != nil {
		return fmt.Errorf("store: parsing schema %s: %w", schemaPath, err)
	}

	mappingCfg, err := mapping.LoadConfig(mappingPath)
	if err != nil {
		return fmt.Errorf("store: loading mapping %s: %w", mappingPath, err)
	}
	// Collapse to one rule per field, keeping the LAST one declared.
	lastRuleByField := make(map[string]mapping.FieldRule, len(mappingCfg.Fields))
	for _, r := range mappingCfg.Fields {
		lastRuleByField[r.Field] = r
	}

	for i, f := range parsed.Fields {
		sf := SchemaField{
			Name:     f.Name,
			TypeJSON: string(f.Type),
			Position: i + 1,
		}
		if len(f.Default) > 0 {
			sf.HasDefault = true
			sf.DefaultJSON = string(f.Default)
		}

		var mr MappingRule
		if rule, ok := lastRuleByField[f.Name]; ok {
			mr = MappingRule{
				Builtin:        rule.Builtin,
				EventParam:     rule.EventParam,
				EventParamPath: rule.EventParamPath,
				Coerce:         rule.Coerce,
			}
			if rule.Default != nil {
				mr.HasDefault = true
				mr.Default = *rule.Default
			}
		}

		if err := s.UpsertField(sf, mr); err != nil {
			return fmt.Errorf("store: seeding field %q: %w", f.Name, err)
		}
	}
	return nil
}
