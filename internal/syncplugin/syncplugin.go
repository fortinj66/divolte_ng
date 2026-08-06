// Package syncplugin defines the template every "push a published schema
// to some downstream system" integration follows - internal/nifiavro and
// internal/druid are the two built-in implementations, available by
// default. A future integration (a different NiFi parameter, a direct-
// to-Druid-JSON path, some other system entirely) implements this same
// Plugin interface rather than growing a special case in the Publish
// handler - see cmd/divolte-collector/main.go for how the active plugin
// set is assembled from stored settings.
package syncplugin

import (
	"fmt"
	"strings"
)

// Field is one schema field passed to a plugin's Sync method - just the
// name and raw Avro type JSON, enough for any downstream plugin to act on
// without depending on internal/store's own SchemaField type.
type Field struct {
	Name     string
	TypeJSON string
}

// Plugin pushes a published schema to one downstream system.
type Plugin interface {
	// Name identifies this plugin in combined sync-result messages, e.g.
	// "NiFi Avro", "Druid".
	Name() string

	// Sync pushes schemaJSON/fields to this plugin's downstream system.
	// Returns a human-readable summary of what happened, even on success
	// (the caller reports it directly in the admin UI's Publish flash
	// message). A non-nil error means THIS plugin's sync failed - RunAll
	// still runs every other configured plugin regardless, so one
	// plugin failing never blocks or skips the others.
	Sync(schemaJSON string, fields []Field) (string, error)
}

// RunAll runs every plugin in plugins, in order, regardless of whether an
// earlier one failed, and returns one combined human-readable summary
// plus an aggregate error if any plugin failed (the summary still
// describes every plugin's individual result either way, so a partial
// failure - e.g. NiFi succeeded, Druid failed - is fully visible, not
// just the first error encountered).
func RunAll(plugins []Plugin, schemaJSON string, fields []Field) (string, error) {
	if len(plugins) == 0 {
		return "", nil
	}
	var parts []string
	var failedNames []string
	for _, p := range plugins {
		msg, err := p.Sync(schemaJSON, fields)
		switch {
		case err != nil:
			parts = append(parts, p.Name()+" FAILED: "+err.Error())
			failedNames = append(failedNames, p.Name())
		case msg != "":
			parts = append(parts, p.Name()+": "+msg)
		default:
			parts = append(parts, p.Name()+": ok")
		}
	}
	summary := strings.Join(parts, "; ")
	if len(failedNames) > 0 {
		return summary, fmt.Errorf("%d of %d plugin(s) failed: %s", len(failedNames), len(plugins), strings.Join(failedNames, ", "))
	}
	return summary, nil
}
