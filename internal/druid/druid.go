// Package druid is the built-in syncplugin.Plugin that updates the Druid
// Kafka supervisor's ingestion spec so a schema change published through
// the admin UI doesn't require someone to hand-edit Druid's
// dimensionsSpec separately.
//
// The example-web-metrics supervisor (verified via Druid's own API, not
// assumed) uses an EXPLICIT, non-schemaless dimensions list
// (useSchemaDiscovery: false, includeAllDimensions: false) - a new field
// appearing in the JSON payload on Druid's input topic will NOT
// automatically become queryable; it needs an explicit new entry in
// dataSchema.dimensionsSpec.dimensions, pushed via a supervisor spec
// update. This package only ADDS new dimensions for fields not already
// present - it never removes or reorders existing ones, so a field this
// package doesn't know about (or that's no longer in the Go schema) stays
// exactly as it was.
package druid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/example/divolte-rewrite/internal/syncplugin"
)

// Config configures the connection to Druid and identifies which
// supervisor to manage.
type Config struct {
	// DisplayName identifies this target in combined Publish results
	// when more than one Druid target is configured (e.g. "druid-dev" vs
	// "druid-prod1") - defaults to "Druid" if empty.
	DisplayName string

	// BaseURL should point at Druid's ROUTER (typically port 8888), not a
	// specific Overlord node directly - the Router proxies to whichever
	// node currently holds Overlord leadership, so this keeps working
	// across a leader failover without this package needing its own
	// leader-discovery logic.
	BaseURL        string
	SupervisorName string
	Timeout        time.Duration // defaults to 30s
}

// FieldSpec is one schema field as known to the Go admin UI - Name plus
// its raw Avro type JSON (e.g. `"string"`, `["null","long"]`,
// `["null",{"type":"array","items":"string"}]`), from which the
// corresponding Druid dimension type is derived.
type FieldSpec struct {
	Name         string
	AvroTypeJSON string
}

// Syncer performs the actual Druid API calls.
type Syncer struct {
	cfg    Config
	client *http.Client
}

// New validates cfg and builds a Syncer - does not connect to Druid yet.
func New(cfg Config) (*Syncer, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("druid: base URL is required")
	}
	if cfg.SupervisorName == "" {
		return nil, fmt.Errorf("druid: supervisor name is required")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Syncer{cfg: cfg, client: &http.Client{Timeout: timeout}}, nil
}

// Plugin adapts Syncer to syncplugin.Plugin.
type Plugin struct {
	syncer      *Syncer
	displayName string
}

// NewPlugin validates cfg and builds a Plugin - does not connect to
// Druid yet.
func NewPlugin(cfg Config) (*Plugin, error) {
	s, err := New(cfg)
	if err != nil {
		return nil, err
	}
	return &Plugin{syncer: s, displayName: cfg.DisplayName}, nil
}

func (p *Plugin) Name() string {
	if p.displayName != "" {
		return "Druid (" + p.displayName + ")"
	}
	return "Druid"
}

// Sync adapts syncplugin.Field to this package's own FieldSpec and
// delegates to SyncFields. schemaJSON is unused - Druid only needs the
// per-field type information, not the full Avro schema document.
func (p *Plugin) Sync(schemaJSON string, fields []syncplugin.Field) (string, error) {
	dFields := make([]FieldSpec, len(fields))
	for i, f := range fields {
		dFields[i] = FieldSpec{Name: f.Name, AvroTypeJSON: f.TypeJSON}
	}
	return p.syncer.SyncFields(dFields)
}

// SyncFields adds any of fields not already present in the supervisor's
// dimensionsSpec, leaving every existing dimension untouched. Returns a
// human-readable summary; "no new fields to add" (nil error) is the
// common case when the schema hasn't grown since the last publish.
func (s *Syncer) SyncFields(fields []FieldSpec) (string, error) {
	var getResp struct {
		Type string                 `json:"type"`
		Spec map[string]interface{} `json:"spec"`
	}
	if err := s.doJSON(http.MethodGet, "/druid/indexer/v1/supervisor/"+s.cfg.SupervisorName, nil, &getResp); err != nil {
		return "", fmt.Errorf("druid: fetching current supervisor spec: %w", err)
	}
	spec := getResp.Spec
	if spec == nil {
		return "", fmt.Errorf("druid: supervisor %q returned no spec", s.cfg.SupervisorName)
	}
	// The GET response's "type" (e.g. "kafka") lives at the OUTER wrapper
	// level, not inside "spec" itself - but the submit/update endpoint
	// expects a SupervisorSpec JSON object that carries its own "type"
	// field directly (Druid's polymorphic deserialization needs it to
	// pick the right SupervisorSpec subtype), so it has to be copied in
	// before POSTing back what's otherwise the same object.
	if _, hasType := spec["type"]; !hasType && getResp.Type != "" {
		spec["type"] = getResp.Type
	}

	dataSchema, _ := spec["dataSchema"].(map[string]interface{})
	if dataSchema == nil {
		return "", fmt.Errorf("druid: spec has no dataSchema")
	}
	dimensionsSpec, _ := dataSchema["dimensionsSpec"].(map[string]interface{})
	if dimensionsSpec == nil {
		return "", fmt.Errorf("druid: dataSchema has no dimensionsSpec")
	}
	rawDims, _ := dimensionsSpec["dimensions"].([]interface{})
	existing := accountedForNames(dimensionsSpec)

	var added []string
	for _, f := range fields {
		if existing[f.Name] {
			continue
		}
		druidType, isArray := druidTypeFor(f.AvroTypeJSON)
		entry := map[string]interface{}{
			"type":              druidType,
			"name":              f.Name,
			"createBitmapIndex": true,
		}
		if isArray {
			entry["multiValueHandling"] = "SORTED_ARRAY"
		}
		rawDims = append(rawDims, entry)
		added = append(added, f.Name)
	}
	log.Printf("druid: %d existing dimensions, %d fields from Go schema, %d identified as new: %v", len(existing), len(fields), len(added), added)

	if len(added) == 0 {
		return "no new fields to add to Druid", nil
	}
	sort.Strings(added)

	dimensionsSpec["dimensions"] = rawDims
	dataSchema["dimensionsSpec"] = dimensionsSpec
	spec["dataSchema"] = dataSchema

	if err := s.doJSON(http.MethodPost, "/druid/indexer/v1/supervisor", spec, nil); err != nil {
		return "", fmt.Errorf("druid: submitting updated supervisor spec: %w", err)
	}

	return fmt.Sprintf("dimensions added (%s)%s", strings.Join(added, ", "), s.checkHealthBestEffort()), nil
}

// checkHealthBestEffort polls the supervisor's /status for up to 15s
// after a spec update - informational only; a slow-to-report status
// shouldn't fail a publish whose spec update already succeeded.
func (s *Syncer) checkHealthBestEffort() string {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var status struct {
			Payload struct {
				Healthy bool `json:"healthy"`
			} `json:"payload"`
		}
		if err := s.doJSON(http.MethodGet, "/druid/indexer/v1/supervisor/"+s.cfg.SupervisorName+"/status", nil, &status); err == nil && status.Payload.Healthy {
			return "; supervisor healthy"
		}
		time.Sleep(2 * time.Second)
	}
	return "; supervisor health not confirmed within 15s (check manually)"
}

// TestConnection validates cfg against the real Druid cluster (fetches
// the supervisor spec and reports its current dimension count) WITHOUT
// changing anything - meant for a settings UI's Test button.
func TestConnection(cfg Config) (string, error) {
	s, err := New(cfg)
	if err != nil {
		return "", err
	}
	var getResp struct {
		Spec map[string]interface{} `json:"spec"`
	}
	if err := s.doJSON(http.MethodGet, "/druid/indexer/v1/supervisor/"+s.cfg.SupervisorName, nil, &getResp); err != nil {
		return "", err
	}
	dataSchema, _ := getResp.Spec["dataSchema"].(map[string]interface{})
	dimensionsSpec, _ := dataSchema["dimensionsSpec"].(map[string]interface{})
	dims, _ := dimensionsSpec["dimensions"].([]interface{})
	return fmt.Sprintf("connected; supervisor %q found with %d existing dimensions", cfg.SupervisorName, len(dims)), nil
}

// accountedForNames returns every dimension name already in
// dimensionsSpec.dimensions, PLUS every name in dimensionExclusions (e.g.
// "timestamp", used by dataSchema.timestampSpec as the special time
// column, and "__time") - a field intentionally excluded must be treated
// as "already accounted for", not "missing, so add it", since trying to
// add it as a normal dimension collides with the exclusion Druid already
// has (confirmed against the real cluster: "dimensions and dimensions
// exclusions cannot overlap").
func accountedForNames(dimensionsSpec map[string]interface{}) map[string]bool {
	names := make(map[string]bool)
	if rawDims, ok := dimensionsSpec["dimensions"].([]interface{}); ok {
		for _, d := range rawDims {
			switch v := d.(type) {
			case map[string]interface{}:
				if name, ok := v["name"].(string); ok {
					names[name] = true
				}
			case string:
				names[v] = true
			}
		}
	}
	if rawExclusions, ok := dimensionsSpec["dimensionExclusions"].([]interface{}); ok {
		for _, e := range rawExclusions {
			if name, ok := e.(string); ok {
				names[name] = true
			}
		}
	}
	return names
}

// druidTypeFor maps an Avro type JSON snippet (as stored in
// store.SchemaField.TypeJSON) to a Druid dimension type ("string",
// "long", or "double") plus whether it's multi-valued. Cheap
// substring-based classification rather than a full JSON-schema walk -
// the Avro type shapes this schema actually uses (see internal/avroenc)
// are a small, known set, all correctly classified by which primitive
// keyword appears in the raw type JSON.
func druidTypeFor(avroTypeJSON string) (druidType string, isArray bool) {
	isArray = strings.Contains(avroTypeJSON, `"array"`)
	switch {
	case strings.Contains(avroTypeJSON, `"long"`), strings.Contains(avroTypeJSON, `"int"`):
		return "long", isArray
	case strings.Contains(avroTypeJSON, `"double"`), strings.Contains(avroTypeJSON, `"float"`):
		return "double", isArray
	default:
		return "string", isArray
	}
}

func (s *Syncer) doJSON(method, path string, body, out interface{}) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encoding request body: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, s.cfg.BaseURL+path, reqBody)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, string(respBody))
	}
	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("decoding response from %s %s: %w", method, path, err)
		}
	}
	return nil
}
