// Package nifiavro is the built-in syncplugin.Plugin that pushes an
// updated Avro schema into the NiFi flow sitting between Kafka and Druid,
// so a schema change published through the admin UI doesn't require
// someone to hand-edit NiFi's own copy of the schema separately.
//
// The schema lives in a NiFi Parameter Context parameter (found via
// NiFi's own UI/API, not hardcoded by convention - see the parameter
// context named "NiFiAvroSchema" in the DVLTE__divolte process group),
// referenced by one controller service (an AvroSchemaRegistry). This
// package is deliberately just the Avro-specific piece - all the generic
// "stop/disable this dependency chain, do something, restore it" NiFi
// mechanics live in internal/nifi, reusable by a future plugin targeting
// a different parameter.
package nifiavro

import (
	"fmt"

	"github.com/example/divolte-rewrite/internal/nifi"
	"github.com/example/divolte-rewrite/internal/syncplugin"
)

// Config identifies exactly which parameter/controller-service this
// instance manages, on top of the generic NiFi connection config. IDs are
// specific to a given NiFi flow (they're NiFi's own internal UUIDs) and
// must be looked up once via NiFi's UI/API, not guessed.
type Config struct {
	// DisplayName identifies this target in combined Publish results
	// when more than one NiFi target is configured (e.g. "nifi-01" vs
	// "nifi-02") - defaults to "NiFi Avro" if empty.
	DisplayName string

	NiFi nifi.Config

	ParameterContextID  string
	ParameterName       string
	ControllerServiceID string // the schema registry whose dependency chain gets stopped/disabled around the update
}

// Plugin implements syncplugin.Plugin for the Divolte Avro schema.
type Plugin struct {
	cfg    Config
	client *nifi.Client
}

// New validates cfg and builds a Plugin - does not connect to NiFi yet.
func New(cfg Config) (*Plugin, error) {
	if cfg.ParameterContextID == "" {
		return nil, fmt.Errorf("nifiavro: parameter context id is required")
	}
	if cfg.ParameterName == "" {
		return nil, fmt.Errorf("nifiavro: parameter name is required")
	}
	client, err := nifi.NewClient(cfg.NiFi)
	if err != nil {
		return nil, err
	}
	return &Plugin{cfg: cfg, client: client}, nil
}

func (p *Plugin) Name() string {
	if p.cfg.DisplayName != "" {
		return "NiFi Avro (" + p.cfg.DisplayName + ")"
	}
	return "NiFi Avro"
}

// Sync stops/disables the schema registry's entire dependency chain (if
// ControllerServiceID is set), updates the parameter to schemaJSON, then
// restarts/re-enables everything in reverse. If anything fails partway,
// this makes a best-effort attempt to restore whatever it already
// changed before returning the error, so a failed publish doesn't leave
// NiFi components stopped for no reason. fields is unused - the Avro
// schema JSON is self-contained.
func (p *Plugin) Sync(schemaJSON string, fields []syncplugin.Field) (string, error) {
	var changed []nifi.ChangedComponent
	if p.cfg.ControllerServiceID != "" {
		var err error
		changed, err = p.client.StopDependencyChain(p.cfg.ControllerServiceID)
		if err != nil {
			_ = p.client.RestoreChain(changed)
			return "", fmt.Errorf("stopping dependent component(s) before parameter update: %w", err)
		}
		if err := p.client.SetControllerServiceState(p.cfg.ControllerServiceID, "DISABLED"); err != nil {
			_ = p.client.RestoreChain(changed)
			return "", fmt.Errorf("disabling schema registry before parameter update: %w", err)
		}
	}

	if err := p.client.UpdateParameterContext(p.cfg.ParameterContextID, p.cfg.ParameterName, schemaJSON); err != nil {
		if p.cfg.ControllerServiceID != "" {
			_ = p.client.SetControllerServiceState(p.cfg.ControllerServiceID, "ENABLED")
			_ = p.client.RestoreChain(changed)
		}
		return "", err
	}

	if p.cfg.ControllerServiceID != "" {
		if err := p.client.SetControllerServiceState(p.cfg.ControllerServiceID, "ENABLED"); err != nil {
			return "", fmt.Errorf("parameter updated, but re-enabling the schema registry failed (needs manual attention in NiFi): %w", err)
		}
		if err := p.client.RestoreChain(changed); err != nil {
			return "", fmt.Errorf("parameter updated and schema registry re-enabled, but restarting dependent component(s) failed (needs manual attention in NiFi): %w", err)
		}
	}
	return "schema updated", nil
}

// TestConnection validates cfg against the real NiFi cluster (current
// parameter context revision, and schema registry state plus how many
// dependents it has, if ControllerServiceID is configured) WITHOUT
// changing anything - meant for a settings UI's Test button.
func TestConnection(cfg Config) (string, error) {
	client, err := nifi.NewClient(cfg.NiFi)
	if err != nil {
		return "", err
	}
	if cfg.ParameterContextID == "" {
		return "", fmt.Errorf("nifiavro: parameter context id is required")
	}
	ver, err := client.ParameterContextRevision(cfg.ParameterContextID)
	if err != nil {
		return "", err
	}
	msg := fmt.Sprintf("connected; parameter context %q at revision %d", cfg.ParameterContextID, ver)
	if cfg.ControllerServiceID != "" {
		state, depCount, err := client.GetControllerServiceInfo(cfg.ControllerServiceID)
		if err != nil {
			return "", fmt.Errorf("connected to the parameter context, but checking the schema registry failed: %w", err)
		}
		msg += fmt.Sprintf("; schema registry state: %s, %d direct dependent component(s)", state, depCount)
	}
	return msg, nil
}
