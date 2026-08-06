// Package store persists the Avro schema fields and mapping rules in a
// shared MariaDB/MySQL database, so internal/adminui can edit them through
// a web UI and publish changes without hand-editing files or restarting
// the server. This is new functionality versus the original Java server
// (which only supported editing the .avsc/.groovy files on disk).
//
// The database is shared across every Divolte instance (d01/d02/d03, and
// eventually p01-p03) - there is exactly one copy of the schema/mapping
// config, not one independent copy per instance. Publishing from any
// instance writes here and hot-swaps that instance's own in-memory copy
// immediately; other instances pick up the change on their own next
// restart (see docs/admin-ui-guide.md).
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/example/divolte-rewrite/internal/avroenc"
	"github.com/example/divolte-rewrite/internal/mapping"
)

// SchemaField is one Avro record field. TypeJSON and DefaultJSON hold raw
// JSON snippets (e.g. `"string"`, `["null","string"]`,
// `["null",{"type":"array","items":"string"}]`) rather than a first-class
// Go representation of every possible Avro type shape - the real schema
// only uses a handful of shapes, and storing the JSON verbatim means this
// package never needs updating if a new (still-JSON-expressible) type shows
// up through the admin UI.
type SchemaField struct {
	Name        string
	TypeJSON    string
	HasDefault  bool
	DefaultJSON string // meaningful only if HasDefault
	Position    int
}

// MappingRule mirrors mapping.FieldRule - exactly one of Builtin,
// EventParam, EventParamPath should be set.
type MappingRule struct {
	Field          string
	Builtin        string
	EventParam     string
	EventParamPath string
	Coerce         string
	HasDefault     bool
	Default        string // meaningful only if HasDefault
}

// Config connects to the shared database.
type Config struct {
	Host     string
	Port     int
	Name     string
	Username string
	Password string
}

// Store wraps a MariaDB/MySQL database holding the schema/mapping tables.
// Safe for concurrent use by multiple goroutines within one process, and
// by multiple Divolte instances/processes at once - the read-then-write
// sequences (ReorderBlock, SetFieldOrder, PublishSnapshot, Revert) use
// `SELECT ... FOR UPDATE` inside a transaction to lock the rows they read,
// so a concurrent writer (whether another goroutine here or another
// instance entirely) can't produce a lost update between the read and the
// write - InnoDB's own row locking does the serialization, rather than an
// in-process mutex that could only ever protect this one instance anyway.
type Store struct {
	db *sql.DB
}

// Open connects to the shared database and ensures the schema exists.
func Open(cfg Config) (*Store, error) {
	// clientFoundRows=true makes RowsAffected() report rows MATCHED by the
	// WHERE clause, not rows whose value actually changed (MySQL/MariaDB's
	// default) - ReorderBlock/SetFieldOrder rely on RowsAffected to detect a
	// field vanishing out from under a concurrent edit, and a field whose
	// position happens to already equal the value being written (i.e. it
	// isn't moving) would otherwise report 0 rows affected and be mistaken
	// for "no longer exists".
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&clientFoundRows=true&loc=UTC",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Name)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: opening %s:%d/%s: %w", cfg.Host, cfg.Port, cfg.Name, err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: connecting to %s:%d/%s: %w", cfg.Host, cfg.Port, cfg.Name, err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS schema_fields (
	name         VARCHAR(255) PRIMARY KEY,
	type_json    TEXT NOT NULL,
	has_default  TINYINT(1) NOT NULL DEFAULT 0,
	default_json TEXT,
	position     INT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS mapping_rules (
	field_name       VARCHAR(255) PRIMARY KEY,
	builtin          VARCHAR(255) NOT NULL DEFAULT '',
	event_param      VARCHAR(255) NOT NULL DEFAULT '',
	event_param_path VARCHAR(255) NOT NULL DEFAULT '',
	coerce           VARCHAR(32) NOT NULL DEFAULT '',
	has_default      TINYINT(1) NOT NULL DEFAULT 0,
	default_value    TEXT NOT NULL,
	FOREIGN KEY (field_name) REFERENCES schema_fields(name) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS published_snapshot (
	id                 TINYINT PRIMARY KEY,
	schema_fields_json LONGTEXT NOT NULL,
	mapping_rules_json LONGTEXT NOT NULL,
	published_at       VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS admin_settings (
	id          TINYINT PRIMARY KEY,
	username    VARCHAR(255) NOT NULL,
	password    VARCHAR(255) NOT NULL,
	primary_url VARCHAR(512) NOT NULL DEFAULT ''
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ldap_settings (
	id                 TINYINT PRIMARY KEY,
	enabled            TINYINT(1) NOT NULL DEFAULT 0,
	servers            TEXT NOT NULL,
	manager_dn         VARCHAR(512) NOT NULL DEFAULT '',
	manager_password   VARCHAR(255) NOT NULL DEFAULT '',
	user_search_base   VARCHAR(512) NOT NULL DEFAULT '',
	user_search_filter VARCHAR(255) NOT NULL DEFAULT '',
	allowed_groups     TEXT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sync_settings (
	id                     TINYINT PRIMARY KEY,
	nifi_enabled           TINYINT(1) NOT NULL DEFAULT 0,
	nifi_base_url          VARCHAR(512) NOT NULL DEFAULT '',
	nifi_client_cert       TEXT NOT NULL,
	nifi_client_key        TEXT NOT NULL,
	nifi_ca_cert           TEXT NOT NULL,
	nifi_parameter_context_id   VARCHAR(64) NOT NULL DEFAULT '',
	nifi_parameter_name         VARCHAR(255) NOT NULL DEFAULT '',
	nifi_controller_service_id  VARCHAR(64) NOT NULL DEFAULT '',
	druid_enabled          TINYINT(1) NOT NULL DEFAULT 0,
	druid_base_url         VARCHAR(512) NOT NULL DEFAULT '',
	druid_supervisor_name  VARCHAR(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS nifi_avro_targets (
	id                     VARCHAR(64) PRIMARY KEY,
	enabled                TINYINT(1) NOT NULL DEFAULT 0,
	base_url               VARCHAR(512) NOT NULL DEFAULT '',
	client_cert            TEXT NOT NULL,
	client_key             TEXT NOT NULL,
	client_key_passphrase  VARCHAR(255) NOT NULL DEFAULT '',
	ca_cert                TEXT NOT NULL,
	parameter_context_id   VARCHAR(64) NOT NULL DEFAULT '',
	parameter_name         VARCHAR(255) NOT NULL DEFAULT '',
	controller_service_id  VARCHAR(64) NOT NULL DEFAULT ''
) ENGINE=InnoDB;

ALTER TABLE nifi_avro_targets ADD COLUMN IF NOT EXISTS client_key_passphrase VARCHAR(255) NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS druid_targets (
	id              VARCHAR(64) PRIMARY KEY,
	enabled         TINYINT(1) NOT NULL DEFAULT 0,
	base_url        VARCHAR(512) NOT NULL DEFAULT '',
	supervisor_name VARCHAR(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS kafka_output_targets (
	id       VARCHAR(64) PRIMARY KEY,
	enabled  TINYINT(1) NOT NULL DEFAULT 0,
	format   VARCHAR(16) NOT NULL DEFAULT 'avro',
	topic    VARCHAR(255) NOT NULL,
	brokers  TEXT NOT NULL
) ENGINE=InnoDB;
`)
	if err != nil {
		return fmt.Errorf("store: migrating schema: %w", err)
	}
	return nil
}

// IsEmpty reports whether schema_fields has no rows yet (used to decide
// whether to seed from the original files on first run).
func (s *Store) IsEmpty() (bool, error) {
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM schema_fields`).Scan(&count); err != nil {
		return false, fmt.Errorf("store: checking IsEmpty: %w", err)
	}
	return count == 0, nil
}

// AdminSettings holds the shared admin-UI login and the designated
// primary instance's admin URL. Stored in the shared database (like
// schema_fields/mapping_rules) so every Divolte instance - d01/d02/d03,
// eventually p01-p03 - uses the SAME login instead of each host having
// its own independent DIVOLTE_ADMIN_PASSWORD, and so every instance
// agrees on which one is "the" canonical admin UI to redirect to.
type AdminSettings struct {
	Username   string
	Password   string
	PrimaryURL string // e.g. "https://collector-01.example.com/admin" - empty disables the redirect
}

// EnsureAdminSettingsSeeded inserts the single admin_settings row if it
// doesn't exist yet, using defaultUsername/defaultPassword (normally an
// instance's own server.yaml/env-var values) as the initial shared
// credentials. A no-op once the row exists - the database becomes the
// source of truth from then on, matching schema_fields/mapping_rules.
func (s *Store) EnsureAdminSettingsSeeded(defaultUsername, defaultPassword string) error {
	_, err := s.db.Exec(`
INSERT INTO admin_settings (id, username, password, primary_url)
VALUES (1, ?, ?, '')
ON DUPLICATE KEY UPDATE id = id`, defaultUsername, defaultPassword)
	if err != nil {
		return fmt.Errorf("store: seeding admin settings: %w", err)
	}
	return nil
}

// GetAdminSettings returns the current shared admin login and primary URL.
func (s *Store) GetAdminSettings() (AdminSettings, error) {
	var a AdminSettings
	err := s.db.QueryRow(`SELECT username, password, primary_url FROM admin_settings WHERE id = 1`).
		Scan(&a.Username, &a.Password, &a.PrimaryURL)
	if err != nil {
		return AdminSettings{}, fmt.Errorf("store: getting admin settings: %w", err)
	}
	return a, nil
}

// SetAdminCredentials updates the shared admin login used by every
// instance.
func (s *Store) SetAdminCredentials(username, password string) error {
	_, err := s.db.Exec(`UPDATE admin_settings SET username = ?, password = ? WHERE id = 1`, username, password)
	if err != nil {
		return fmt.Errorf("store: setting admin credentials: %w", err)
	}
	return nil
}

// SetPrimaryURL updates the designated primary instance's admin URL that
// every instance's admin UI redirects non-primary requests to. Empty
// disables the redirect.
func (s *Store) SetPrimaryURL(url string) error {
	_, err := s.db.Exec(`UPDATE admin_settings SET primary_url = ? WHERE id = 1`, url)
	if err != nil {
		return fmt.Errorf("store: setting primary url: %w", err)
	}
	return nil
}

// LDAPSettings holds optional LDAP/Active Directory authentication config
// for the admin UI - editable at runtime via /settings and stored in the
// shared database (like AdminSettings), so every instance sees the SAME
// config instead of each reading its own server.yaml. See
// internal/ldapauth's package doc for the authentication semantics
// (search-then-bind, always requires AllowedGroups membership).
type LDAPSettings struct {
	Enabled          bool
	Servers          []string
	ManagerDN        string
	ManagerPassword  string
	UserSearchBase   string
	UserSearchFilter string
	AllowedGroups    []string
}

// joinLines/splitLines store a []string as newline-separated TEXT -
// simpler than a JSON column for values that are themselves plain
// strings with no embedded newlines (server URLs, group names/DNs), and
// still trivially editable as a multi-line <textarea>.
func joinLines(items []string) string { return strings.Join(items, "\n") }

func splitLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

// EnsureLDAPSettingsSeeded inserts the single ldap_settings row if it
// doesn't exist yet, using defaults (normally an instance's own
// server.yaml admin.ldap values) as the initial config - a no-op once the
// row exists, matching EnsureAdminSettingsSeeded's pattern. From then on
// the database, editable via /settings, is authoritative - not any
// instance's own config file.
func (s *Store) EnsureLDAPSettingsSeeded(defaults LDAPSettings) error {
	_, err := s.db.Exec(`
INSERT INTO ldap_settings (id, enabled, servers, manager_dn, manager_password, user_search_base, user_search_filter, allowed_groups)
VALUES (1, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE id = id`,
		defaults.Enabled, joinLines(defaults.Servers), defaults.ManagerDN, defaults.ManagerPassword,
		defaults.UserSearchBase, defaults.UserSearchFilter, joinLines(defaults.AllowedGroups))
	if err != nil {
		return fmt.Errorf("store: seeding ldap settings: %w", err)
	}
	return nil
}

// GetLDAPSettings returns the current LDAP config. Returns a zero-value
// (Enabled=false) LDAPSettings, not an error, if the row doesn't exist
// yet - LDAP being unconfigured is a normal, common state, not a fault.
func (s *Store) GetLDAPSettings() (LDAPSettings, error) {
	var enabled bool
	var servers, managerDN, managerPassword, userSearchBase, userSearchFilter, allowedGroups string
	err := s.db.QueryRow(`SELECT enabled, servers, manager_dn, manager_password, user_search_base, user_search_filter, allowed_groups FROM ldap_settings WHERE id = 1`).
		Scan(&enabled, &servers, &managerDN, &managerPassword, &userSearchBase, &userSearchFilter, &allowedGroups)
	if err == sql.ErrNoRows {
		return LDAPSettings{}, nil
	}
	if err != nil {
		return LDAPSettings{}, fmt.Errorf("store: getting ldap settings: %w", err)
	}
	return LDAPSettings{
		Enabled:          enabled,
		Servers:          splitLines(servers),
		ManagerDN:        managerDN,
		ManagerPassword:  managerPassword,
		UserSearchBase:   userSearchBase,
		UserSearchFilter: userSearchFilter,
		AllowedGroups:    splitLines(allowedGroups),
	}, nil
}

// SetLDAPSettings replaces the stored LDAP config wholesale (upsert, so
// it works even if EnsureLDAPSettingsSeeded was never called).
func (s *Store) SetLDAPSettings(settings LDAPSettings) error {
	_, err := s.db.Exec(`
INSERT INTO ldap_settings (id, enabled, servers, manager_dn, manager_password, user_search_base, user_search_filter, allowed_groups)
VALUES (1, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE enabled = VALUES(enabled), servers = VALUES(servers), manager_dn = VALUES(manager_dn),
	manager_password = VALUES(manager_password), user_search_base = VALUES(user_search_base),
	user_search_filter = VALUES(user_search_filter), allowed_groups = VALUES(allowed_groups)`,
		settings.Enabled, joinLines(settings.Servers), settings.ManagerDN, settings.ManagerPassword,
		settings.UserSearchBase, settings.UserSearchFilter, joinLines(settings.AllowedGroups))
	if err != nil {
		return fmt.Errorf("store: setting ldap settings: %w", err)
	}
	return nil
}

// SyncSettings holds config for pushing a published schema out to NiFi's
// Parameter Context (internal/nifisync) and Druid's Kafka supervisor
// (internal/druidsync), so a schema change doesn't require hand-editing
// either downstream system separately. Stored in the shared database
// (like AdminSettings/LDAPSettings), editable at runtime via /settings.
// NiFi's client cert/key are stored here as PEM text - see internal/
// adminui's settings form for the operational tradeoff (this puts key
// material in the database rather than a per-host file).
type SyncSettings struct {
	NiFiEnabled             bool
	NiFiBaseURL             string
	NiFiClientCertPEM       string
	NiFiClientKeyPEM        string
	NiFiCACertPEM           string
	NiFiParameterContextID  string
	NiFiParameterName       string
	NiFiControllerServiceID string

	DruidEnabled        bool
	DruidBaseURL        string
	DruidSupervisorName string
}

// EnsureSyncSettingsSeeded inserts the single sync_settings row if it
// doesn't exist yet - a no-op once the row exists, matching
// EnsureAdminSettingsSeeded/EnsureLDAPSettingsSeeded's pattern. Both NiFi
// and Druid sync stay disabled until explicitly configured via /settings.
func (s *Store) EnsureSyncSettingsSeeded() error {
	_, err := s.db.Exec(`
INSERT INTO sync_settings (id, nifi_enabled, nifi_base_url, nifi_client_cert, nifi_client_key, nifi_ca_cert,
	nifi_parameter_context_id, nifi_parameter_name, nifi_controller_service_id, druid_enabled, druid_base_url, druid_supervisor_name)
VALUES (1, 0, '', '', '', '', '', '', '', 0, '', '')
ON DUPLICATE KEY UPDATE id = id`)
	if err != nil {
		return fmt.Errorf("store: seeding sync settings: %w", err)
	}
	return nil
}

// GetSyncSettings returns the current NiFi/Druid sync config. Returns a
// zero-value (both disabled) SyncSettings, not an error, if the row
// doesn't exist yet.
func (s *Store) GetSyncSettings() (SyncSettings, error) {
	var v SyncSettings
	err := s.db.QueryRow(`
SELECT nifi_enabled, nifi_base_url, nifi_client_cert, nifi_client_key, nifi_ca_cert,
	nifi_parameter_context_id, nifi_parameter_name, nifi_controller_service_id,
	druid_enabled, druid_base_url, druid_supervisor_name
FROM sync_settings WHERE id = 1`).Scan(
		&v.NiFiEnabled, &v.NiFiBaseURL, &v.NiFiClientCertPEM, &v.NiFiClientKeyPEM, &v.NiFiCACertPEM,
		&v.NiFiParameterContextID, &v.NiFiParameterName, &v.NiFiControllerServiceID,
		&v.DruidEnabled, &v.DruidBaseURL, &v.DruidSupervisorName)
	if err == sql.ErrNoRows {
		return SyncSettings{}, nil
	}
	if err != nil {
		return SyncSettings{}, fmt.Errorf("store: getting sync settings: %w", err)
	}
	return v, nil
}

// SetSyncSettings replaces the stored NiFi/Druid sync config wholesale
// (upsert, so it works even if EnsureSyncSettingsSeeded was never
// called).
func (s *Store) SetSyncSettings(v SyncSettings) error {
	_, err := s.db.Exec(`
INSERT INTO sync_settings (id, nifi_enabled, nifi_base_url, nifi_client_cert, nifi_client_key, nifi_ca_cert,
	nifi_parameter_context_id, nifi_parameter_name, nifi_controller_service_id, druid_enabled, druid_base_url, druid_supervisor_name)
VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE nifi_enabled = VALUES(nifi_enabled), nifi_base_url = VALUES(nifi_base_url),
	nifi_client_cert = VALUES(nifi_client_cert), nifi_client_key = VALUES(nifi_client_key), nifi_ca_cert = VALUES(nifi_ca_cert),
	nifi_parameter_context_id = VALUES(nifi_parameter_context_id), nifi_parameter_name = VALUES(nifi_parameter_name),
	nifi_controller_service_id = VALUES(nifi_controller_service_id),
	druid_enabled = VALUES(druid_enabled), druid_base_url = VALUES(druid_base_url), druid_supervisor_name = VALUES(druid_supervisor_name)`,
		v.NiFiEnabled, v.NiFiBaseURL, v.NiFiClientCertPEM, v.NiFiClientKeyPEM, v.NiFiCACertPEM,
		v.NiFiParameterContextID, v.NiFiParameterName, v.NiFiControllerServiceID,
		v.DruidEnabled, v.DruidBaseURL, v.DruidSupervisorName)
	if err != nil {
		return fmt.Errorf("store: setting sync settings: %w", err)
	}
	return nil
}

// ListSchemaFields returns all fields ordered by their stored position.
func (s *Store) ListSchemaFields() ([]SchemaField, error) {
	return listSchemaFields(s.db)
}

// queryer is satisfied by both *sql.DB and *sql.Tx, so the same query logic
// works whether or not the caller wants it inside a transaction.
type queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

func listSchemaFields(q queryer) ([]SchemaField, error) {
	rows, err := q.Query(`SELECT name, type_json, has_default, default_json, position FROM schema_fields ORDER BY position`)
	if err != nil {
		return nil, fmt.Errorf("store: listing schema fields: %w", err)
	}
	defer rows.Close()

	var out []SchemaField
	for rows.Next() {
		var f SchemaField
		var defaultJSON sql.NullString
		if err := rows.Scan(&f.Name, &f.TypeJSON, &f.HasDefault, &defaultJSON, &f.Position); err != nil {
			return nil, fmt.Errorf("store: scanning schema field: %w", err)
		}
		f.DefaultJSON = defaultJSON.String
		out = append(out, f)
	}
	return out, rows.Err()
}

// listSchemaFieldsForUpdate is listSchemaFields, but locks every returned
// row (SELECT ... FOR UPDATE) - must be called inside a transaction, used
// by the read-then-write sequences below so a concurrent writer (another
// goroutine or another instance) can't slip in between the read and the
// write.
func listSchemaFieldsForUpdate(tx *sql.Tx) ([]SchemaField, error) {
	rows, err := tx.Query(`SELECT name, type_json, has_default, default_json, position FROM schema_fields ORDER BY position FOR UPDATE`)
	if err != nil {
		return nil, fmt.Errorf("store: listing schema fields for update: %w", err)
	}
	defer rows.Close()

	var out []SchemaField
	for rows.Next() {
		var f SchemaField
		var defaultJSON sql.NullString
		if err := rows.Scan(&f.Name, &f.TypeJSON, &f.HasDefault, &defaultJSON, &f.Position); err != nil {
			return nil, fmt.Errorf("store: scanning schema field: %w", err)
		}
		f.DefaultJSON = defaultJSON.String
		out = append(out, f)
	}
	return out, rows.Err()
}

// ListMappingRules returns all mapping rules.
func (s *Store) ListMappingRules() ([]MappingRule, error) {
	return listMappingRules(s.db)
}

func listMappingRules(q queryer) ([]MappingRule, error) {
	rows, err := q.Query(`SELECT field_name, builtin, event_param, event_param_path, coerce, has_default, default_value FROM mapping_rules`)
	if err != nil {
		return nil, fmt.Errorf("store: listing mapping rules: %w", err)
	}
	defer rows.Close()

	var out []MappingRule
	for rows.Next() {
		var r MappingRule
		if err := rows.Scan(&r.Field, &r.Builtin, &r.EventParam, &r.EventParamPath, &r.Coerce, &r.HasDefault, &r.Default); err != nil {
			return nil, fmt.Errorf("store: scanning mapping rule: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetField returns one field and its mapping rule (rule is nil if the field
// has none - shouldn't normally happen, but the admin UI needs to handle it
// gracefully for a field added without a rule yet).
func (s *Store) GetField(name string) (*SchemaField, *MappingRule, error) {
	var f SchemaField
	var defaultJSON sql.NullString
	err := s.db.QueryRow(`SELECT name, type_json, has_default, default_json, position FROM schema_fields WHERE name = ?`, name).
		Scan(&f.Name, &f.TypeJSON, &f.HasDefault, &defaultJSON, &f.Position)
	if err == sql.ErrNoRows {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("store: getting field %q: %w", name, err)
	}
	f.DefaultJSON = defaultJSON.String

	var r MappingRule
	err = s.db.QueryRow(`SELECT field_name, builtin, event_param, event_param_path, coerce, has_default, default_value FROM mapping_rules WHERE field_name = ?`, name).
		Scan(&r.Field, &r.Builtin, &r.EventParam, &r.EventParamPath, &r.Coerce, &r.HasDefault, &r.Default)
	if err == sql.ErrNoRows {
		return &f, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("store: getting mapping rule for %q: %w", name, err)
	}
	return &f, &r, nil
}

// UpsertField creates or replaces a field and its mapping rule in one
// transaction. If position is <= 0, the field is appended after the
// current maximum position (i.e. new fields go last, matching how you'd
// naturally add a field through the UI).
func (s *Store) UpsertField(f SchemaField, r MappingRule) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("store: beginning transaction: %w", err)
	}
	defer tx.Rollback()

	if f.Position <= 0 {
		var maxPos sql.NullInt64
		if err := tx.QueryRow(`SELECT MAX(position) FROM schema_fields FOR UPDATE`).Scan(&maxPos); err != nil {
			return fmt.Errorf("store: computing next position: %w", err)
		}
		f.Position = int(maxPos.Int64) + 1
	}

	_, err = tx.Exec(`
INSERT INTO schema_fields (name, type_json, has_default, default_json, position)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE type_json = VALUES(type_json), has_default = VALUES(has_default), default_json = VALUES(default_json)`,
		f.Name, f.TypeJSON, f.HasDefault, f.DefaultJSON, f.Position)
	if err != nil {
		return fmt.Errorf("store: upserting field %q: %w", f.Name, err)
	}

	r.Field = f.Name
	_, err = tx.Exec(`
INSERT INTO mapping_rules (field_name, builtin, event_param, event_param_path, coerce, has_default, default_value)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE builtin = VALUES(builtin), event_param = VALUES(event_param),
	event_param_path = VALUES(event_param_path), coerce = VALUES(coerce),
	has_default = VALUES(has_default), default_value = VALUES(default_value)`,
		r.Field, r.Builtin, r.EventParam, r.EventParamPath, r.Coerce, r.HasDefault, r.Default)
	if err != nil {
		return fmt.Errorf("store: upserting mapping rule for %q: %w", f.Name, err)
	}

	return tx.Commit()
}

// ReorderBlock moves a contiguous block of fields (identified by name) up
// or down by one position, swapping the whole block past its single
// neighboring field on that side, e.g. order A,B,C,D moving [B,C] up
// becomes B,C,A,D; moving [B,C] down becomes A,D,B,C. names must
// currently form a contiguous run in position order, or this errors.
// direction must be "up" or "down". A move off either end of the list
// (nothing to swap with) is a no-op, not an error.
func (s *Store) ReorderBlock(names []string, direction string) error {
	if len(names) == 0 {
		return fmt.Errorf("store: no fields specified to move")
	}
	if direction != "up" && direction != "down" {
		return fmt.Errorf("store: unknown direction %q", direction)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("store: beginning transaction: %w", err)
	}
	defer tx.Rollback()

	fields, err := listSchemaFieldsForUpdate(tx)
	if err != nil {
		return err
	}
	order := make([]string, len(fields))
	for i, f := range fields {
		order[i] = f.Name
	}

	wanted := make(map[string]bool, len(names))
	for _, n := range names {
		wanted[n] = true
	}
	var indices []int
	for i, n := range order {
		if wanted[n] {
			indices = append(indices, i)
		}
	}
	if len(indices) != len(names) {
		return fmt.Errorf("store: one or more selected fields don't exist")
	}
	for i := 1; i < len(indices); i++ {
		if indices[i] != indices[i-1]+1 {
			return fmt.Errorf("store: selected fields must be consecutive to be moved together")
		}
	}
	start, end := indices[0], indices[len(indices)-1]

	switch direction {
	case "up":
		if start == 0 {
			return nil // already at the top
		}
		neighbor := order[start-1]
		newOrder := append([]string{}, order[:start-1]...)
		newOrder = append(newOrder, order[start:end+1]...)
		newOrder = append(newOrder, neighbor)
		newOrder = append(newOrder, order[end+1:]...)
		order = newOrder
	case "down":
		if end == len(order)-1 {
			return nil // already at the bottom
		}
		neighbor := order[end+1]
		newOrder := append([]string{}, order[:start]...)
		newOrder = append(newOrder, neighbor)
		newOrder = append(newOrder, order[start:end+1]...)
		newOrder = append(newOrder, order[end+2:]...)
		order = newOrder
	}

	for i, name := range order {
		res, err := tx.Exec(`UPDATE schema_fields SET position = ? WHERE name = ?`, i+1, name)
		if err != nil {
			return fmt.Errorf("store: updating position for %q: %w", name, err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("store: field %q no longer exists (changed concurrently?)", name)
		}
	}
	return tx.Commit()
}

// SetFieldOrder persists an exact field ordering - used by drag-and-drop
// reordering in the admin UI, where the client computes the full new order
// and submits it directly rather than a single up/down move. names must be
// exactly the set of existing field names, in the desired new order.
func (s *Store) SetFieldOrder(names []string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("store: beginning transaction: %w", err)
	}
	defer tx.Rollback()

	fields, err := listSchemaFieldsForUpdate(tx)
	if err != nil {
		return err
	}
	if len(names) != len(fields) {
		return fmt.Errorf("store: field order must include exactly the %d existing fields, got %d", len(fields), len(names))
	}
	existing := make(map[string]bool, len(fields))
	for _, f := range fields {
		existing[f.Name] = true
	}
	seen := make(map[string]bool, len(names))
	for _, n := range names {
		if !existing[n] {
			return fmt.Errorf("store: unknown field %q", n)
		}
		if seen[n] {
			return fmt.Errorf("store: field %q appears more than once in the new order", n)
		}
		seen[n] = true
	}

	for i, name := range names {
		res, err := tx.Exec(`UPDATE schema_fields SET position = ? WHERE name = ?`, i+1, name)
		if err != nil {
			return fmt.Errorf("store: updating position for %q: %w", name, err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("store: field %q no longer exists (changed concurrently?)", name)
		}
	}
	return tx.Commit()
}

// DeleteField removes a field and (via ON DELETE CASCADE) its mapping rule.
func (s *Store) DeleteField(name string) error {
	res, err := s.db.Exec(`DELETE FROM schema_fields WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("store: deleting field %q: %w", name, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("store: no such field: %q", name)
	}
	return nil
}

// BuildSchemaJSON reconstructs a full Avro record schema JSON document from
// the stored fields, ordered by position - suitable for avroenc.LoadSchema.
func (s *Store) BuildSchemaJSON(namespace, recordName string) (string, error) {
	fields, err := s.ListSchemaFields()
	if err != nil {
		return "", err
	}
	return buildSchemaJSON(fields, namespace, recordName)
}

func buildSchemaJSON(fields []SchemaField, namespace, recordName string) (string, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`{"namespace":%s,"type":"record","name":%s,"fields":[`, jsonString(namespace), jsonString(recordName)))
	for i, f := range fields {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf(`{"name":%s,"type":%s`, jsonString(f.Name), f.TypeJSON))
		if f.HasDefault {
			sb.WriteString(`,"default":`)
			sb.WriteString(f.DefaultJSON)
		}
		sb.WriteByte('}')
	}
	sb.WriteString(`]}`)

	// Validate it actually parses as a usable schema before handing it back.
	if _, err := avroenc.LoadSchema(sb.String()); err != nil {
		return "", fmt.Errorf("store: reconstructed schema JSON is invalid: %w", err)
	}
	return sb.String(), nil
}

// BuildMappingConfig reconstructs a mapping.Config from the stored mapping
// rules, ordered by each field's schema position (for readable/stable
// output - evaluation order only matters when two rules target the same
// field, which the admin UI's one-rule-per-field model prevents going
// forward).
func (s *Store) BuildMappingConfig() (*mapping.Config, error) {
	fields, err := s.ListSchemaFields()
	if err != nil {
		return nil, err
	}
	rules, err := s.ListMappingRules()
	if err != nil {
		return nil, err
	}
	return buildMappingConfig(fields, rules), nil
}

func buildMappingConfig(fields []SchemaField, rules []MappingRule) *mapping.Config {
	byField := make(map[string]MappingRule, len(rules))
	for _, r := range rules {
		byField[r.Field] = r
	}

	cfg := &mapping.Config{}
	for _, f := range fields {
		r, ok := byField[f.Name]
		if !ok {
			continue // a field with no rule row at all is simply not mapped
		}
		if r.Builtin == "" && r.EventParam == "" && r.EventParamPath == "" {
			// A rule row exists but has no source configured yet (e.g. a
			// field added through the admin UI before its mapping was set,
			// or - as seeded from the real production mapping.yaml -
			// cart_sku, which no rule in the original ever actually
			// targeted). Treat this exactly like "no rule row at all": the
			// field is simply absent, never a hard mapping.Evaluate error.
			continue
		}
		rule := mapping.FieldRule{
			Field:          f.Name,
			Builtin:        r.Builtin,
			EventParam:     r.EventParam,
			EventParamPath: r.EventParamPath,
			Coerce:         r.Coerce,
		}
		if r.HasDefault {
			d := r.Default
			rule.Default = &d
		}
		cfg.Fields = append(cfg.Fields, rule)
	}
	return cfg
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// SaveSnapshot captures the current schema_fields/mapping_rules as the
// "last published" baseline that Revert restores to. Called both after a
// successful Publish and once at server boot (whatever the store contains
// when the server starts serving traffic with it counts as "published").
//
// Prefer PublishSnapshot from adminui's publish flow: it builds the schema
// JSON/mapping config AND saves the snapshot from one single consistent
// read, so what gets snapshotted as "published" is guaranteed to be exactly
// what was actually handed to Publisher.Publish - calling BuildSchemaJSON/
// BuildMappingConfig and SaveSnapshot as separate steps leaves a window
// where a concurrent edit between those calls makes the snapshot record a
// state that was never actually published.
func (s *Store) SaveSnapshot() error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("store: beginning transaction: %w", err)
	}
	defer tx.Rollback()

	fields, err := listSchemaFieldsForUpdate(tx)
	if err != nil {
		return err
	}
	rules, err := listMappingRules(tx)
	if err != nil {
		return err
	}
	if err := writeSnapshot(tx, fields, rules); err != nil {
		return err
	}
	return tx.Commit()
}

// PublishSnapshot reads schema_fields/mapping_rules exactly once (locking
// the rows for the duration of the transaction, so a concurrent publish
// from another instance can't interleave), builds the schema JSON and
// mapping config from that single consistent read, and (if both build
// cleanly) saves that same data as the published snapshot - so the
// snapshot Revert restores to is always exactly what was built here, never
// a later or earlier state some concurrent edit produced in between.
func (s *Store) PublishSnapshot(namespace, recordName string) (schemaJSON string, mappingCfg *mapping.Config, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return "", nil, fmt.Errorf("store: beginning transaction: %w", err)
	}
	defer tx.Rollback()

	fields, err := listSchemaFieldsForUpdate(tx)
	if err != nil {
		return "", nil, err
	}
	rules, err := listMappingRules(tx)
	if err != nil {
		return "", nil, err
	}

	schemaJSON, err = buildSchemaJSON(fields, namespace, recordName)
	if err != nil {
		return "", nil, err
	}
	mappingCfg = buildMappingConfig(fields, rules)

	if err := writeSnapshot(tx, fields, rules); err != nil {
		return "", nil, err
	}
	if err := tx.Commit(); err != nil {
		return "", nil, fmt.Errorf("store: committing publish snapshot: %w", err)
	}
	return schemaJSON, mappingCfg, nil
}

// writeSnapshot persists fields/rules as the published snapshot, within tx.
func writeSnapshot(tx *sql.Tx, fields []SchemaField, rules []MappingRule) error {
	fieldsJSON, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("store: marshaling snapshot fields: %w", err)
	}
	rulesJSON, err := json.Marshal(rules)
	if err != nil {
		return fmt.Errorf("store: marshaling snapshot rules: %w", err)
	}

	_, err = tx.Exec(`
INSERT INTO published_snapshot (id, schema_fields_json, mapping_rules_json, published_at)
VALUES (1, ?, ?, ?)
ON DUPLICATE KEY UPDATE schema_fields_json = VALUES(schema_fields_json),
	mapping_rules_json = VALUES(mapping_rules_json), published_at = VALUES(published_at)`,
		string(fieldsJSON), string(rulesJSON), time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("store: saving snapshot: %w", err)
	}
	return nil
}

// NiFiAvroTarget is one named NiFi cluster to push the Avro schema to -
// there can be multiple (e.g. nifi-01 and nifi-02 are separate
// clusters). ID is a short slug (e.g. "nifi-legacy") - both the primary
// key and the display name shown in combined Publish results.
type NiFiAvroTarget struct {
	ID                  string
	Enabled             bool
	BaseURL             string
	ClientCertPEM       string
	ClientKeyPEM        string
	ClientKeyPassphrase string
	CACertPEM           string
	ParameterContextID  string
	ParameterName       string
	ControllerServiceID string
}

// DruidTarget is one named Druid cluster/supervisor to push dimension
// updates to - there can be multiple (dev, prod1, prod2, ...). ID is a
// short slug (e.g. "druid-dev") - both the primary key and the display
// name shown in combined Publish results.
type DruidTarget struct {
	ID             string
	Enabled        bool
	BaseURL        string
	SupervisorName string
}

// ListNiFiAvroTargets returns every configured NiFi target, ordered by
// ID.
func (s *Store) ListNiFiAvroTargets() ([]NiFiAvroTarget, error) {
	rows, err := s.db.Query(`SELECT id, enabled, base_url, client_cert, client_key, client_key_passphrase, ca_cert, parameter_context_id, parameter_name, controller_service_id FROM nifi_avro_targets ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("store: listing nifi avro targets: %w", err)
	}
	defer rows.Close()

	var out []NiFiAvroTarget
	for rows.Next() {
		var t NiFiAvroTarget
		if err := rows.Scan(&t.ID, &t.Enabled, &t.BaseURL, &t.ClientCertPEM, &t.ClientKeyPEM, &t.ClientKeyPassphrase, &t.CACertPEM, &t.ParameterContextID, &t.ParameterName, &t.ControllerServiceID); err != nil {
			return nil, fmt.Errorf("store: scanning nifi avro target: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// GetNiFiAvroTarget returns one target by ID, or nil if it doesn't exist.
func (s *Store) GetNiFiAvroTarget(id string) (*NiFiAvroTarget, error) {
	var t NiFiAvroTarget
	err := s.db.QueryRow(`SELECT id, enabled, base_url, client_cert, client_key, client_key_passphrase, ca_cert, parameter_context_id, parameter_name, controller_service_id FROM nifi_avro_targets WHERE id = ?`, id).
		Scan(&t.ID, &t.Enabled, &t.BaseURL, &t.ClientCertPEM, &t.ClientKeyPEM, &t.ClientKeyPassphrase, &t.CACertPEM, &t.ParameterContextID, &t.ParameterName, &t.ControllerServiceID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: getting nifi avro target %q: %w", id, err)
	}
	return &t, nil
}

// UpsertNiFiAvroTarget creates or replaces a target by ID.
func (s *Store) UpsertNiFiAvroTarget(t NiFiAvroTarget) error {
	_, err := s.db.Exec(`
INSERT INTO nifi_avro_targets (id, enabled, base_url, client_cert, client_key, client_key_passphrase, ca_cert, parameter_context_id, parameter_name, controller_service_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE enabled = VALUES(enabled), base_url = VALUES(base_url), client_cert = VALUES(client_cert),
	client_key = VALUES(client_key), client_key_passphrase = VALUES(client_key_passphrase), ca_cert = VALUES(ca_cert), parameter_context_id = VALUES(parameter_context_id),
	parameter_name = VALUES(parameter_name), controller_service_id = VALUES(controller_service_id)`,
		t.ID, t.Enabled, t.BaseURL, t.ClientCertPEM, t.ClientKeyPEM, t.ClientKeyPassphrase, t.CACertPEM, t.ParameterContextID, t.ParameterName, t.ControllerServiceID)
	if err != nil {
		return fmt.Errorf("store: upserting nifi avro target %q: %w", t.ID, err)
	}
	return nil
}

// DeleteNiFiAvroTarget removes a target by ID - a no-op (not an error) if
// it doesn't exist.
func (s *Store) DeleteNiFiAvroTarget(id string) error {
	if _, err := s.db.Exec(`DELETE FROM nifi_avro_targets WHERE id = ?`, id); err != nil {
		return fmt.Errorf("store: deleting nifi avro target %q: %w", id, err)
	}
	return nil
}

// ListDruidTargets returns every configured Druid target, ordered by ID.
func (s *Store) ListDruidTargets() ([]DruidTarget, error) {
	rows, err := s.db.Query(`SELECT id, enabled, base_url, supervisor_name FROM druid_targets ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("store: listing druid targets: %w", err)
	}
	defer rows.Close()

	var out []DruidTarget
	for rows.Next() {
		var t DruidTarget
		if err := rows.Scan(&t.ID, &t.Enabled, &t.BaseURL, &t.SupervisorName); err != nil {
			return nil, fmt.Errorf("store: scanning druid target: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// GetDruidTarget returns one target by ID, or nil if it doesn't exist.
func (s *Store) GetDruidTarget(id string) (*DruidTarget, error) {
	var t DruidTarget
	err := s.db.QueryRow(`SELECT id, enabled, base_url, supervisor_name FROM druid_targets WHERE id = ?`, id).
		Scan(&t.ID, &t.Enabled, &t.BaseURL, &t.SupervisorName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: getting druid target %q: %w", id, err)
	}
	return &t, nil
}

// UpsertDruidTarget creates or replaces a target by ID.
func (s *Store) UpsertDruidTarget(t DruidTarget) error {
	_, err := s.db.Exec(`
INSERT INTO druid_targets (id, enabled, base_url, supervisor_name)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE enabled = VALUES(enabled), base_url = VALUES(base_url), supervisor_name = VALUES(supervisor_name)`,
		t.ID, t.Enabled, t.BaseURL, t.SupervisorName)
	if err != nil {
		return fmt.Errorf("store: upserting druid target %q: %w", t.ID, err)
	}
	return nil
}

// DeleteDruidTarget removes a target by ID - a no-op (not an error) if it
// doesn't exist.
func (s *Store) DeleteDruidTarget(id string) error {
	if _, err := s.db.Exec(`DELETE FROM druid_targets WHERE id = ?`, id); err != nil {
		return fmt.Errorf("store: deleting druid target %q: %w", id, err)
	}
	return nil
}

// KafkaOutputTarget is one named Kafka output sink - there can be
// multiple simultaneously, each independently enabled, in either Avro or
// JSON format. ID is a short slug (e.g. "legacy", "json-nifi") - both the
// primary key and the display name. Brokers is a comma-separated list.
// client_id is deliberately NOT stored per-target - it identifies the
// producer/host (this instance's own cfg.Kafka.ClientID), not the target.
type KafkaOutputTarget struct {
	ID      string
	Enabled bool
	Format  string // "avro" | "json"
	Topic   string
	Brokers string
}

// ListKafkaOutputTargets returns every configured Kafka output target,
// ordered by ID.
func (s *Store) ListKafkaOutputTargets() ([]KafkaOutputTarget, error) {
	rows, err := s.db.Query(`SELECT id, enabled, format, topic, brokers FROM kafka_output_targets ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("store: listing kafka output targets: %w", err)
	}
	defer rows.Close()

	var out []KafkaOutputTarget
	for rows.Next() {
		var t KafkaOutputTarget
		if err := rows.Scan(&t.ID, &t.Enabled, &t.Format, &t.Topic, &t.Brokers); err != nil {
			return nil, fmt.Errorf("store: scanning kafka output target: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// GetKafkaOutputTarget returns one target by ID, or nil if it doesn't exist.
func (s *Store) GetKafkaOutputTarget(id string) (*KafkaOutputTarget, error) {
	var t KafkaOutputTarget
	err := s.db.QueryRow(`SELECT id, enabled, format, topic, brokers FROM kafka_output_targets WHERE id = ?`, id).
		Scan(&t.ID, &t.Enabled, &t.Format, &t.Topic, &t.Brokers)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: getting kafka output target %q: %w", id, err)
	}
	return &t, nil
}

// UpsertKafkaOutputTarget creates or replaces a target by ID.
func (s *Store) UpsertKafkaOutputTarget(t KafkaOutputTarget) error {
	_, err := s.db.Exec(`
INSERT INTO kafka_output_targets (id, enabled, format, topic, brokers)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE enabled = VALUES(enabled), format = VALUES(format), topic = VALUES(topic), brokers = VALUES(brokers)`,
		t.ID, t.Enabled, t.Format, t.Topic, t.Brokers)
	if err != nil {
		return fmt.Errorf("store: upserting kafka output target %q: %w", t.ID, err)
	}
	return nil
}

// DeleteKafkaOutputTarget removes a target by ID - a no-op (not an error)
// if it doesn't exist.
func (s *Store) DeleteKafkaOutputTarget(id string) error {
	if _, err := s.db.Exec(`DELETE FROM kafka_output_targets WHERE id = ?`, id); err != nil {
		return fmt.Errorf("store: deleting kafka output target %q: %w", id, err)
	}
	return nil
}

// EnsureKafkaTargetsMigratedFromLegacyConfig is a ONE-TIME migration: if no
// Kafka output targets exist yet, converts the legacy static
// server.yaml/env-var kafka.{brokers,topic} config into a single target
// named "legacy" (format "avro", matching the only format the collector
// has ever actually produced) - a no-op once any target row exists. Since
// every dev instance shares one DB, whichever instance boots first creates
// this row; every other instance's call here is a no-op, so there's no
// collision even though d02/d03 currently point at the identical topic.
// The passed-in legacyBrokers/legacyTopic are this instance's own
// server.yaml values - NOT re-read from anywhere in the DB, since there is
// no DB-backed legacy Kafka config to read (unlike sync_settings).
func (s *Store) EnsureKafkaTargetsMigratedFromLegacyConfig(legacyBrokers []string, legacyTopic string) error {
	targets, err := s.ListKafkaOutputTargets()
	if err != nil {
		return fmt.Errorf("store: listing kafka output targets: %w", err)
	}
	if len(targets) > 0 || legacyTopic == "" {
		return nil
	}
	if err := s.UpsertKafkaOutputTarget(KafkaOutputTarget{
		ID:      "legacy",
		Enabled: true,
		Format:  "avro",
		Topic:   legacyTopic,
		Brokers: strings.Join(legacyBrokers, ","),
	}); err != nil {
		return fmt.Errorf("store: migrating legacy kafka config: %w", err)
	}
	return nil
}

// EnsureTargetsMigratedFromLegacySyncSettings is a ONE-TIME migration: if
// no NiFi/Druid targets exist yet AND the legacy single-target
// sync_settings row has meaningful config, converts it into the first
// named target ("nifi-legacy" / "druid-dev", matching what that legacy
// config actually pointed at) - a no-op once any target row exists,
// matching this codebase's EnsureXSeeded convention. The legacy
// sync_settings row and its Get/SetSyncSettings accessors are left alone
// (not deleted) - they're simply superseded once real targets exist.
func (s *Store) EnsureTargetsMigratedFromLegacySyncSettings() error {
	legacy, err := s.GetSyncSettings()
	if err != nil {
		return fmt.Errorf("store: loading legacy sync settings for migration: %w", err)
	}

	nifiTargets, err := s.ListNiFiAvroTargets()
	if err != nil {
		return fmt.Errorf("store: listing nifi avro targets: %w", err)
	}
	if len(nifiTargets) == 0 && legacy.NiFiBaseURL != "" {
		if err := s.UpsertNiFiAvroTarget(NiFiAvroTarget{
			ID:                  "nifi-legacy",
			Enabled:             legacy.NiFiEnabled,
			BaseURL:             legacy.NiFiBaseURL,
			ClientCertPEM:       legacy.NiFiClientCertPEM,
			ClientKeyPEM:        legacy.NiFiClientKeyPEM,
			CACertPEM:           legacy.NiFiCACertPEM,
			ParameterContextID:  legacy.NiFiParameterContextID,
			ParameterName:       legacy.NiFiParameterName,
			ControllerServiceID: legacy.NiFiControllerServiceID,
		}); err != nil {
			return fmt.Errorf("store: migrating legacy nifi settings: %w", err)
		}
	}

	druidTargets, err := s.ListDruidTargets()
	if err != nil {
		return fmt.Errorf("store: listing druid targets: %w", err)
	}
	if len(druidTargets) == 0 && legacy.DruidBaseURL != "" {
		if err := s.UpsertDruidTarget(DruidTarget{
			ID:             "druid-dev",
			Enabled:        legacy.DruidEnabled,
			BaseURL:        legacy.DruidBaseURL,
			SupervisorName: legacy.DruidSupervisorName,
		}); err != nil {
			return fmt.Errorf("store: migrating legacy druid settings: %w", err)
		}
	}
	return nil
}

// GetPublishedSnapshot returns the schema JSON and mapping config for
// whatever was last actually published - the read-only counterpart to
// PublishSnapshot, for callers (the config watchdog) that only need to
// check/apply the current published state on THIS instance, not publish a
// new one. Like GetPublishedSchemaJSON, reads the "published" snapshot,
// NOT the in-progress edit buffer.
func (s *Store) GetPublishedSnapshot(namespace, recordName string) (schemaJSON string, mappingCfg *mapping.Config, err error) {
	fields, rules, found, err := s.loadSnapshot()
	if err != nil {
		return "", nil, fmt.Errorf("store: loading published snapshot: %w", err)
	}
	if !found {
		return "", nil, fmt.Errorf("store: no published snapshot exists yet")
	}
	schemaJSON, err = buildSchemaJSON(fields, namespace, recordName)
	if err != nil {
		return "", nil, err
	}
	return schemaJSON, buildMappingConfig(fields, rules), nil
}

// GetPublishedSchemaJSON returns the Avro schema JSON for whatever was
// last actually published (via Publish, or seeded at startup) - NOT the
// in-progress edit buffer, which may differ if there are unpublished
// changes. Meant for external consumers (e.g. a public /schema HTTP
// endpoint) that need to know the schema actually live in Kafka right
// now, not what an admin might be mid-editing.
func (s *Store) GetPublishedSchemaJSON(namespace, recordName string) (string, error) {
	fields, _, found, err := s.loadSnapshot()
	if err != nil {
		return "", fmt.Errorf("store: loading published snapshot: %w", err)
	}
	if !found {
		return "", fmt.Errorf("store: no published snapshot exists yet")
	}
	return buildSchemaJSON(fields, namespace, recordName)
}

func (s *Store) loadSnapshot() (fields []SchemaField, rules []MappingRule, found bool, err error) {
	var fieldsJSON, rulesJSON string
	err = s.db.QueryRow(`SELECT schema_fields_json, mapping_rules_json FROM published_snapshot WHERE id = 1`).
		Scan(&fieldsJSON, &rulesJSON)
	if err == sql.ErrNoRows {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("store: loading snapshot: %w", err)
	}
	if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
		return nil, nil, false, fmt.Errorf("store: unmarshaling snapshot fields: %w", err)
	}
	if err := json.Unmarshal([]byte(rulesJSON), &rules); err != nil {
		return nil, nil, false, fmt.Errorf("store: unmarshaling snapshot rules: %w", err)
	}
	return fields, rules, true, nil
}

// HasUnpublishedChanges reports whether the current schema_fields/
// mapping_rules differ from the last snapshot (in content OR order - a
// reorder alone counts as a change). If nothing has ever been snapshotted,
// reports true whenever the store isn't empty (nothing to compare against
// yet, so anything present is "unpublished").
func (s *Store) HasUnpublishedChanges() (bool, error) {
	fields, err := s.ListSchemaFields()
	if err != nil {
		return false, err
	}
	rules, err := s.ListMappingRules()
	if err != nil {
		return false, err
	}
	snapFields, snapRules, found, err := s.loadSnapshot()
	if err != nil {
		return false, err
	}
	if !found {
		return len(fields) > 0, nil
	}

	curFieldsJSON, _ := json.Marshal(fields)
	curRulesJSON, _ := json.Marshal(rules)
	snapFieldsJSON, _ := json.Marshal(snapFields)
	snapRulesJSON, _ := json.Marshal(snapRules)
	return string(curFieldsJSON) != string(snapFieldsJSON) || string(curRulesJSON) != string(snapRulesJSON), nil
}

// Revert discards all edits/adds/deletes/reorders made since the last
// snapshot, restoring schema_fields and mapping_rules to match it exactly.
// Errors if nothing has ever been published/snapshotted.
func (s *Store) Revert() error {
	fields, rules, found, err := s.loadSnapshot()
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("store: nothing has been published yet, nothing to revert to")
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("store: beginning transaction: %w", err)
	}
	defer tx.Rollback()
	if err := replaceAll(tx, fields, rules); err != nil {
		return err
	}
	return tx.Commit()
}

// replaceAll wipes schema_fields/mapping_rules and reinserts fields/rules
// exactly, within tx.
func replaceAll(tx *sql.Tx, fields []SchemaField, rules []MappingRule) error {
	if _, err := tx.Exec(`DELETE FROM mapping_rules`); err != nil {
		return fmt.Errorf("store: clearing mapping rules: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM schema_fields`); err != nil {
		return fmt.Errorf("store: clearing schema fields: %w", err)
	}
	for _, f := range fields {
		if _, err := tx.Exec(`INSERT INTO schema_fields (name, type_json, has_default, default_json, position) VALUES (?, ?, ?, ?, ?)`,
			f.Name, f.TypeJSON, f.HasDefault, f.DefaultJSON, f.Position); err != nil {
			return fmt.Errorf("store: restoring field %q: %w", f.Name, err)
		}
	}
	for _, r := range rules {
		if _, err := tx.Exec(`INSERT INTO mapping_rules (field_name, builtin, event_param, event_param_path, coerce, has_default, default_value) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			r.Field, r.Builtin, r.EventParam, r.EventParamPath, r.Coerce, r.HasDefault, r.Default); err != nil {
			return fmt.Errorf("store: restoring mapping rule for %q: %w", r.Field, err)
		}
	}
	return nil
}
