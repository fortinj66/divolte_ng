// Package config loads the Go server's bootstrap configuration: listen
// address, source routing (prefix/script name/event suffix), the schema
// and mapping file paths, and Kafka sink settings. Deliberately a small,
// flat YAML file rather than a HOCON port of divolte-collector.conf - the
// wire protocol and schema/mapping content are what need to stay
// compatible, not the bootstrap config format itself.
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type KafkaConfig struct {
	Brokers  []string `yaml:"brokers"`
	ClientID string   `yaml:"client_id"`
	Topic    string   `yaml:"topic"`
}

// DBConfig connects to the shared MariaDB/MySQL database that backs
// internal/store - every Divolte instance (across d01/d02/d03, and
// eventually p01-p03) points at the SAME database, so there is exactly one
// copy of the schema/mapping config rather than one independent copy per
// instance. This replaces the earlier per-instance-SQLite-plus-HTTP-push
// design entirely: since every instance now reads and writes the same
// rows, there's no more need for a designated "primary" instance to push
// changes out to "replicas".
//
// Each instance still loads its own in-memory mapping/schema copy (the
// actual thing used to encode events) once at startup - publishing from
// one instance hot-swaps that instance's own in-memory copy immediately,
// but another instance only picks up the change on its own next restart.
// See docs/admin-ui-guide.md for the operational implications.
type DBConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Name     string `yaml:"name"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// LDAPConfig configures optional LDAP/Active Directory authentication for
// the admin UI (internal/ldapauth), as an ADDITIONAL way in alongside the
// shared username/password stored in the database (internal/store's
// AdminSettings) - either credential set works. Disabled unless Enabled
// is true. This UI can edit live schema/mapping and publish changes, so
// LDAP auth here always requires AllowedGroups to be non-empty - it never
// grants access to "anyone who can log into the domain" the way some
// looser LDAP integrations do (compare NiFi's own login-identity-
// providers.xml in this environment, which has no such restriction).
type LDAPConfig struct {
	Enabled bool `yaml:"enabled"`

	// Servers are tried in order until one connects, e.g.
	// "ldap://ldap1.example.com".
	Servers []string `yaml:"servers"`

	// ManagerDN/ManagerPassword bind to search for the user's DN before
	// the real authentication bind - a service account, not the end
	// user's own credentials. ManagerPassword is normally supplied via
	// EnvLDAPBindPassword instead of sitting in the YAML file.
	ManagerDN       string `yaml:"manager_dn"`
	ManagerPassword string `yaml:"manager_password"`

	// UserSearchBase/UserSearchFilter locate the user's DN - "{0}" in the
	// filter is replaced with the submitted username, e.g. base
	// "DC=example,DC=com", filter "sAMAccountName={0}".
	UserSearchBase   string `yaml:"user_search_base"`
	UserSearchFilter string `yaml:"user_search_filter"`

	// AllowedGroups: the submitted user must belong to at least one of
	// these AD groups (nested/indirect membership included) to
	// authenticate - required to be non-empty when Enabled is true. Each
	// entry may be a full distinguished name or a bare group name (e.g.
	// "admins"), resolved to its DN via UserSearchBase at
	// authentication time.
	AllowedGroups []string `yaml:"allowed_groups"`

	ConnectTimeoutSeconds int `yaml:"connect_timeout_seconds"`
	ReadTimeoutSeconds    int `yaml:"read_timeout_seconds"`
}

// AdminConfig configures the schema/mapping web UI (internal/adminui) - new
// functionality versus the original Java server.
type AdminConfig struct {
	ListenAddr       string     `yaml:"listen_addr"`
	DB               DBConfig   `yaml:"db"`
	Username         string     `yaml:"username"`
	Password         string     `yaml:"password"`
	SchemaNamespace  string     `yaml:"schema_namespace"`
	SchemaRecordName string     `yaml:"schema_record_name"`
	LDAP             LDAPConfig `yaml:"ldap"`

	// URIPrefix lets the admin UI be reached under a path prefix (e.g.
	// "/admin") behind a reverse proxy that strips the prefix before
	// forwarding to this server - see internal/adminui's Config.URIPrefix
	// doc comment for the full explanation. Empty (the default) means
	// root-mounted, matching the original behavior. Must not have a
	// trailing slash.
	URIPrefix string `yaml:"uri_prefix"`
}

type Config struct {
	ListenAddr  string `yaml:"listen_addr"`
	Prefix      string `yaml:"prefix"`
	ScriptName  string `yaml:"script_name"`
	EventSuffix string `yaml:"event_suffix"`

	SchemaFile  string `yaml:"schema_file"`
	MappingFile string `yaml:"mapping_file"`

	// StaticOverrideDir, if set, is checked at startup/request time for
	// files that take precedence over the compiled-in go:embed defaults -
	// {StaticOverrideDir}/{ScriptName} overrides the tracking JS body
	// (internal/httpserver), and {StaticOverrideDir}/logo.svg and
	// {StaticOverrideDir}/favicon.ico override the admin UI's branding
	// (internal/adminui). Lets a deployment mount its own tracking script
	// and logo/favicon onto an unmodified image, rather than needing its
	// own fork/build. Empty (the default) disables all three overrides,
	// matching every existing deployment's behavior exactly.
	StaticOverrideDir string `yaml:"static_override_dir"`

	Kafka KafkaConfig `yaml:"kafka"`
	Admin AdminConfig `yaml:"admin"`

	Workers             int `yaml:"workers"`
	QueueSize           int `yaml:"queue_size"`
	DuplicateMemorySize int `yaml:"duplicate_memory_size"`

	// ShutdownDelaySeconds is how long /ping keeps failing before the
	// server actually starts draining connections on shutdown - gives a
	// load balancer time to notice and stop routing first, matching
	// legacy's shutdownDelay (Server.java: pingHandler.shutdown() flips the
	// health check, then a sleep, before the real drain begins).
	ShutdownDelaySeconds int `yaml:"shutdown_delay_seconds"`

	// ConfigWatchdogIntervalSeconds is how often this instance polls the
	// shared DB for a schema/mapping change published (from any instance,
	// though in practice only ever the admin UI's designated primary) or a
	// Kafka output target change, and applies it live with no restart -
	// otherwise only the instance that actually handled the admin edit
	// picks it up, and every other instance shares the DB row but keeps
	// serving with whatever it loaded at its own boot indefinitely.
	ConfigWatchdogIntervalSeconds int `yaml:"config_watchdog_interval_seconds"`
}

// Env var names matching production's overrides for the Kafka producer
// (see divolte-collector.conf: bootstrap.servers = ${?DIVOLTE_KAFKA_BROKER_LIST},
// client.id = ${?DIVOLTE_KAFKA_CLIENT_ID}), plus ones for the admin UI and
// database passwords so neither needs to sit in plaintext in the config
// file.
const (
	EnvKafkaBrokerList  = "DIVOLTE_KAFKA_BROKER_LIST"
	EnvKafkaClientID    = "DIVOLTE_KAFKA_CLIENT_ID"
	EnvAdminPassword    = "DIVOLTE_ADMIN_PASSWORD"
	EnvDBPassword       = "DIVOLTE_DB_PASSWORD"
	EnvLDAPBindPassword = "DIVOLTE_LDAP_BIND_PASSWORD"
)

func defaults() Config {
	return Config{
		ListenAddr:                    ":8290",
		Prefix:                        "/webstats/",
		ScriptName:                    "divolte_ng.js",
		EventSuffix:                   "csc-event",
		Workers:                       4,
		QueueSize:                     10_000,
		DuplicateMemorySize:           1_000_000,
		ShutdownDelaySeconds:          5,
		ConfigWatchdogIntervalSeconds: 30,
		Admin: AdminConfig{
			ListenAddr: ":8291",
			DB: DBConfig{
				Port: 3306,
			},
			Username:         "admin",
			SchemaNamespace:  "com.example.divolte.record",
			SchemaRecordName: "example_event",
			LDAP: LDAPConfig{
				ConnectTimeoutSeconds: 10,
				ReadTimeoutSeconds:    10,
			},
		},
	}
}

// Load reads a YAML config file and applies env var overrides on top
// (matching production's ${?DIVOLTE_KAFKA_BROKER_LIST} / ${?DIVOLTE_KAFKA_CLIENT_ID}
// substitution behavior).
func Load(path string) (*Config, error) {
	cfg := defaults()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parsing %s: %w", path, err)
	}

	if v := os.Getenv(EnvKafkaBrokerList); v != "" {
		cfg.Kafka.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv(EnvKafkaClientID); v != "" {
		cfg.Kafka.ClientID = v
	}
	if v := os.Getenv(EnvAdminPassword); v != "" {
		cfg.Admin.Password = v
	}
	if v := os.Getenv(EnvDBPassword); v != "" {
		cfg.Admin.DB.Password = v
	}
	if v := os.Getenv(EnvLDAPBindPassword); v != "" {
		cfg.Admin.LDAP.ManagerPassword = v
	}

	if cfg.SchemaFile == "" {
		return nil, fmt.Errorf("config: schema_file is required")
	}
	if cfg.MappingFile == "" {
		return nil, fmt.Errorf("config: mapping_file is required")
	}
	if len(cfg.Kafka.Brokers) == 0 {
		return nil, fmt.Errorf("config: kafka.brokers is required (or set %s)", EnvKafkaBrokerList)
	}
	if cfg.Kafka.Topic == "" {
		return nil, fmt.Errorf("config: kafka.topic is required")
	}
	if cfg.Admin.Password == "" {
		return nil, fmt.Errorf("config: admin.password is required (or set %s)", EnvAdminPassword)
	}
	if cfg.Workers < 1 {
		return nil, fmt.Errorf("config: workers must be >= 1, got %d", cfg.Workers)
	}
	if cfg.QueueSize < 1 {
		return nil, fmt.Errorf("config: queue_size must be >= 1, got %d", cfg.QueueSize)
	}
	if cfg.DuplicateMemorySize < 0 {
		return nil, fmt.Errorf("config: duplicate_memory_size must be >= 0, got %d", cfg.DuplicateMemorySize)
	}
	if cfg.Admin.DB.Host == "" {
		return nil, fmt.Errorf("config: admin.db.host is required")
	}
	if cfg.Admin.DB.Name == "" {
		return nil, fmt.Errorf("config: admin.db.name is required")
	}
	if cfg.Admin.DB.Username == "" {
		return nil, fmt.Errorf("config: admin.db.username is required")
	}
	if cfg.Admin.DB.Password == "" {
		return nil, fmt.Errorf("config: admin.db.password is required (or set %s)", EnvDBPassword)
	}
	if cfg.Admin.LDAP.Enabled {
		if len(cfg.Admin.LDAP.Servers) == 0 {
			return nil, fmt.Errorf("config: admin.ldap.servers is required when admin.ldap.enabled is true")
		}
		if cfg.Admin.LDAP.UserSearchBase == "" {
			return nil, fmt.Errorf("config: admin.ldap.user_search_base is required when admin.ldap.enabled is true")
		}
		if cfg.Admin.LDAP.UserSearchFilter == "" {
			return nil, fmt.Errorf("config: admin.ldap.user_search_filter is required when admin.ldap.enabled is true")
		}
		if len(cfg.Admin.LDAP.AllowedGroups) == 0 {
			return nil, fmt.Errorf("config: admin.ldap.allowed_groups must be non-empty when admin.ldap.enabled is true - LDAP auth here always requires group membership")
		}
		if cfg.Admin.LDAP.ManagerDN == "" {
			return nil, fmt.Errorf("config: admin.ldap.manager_dn is required when admin.ldap.enabled is true")
		}
		if cfg.Admin.LDAP.ManagerPassword == "" {
			return nil, fmt.Errorf("config: admin.ldap.manager_password is required when admin.ldap.enabled is true (or set %s)", EnvLDAPBindPassword)
		}
	}
	return &cfg, nil
}
