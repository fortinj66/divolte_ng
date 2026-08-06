// Command divolte-collector is the Go rewrite of divolte-collector's
// browser event ingestion server: serves the tracking tag, accepts the
// event beacon, maps events per a schema/mapping config, encodes them as
// Avro, and publishes to Kafka. A second HTTP listener serves the admin UI
// for editing that schema/mapping config and hot-reloading it live.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/example/divolte-rewrite/internal/adminui"
	"github.com/example/divolte-rewrite/internal/avroenc"
	"github.com/example/divolte-rewrite/internal/config"
	"github.com/example/divolte-rewrite/internal/druid"
	"github.com/example/divolte-rewrite/internal/httpserver"
	"github.com/example/divolte-rewrite/internal/kafkasink"
	"github.com/example/divolte-rewrite/internal/ldapauth"
	"github.com/example/divolte-rewrite/internal/mapping"
	"github.com/example/divolte-rewrite/internal/nifi"
	"github.com/example/divolte-rewrite/internal/nifiavro"
	"github.com/example/divolte-rewrite/internal/store"
	"github.com/example/divolte-rewrite/internal/syncplugin"
)

// kafkaTargetSpecs converts enabled store rows into kafkasink.TargetSpecs -
// kafkasink deliberately doesn't import internal/store, matching how
// internal/nifiavro/internal/druid take their own Config types.
func kafkaTargetSpecs(targets []store.KafkaOutputTarget) []kafkasink.TargetSpec {
	var specs []kafkasink.TargetSpec
	for _, t := range targets {
		if !t.Enabled {
			continue
		}
		specs = append(specs, kafkasink.TargetSpec{
			ID:      t.ID,
			Format:  t.Format,
			Topic:   t.Topic,
			Brokers: splitBrokers(t.Brokers),
		})
	}
	return specs
}

// splitBrokers parses a comma-separated broker list, trimming whitespace
// and dropping empty entries.
func splitBrokers(brokers string) []string {
	var out []string
	for _, b := range strings.Split(brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// kafkaTargetsSummary renders the enabled targets for the startup log line.
func kafkaTargetsSummary(targets []store.KafkaOutputTarget) string {
	var parts []string
	for _, t := range targets {
		if !t.Enabled {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s(%s)", t.ID, t.Topic, t.Format))
	}
	return strings.Join(parts, ",")
}

func main() {
	configPath := flag.String("config", "configs/server.yaml", "path to the server bootstrap config YAML")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("loading config: %v", err)
	}

	db, err := store.Open(store.Config{
		Host:     cfg.Admin.DB.Host,
		Port:     cfg.Admin.DB.Port,
		Name:     cfg.Admin.DB.Name,
		Username: cfg.Admin.DB.Username,
		Password: cfg.Admin.DB.Password,
	})
	if err != nil {
		log.Fatalf("opening schema/mapping store at %s:%d/%s: %v", cfg.Admin.DB.Host, cfg.Admin.DB.Port, cfg.Admin.DB.Name, err)
	}
	defer db.Close()

	empty, err := db.IsEmpty()
	if err != nil {
		log.Fatalf("checking store: %v", err)
	}
	if empty {
		log.Printf("store is empty, seeding from %s and %s", cfg.SchemaFile, cfg.MappingFile)
		if err := db.SeedFromFiles(cfg.SchemaFile, cfg.MappingFile); err != nil {
			log.Fatalf("seeding store: %v", err)
		}
	}

	// Seeds the shared admin login (once) from this instance's own
	// server.yaml/env-var values - a no-op on every instance after the
	// first one to reach here, since the database row already exists by
	// then. From here on the database, not any instance's own config, is
	// the source of truth for the admin login (see internal/store's
	// AdminSettings) - this is what lets d01/d02/d03 share one login
	// instead of each keeping its own independent DIVOLTE_ADMIN_PASSWORD.
	if err := db.EnsureAdminSettingsSeeded(cfg.Admin.Username, cfg.Admin.Password); err != nil {
		log.Fatalf("seeding admin settings: %v", err)
	}

	// The store (not the original files) is the live source of truth from
	// here on - the admin UI's "Publish" action re-derives from the store
	// the same way this initial load does.
	schemaJSON, err := db.BuildSchemaJSON(cfg.Admin.SchemaNamespace, cfg.Admin.SchemaRecordName)
	if err != nil {
		log.Fatalf("building schema from store: %v", err)
	}
	codec, err := avroenc.LoadSchema(schemaJSON)
	if err != nil {
		log.Fatalf("loading avro schema: %v", err)
	}
	mappingCfg, err := db.BuildMappingConfig()
	if err != nil {
		log.Fatalf("building mapping config from store: %v", err)
	}
	log.Printf("loaded %d field rules from %s:%d/%s", len(mappingCfg.Fields), cfg.Admin.DB.Host, cfg.Admin.DB.Port, cfg.Admin.DB.Name)

	// Fail fast on a mapping rule that targets a field the schema doesn't
	// actually declare, matching legacy's startup-time config validation
	// (ValidatedConfiguration + config/constraint/*.java, e.g.
	// MappingSourceSinkReferencesMustExist) rather than silently producing
	// records that don't match this mapping at runtime.
	if missing := unknownMappingFields(mappingCfg, codec); len(missing) > 0 {
		log.Fatalf("config: mapping references %d field(s) not in the schema: %v", len(missing), missing)
	}

	// Whatever the store contains at boot is what's about to go live -
	// snapshot it now so "revert to last published" has a correct baseline
	// even before anyone clicks Publish through the admin UI.
	if err := db.SaveSnapshot(); err != nil {
		log.Fatalf("saving initial published snapshot: %v", err)
	}

	// One-time migration: converts the legacy static server.yaml
	// kafka.{brokers,topic} config into a single DB-backed target named
	// "legacy" (format "avro") - a no-op after the first instance to
	// reach here does it, since d02/d03 share one DB and already point
	// at the same real topic. See EnsureKafkaTargetsMigratedFromLegacyConfig's
	// doc comment.
	if err := db.EnsureKafkaTargetsMigratedFromLegacyConfig(cfg.Kafka.Brokers, cfg.Kafka.Topic); err != nil {
		log.Fatalf("migrating legacy kafka config: %v", err)
	}
	kafkaTargets, err := db.ListKafkaOutputTargets()
	if err != nil {
		log.Fatalf("loading kafka output targets: %v", err)
	}
	kafkaMgr := kafkasink.NewManager(cfg.Kafka.ClientID)
	if err := kafkaMgr.Reconcile(kafkaTargetSpecs(kafkaTargets)); err != nil {
		log.Fatalf("connecting kafka output targets: %v", err)
	}

	// kafkaReconcile reads the current target list fresh on every call
	// (not cached) - called from every /kafka-targets create/update/
	// delete/toggle so a saved change takes effect on the very next
	// event, without a restart. Reconcile itself leaves an unchanged
	// target's live producer untouched, so editing one target never
	// bounces another's connection - see Manager.Reconcile's doc comment.
	kafkaReconcile := func() error {
		targets, err := db.ListKafkaOutputTargets()
		if err != nil {
			return fmt.Errorf("loading kafka output targets: %w", err)
		}
		return kafkaMgr.Reconcile(kafkaTargetSpecs(targets))
	}

	srv, handler := httpserver.New(httpserver.Config{
		Prefix:              cfg.Prefix,
		ScriptName:          cfg.ScriptName,
		EventSuffix:         cfg.EventSuffix,
		MappingCfg:          mappingCfg,
		Codec:               codec,
		Sink:                kafkaMgr,
		Workers:             cfg.Workers,
		QueueSize:           cfg.QueueSize,
		DuplicateMemorySize: cfg.DuplicateMemorySize,
	})

	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Seeds the shared LDAP config (once) from this instance's own
	// server.yaml admin.ldap values - a no-op on every instance after the
	// first one to reach here, same pattern as the admin login seed
	// above. From here on the database, editable via /settings, is
	// authoritative - not any instance's own config file, and not even
	// this seed call's values on subsequent runs.
	if err := db.EnsureLDAPSettingsSeeded(store.LDAPSettings{
		Enabled:          cfg.Admin.LDAP.Enabled,
		Servers:          cfg.Admin.LDAP.Servers,
		ManagerDN:        cfg.Admin.LDAP.ManagerDN,
		ManagerPassword:  cfg.Admin.LDAP.ManagerPassword,
		UserSearchBase:   cfg.Admin.LDAP.UserSearchBase,
		UserSearchFilter: cfg.Admin.LDAP.UserSearchFilter,
		AllowedGroups:    cfg.Admin.LDAP.AllowedGroups,
	}); err != nil {
		log.Fatalf("seeding ldap settings: %v", err)
	}

	// Seeds the shared NiFi/Druid sync config row (once) - both start
	// disabled until explicitly configured via /settings, matching
	// EnsureSyncSettingsSeeded's own doc comment.
	if err := db.EnsureSyncSettingsSeeded(); err != nil {
		log.Fatalf("seeding sync settings: %v", err)
	}

	// One-time migration: converts the legacy single-target sync_settings
	// row (if populated) into the first named NiFi/Druid target, so an
	// existing single-cluster setup keeps working once multi-target
	// support exists - a no-op after the first instance to reach here
	// does it. See EnsureTargetsMigratedFromLegacySyncSettings's doc
	// comment.
	if err := db.EnsureTargetsMigratedFromLegacySyncSettings(); err != nil {
		log.Fatalf("migrating legacy sync settings to targets: %v", err)
	}

	// LDAP authentication is an additional way in alongside the shared
	// database login - always wired up (not conditional on Enabled),
	// since whether it's actually active now lives in the database
	// (editable via /settings) and is re-checked on every login attempt,
	// not fixed at process startup. See internal/ldapauth's package doc
	// for why this always requires group membership.
	ldapAuth := ldapauth.NewDynamic(func() (ldapauth.Config, bool, error) {
		settings, err := db.GetLDAPSettings()
		if err != nil {
			return ldapauth.Config{}, false, err
		}
		if !settings.Enabled {
			return ldapauth.Config{}, false, nil
		}
		return ldapauth.Config{
			Servers:          settings.Servers,
			ManagerDN:        settings.ManagerDN,
			ManagerPassword:  settings.ManagerPassword,
			UserSearchBase:   settings.UserSearchBase,
			UserSearchFilter: settings.UserSearchFilter,
			AllowedGroups:    settings.AllowedGroups,
			ConnectTimeout:   time.Duration(cfg.Admin.LDAP.ConnectTimeoutSeconds) * time.Second,
			ReadTimeout:      time.Duration(cfg.Admin.LDAP.ReadTimeoutSeconds) * time.Second,
		}, true, nil
	})

	ldapConnectTimeout := time.Duration(cfg.Admin.LDAP.ConnectTimeoutSeconds) * time.Second
	ldapReadTimeout := time.Duration(cfg.Admin.LDAP.ReadTimeoutSeconds) * time.Second

	// publishSync reads the current target lists fresh on every call (not
	// built once at startup) - same reasoning as the LDAP dynamic
	// authenticator: an edit made through /nifi-targets or /druid-targets
	// takes effect on the very next Publish, without a restart. Assembles
	// one plugin instance per ENABLED target (there can be more than one
	// of each kind - e.g. nifi-01 and nifi-02 are separate clusters)
	// and runs all of them via syncplugin.RunAll - a future plugin type
	// is added here the same way, not as a special case elsewhere.
	publishSync := func(schemaJSON string, fields []adminui.PublishSyncField) (string, error) {
		nifiTargets, err := db.ListNiFiAvroTargets()
		if err != nil {
			return "", fmt.Errorf("loading nifi targets: %w", err)
		}
		druidTargets, err := db.ListDruidTargets()
		if err != nil {
			return "", fmt.Errorf("loading druid targets: %w", err)
		}

		var plugins []syncplugin.Plugin
		for _, t := range nifiTargets {
			if !t.Enabled {
				continue
			}
			p, err := nifiavro.New(nifiavro.Config{
				DisplayName: t.ID,
				NiFi: nifi.Config{
					BaseURL:             t.BaseURL,
					ClientCertPEM:       t.ClientCertPEM,
					ClientKeyPEM:        t.ClientKeyPEM,
					ClientKeyPassphrase: t.ClientKeyPassphrase,
					CACertPEM:           t.CACertPEM,
				},
				ParameterContextID:  t.ParameterContextID,
				ParameterName:       t.ParameterName,
				ControllerServiceID: t.ControllerServiceID,
			})
			if err != nil {
				return "", fmt.Errorf("configuring nifi target %q: %w", t.ID, err)
			}
			plugins = append(plugins, p)
		}
		for _, t := range druidTargets {
			if !t.Enabled {
				continue
			}
			p, err := druid.NewPlugin(druid.Config{
				DisplayName:    t.ID,
				BaseURL:        t.BaseURL,
				SupervisorName: t.SupervisorName,
			})
			if err != nil {
				return "", fmt.Errorf("configuring druid target %q: %w", t.ID, err)
			}
			plugins = append(plugins, p)
		}

		pluginFields := make([]syncplugin.Field, len(fields))
		for i, f := range fields {
			pluginFields[i] = syncplugin.Field{Name: f.Name, TypeJSON: f.TypeJSON}
		}
		return syncplugin.RunAll(plugins, schemaJSON, pluginFields)
	}

	adminHandler, err := adminui.New(adminui.Config{
		Store:            db,
		Publisher:        srv,
		SchemaNamespace:  cfg.Admin.SchemaNamespace,
		SchemaRecordName: cfg.Admin.SchemaRecordName,
		URIPrefix:        cfg.Admin.URIPrefix,
		LDAPAuth:         ldapAuth,
		LDAPTest: func(servers []string, managerDN, managerPassword, userSearchBase, userSearchFilter string, allowedGroups []string) (string, error) {
			return ldapauth.TestConnection(ldapauth.Config{
				Servers:          servers,
				ManagerDN:        managerDN,
				ManagerPassword:  managerPassword,
				UserSearchBase:   userSearchBase,
				UserSearchFilter: userSearchFilter,
				AllowedGroups:    allowedGroups,
				ConnectTimeout:   ldapConnectTimeout,
				ReadTimeout:      ldapReadTimeout,
			})
		},
		PublishSync: publishSync,
		NiFiTest: func(v map[string]string) (string, error) {
			return nifiavro.TestConnection(nifiavro.Config{
				NiFi: nifi.Config{
					BaseURL:             v["base_url"],
					ClientCertPEM:       v["client_cert"],
					ClientKeyPEM:        v["client_key"],
					ClientKeyPassphrase: v["client_key_passphrase"],
					CACertPEM:           v["ca_cert"],
				},
				ParameterContextID:  v["parameter_context_id"],
				ParameterName:       v["parameter_name"],
				ControllerServiceID: v["controller_service_id"],
			})
		},
		DruidTest: func(v map[string]string) (string, error) {
			return druid.TestConnection(druid.Config{
				BaseURL:        v["base_url"],
				SupervisorName: v["supervisor_name"],
			})
		},
		KafkaReconcile: kafkaReconcile,
		KafkaTest: func(v map[string]string) (string, error) {
			return kafkasink.TestConnection(splitBrokers(v["brokers"]), v["topic"])
		},
	})
	if err != nil {
		log.Fatalf("building admin UI: %v", err)
	}
	adminSrv := &http.Server{
		Addr:              cfg.Admin.ListenAddr,
		Handler:           adminHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("listening on %s (tag: %s%s, event beacon: %s%s, kafka targets: %s)",
			cfg.ListenAddr, cfg.Prefix, cfg.ScriptName, cfg.Prefix, cfg.EventSuffix, kafkaTargetsSummary(kafkaTargets))
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()
	go func() {
		log.Printf("admin UI listening on %s", cfg.Admin.ListenAddr)
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("admin http server: %v", err)
		}
	}()

	watchdogStop := make(chan struct{})
	go runConfigWatchdog(db, srv, kafkaMgr, cfg, watchdogStop)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")
	close(watchdogStop)

	// Fail /ping first and give the load balancer's health-check interval
	// a chance to notice and stop routing here, THEN stop accepting new
	// HTTP connections, THEN drain the processing pool and close the Kafka
	// sink - "stop upstream before downstream", matching the original's
	// full shutdown ordering (Server.java's pingHandler.shutdown() + sleep
	// + drain), not just the last two steps.
	srv.PrepareShutdown()
	shutdownDelay := time.Duration(cfg.ShutdownDelaySeconds) * time.Second
	log.Printf("failing health checks for %s before draining...", shutdownDelay)
	time.Sleep(shutdownDelay)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server shutdown: %v", err)
	}
	if err := adminSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("admin http server shutdown: %v", err)
	}
	if err := srv.Close(shutdownCtx); err != nil {
		log.Printf("draining processing pool: %v", err)
	}
	log.Println("shutdown complete")
}

// snapshotSignature returns a comparable string for a schema+mapping pair,
// so the config watchdog can cheaply detect "did anything change" without
// re-parsing/re-validating on every tick when nothing did. Comparing
// schemaJSON alone would miss a mapping-only edit that doesn't add/remove a
// field (e.g. changing which event_param feeds an existing field), so the
// signature covers both halves.
func snapshotSignature(schemaJSON string, mappingCfg *mapping.Config) (string, error) {
	mappingJSON, err := json.Marshal(mappingCfg)
	if err != nil {
		return "", fmt.Errorf("marshaling mapping config: %w", err)
	}
	return schemaJSON + "\x00" + string(mappingJSON), nil
}

// runConfigWatchdog polls the shared DB every cfg.ConfigWatchdogIntervalSeconds
// for a published schema/mapping change or a Kafka output target change and
// applies it live via srv.Publish / kafkaMgr.Reconcile - with no restart.
// Without this, only the ONE instance that actually handled a given admin
// edit (in practice always the primary, since primaryRedirect funnels every
// admin action through it) picks up the change; every sibling instance
// shares the same DB row but keeps serving whatever it loaded at its own
// boot indefinitely. Runs on every instance identically - a harmless no-op
// tick on whichever instance happens to be the one edits are made through.
func runConfigWatchdog(db *store.Store, srv *httpserver.Server, kafkaMgr *kafkasink.Manager, cfg *config.Config, stop <-chan struct{}) {
	interval := time.Duration(cfg.ConfigWatchdogIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Seed lastSignature from what's already live at boot, so the first
	// tick doesn't treat the server's own just-loaded config as "new".
	var lastSignature string
	if schemaJSON, mappingCfg, err := db.GetPublishedSnapshot(cfg.Admin.SchemaNamespace, cfg.Admin.SchemaRecordName); err == nil {
		lastSignature, _ = snapshotSignature(schemaJSON, mappingCfg)
	}

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			schemaJSON, mappingCfg, err := db.GetPublishedSnapshot(cfg.Admin.SchemaNamespace, cfg.Admin.SchemaRecordName)
			if err != nil {
				log.Printf("config watchdog: loading published snapshot: %v", err)
			} else if sig, err := snapshotSignature(schemaJSON, mappingCfg); err != nil {
				log.Printf("config watchdog: computing signature: %v", err)
			} else if sig != lastSignature {
				if codec, err := avroenc.LoadSchema(schemaJSON); err != nil {
					log.Printf("config watchdog: parsing published schema, not applying: %v", err)
				} else if missing := unknownMappingFields(mappingCfg, codec); len(missing) > 0 {
					log.Printf("config watchdog: published mapping references unknown field(s) %v, not applying", missing)
				} else {
					srv.Publish(mappingCfg, codec)
					lastSignature = sig
					log.Printf("config watchdog: picked up a schema/mapping change")
				}
			}

			if targets, err := db.ListKafkaOutputTargets(); err != nil {
				log.Printf("config watchdog: listing kafka output targets: %v", err)
			} else if err := kafkaMgr.Reconcile(kafkaTargetSpecs(targets)); err != nil {
				log.Printf("config watchdog: reconciling kafka targets: %v", err)
			}
		}
	}
}

// unknownMappingFields returns the mapping rule field names that codec's
// schema doesn't declare, if any.
func unknownMappingFields(mappingCfg *mapping.Config, codec *avroenc.Codec) []string {
	var missing []string
	for _, rule := range mappingCfg.Fields {
		if !codec.HasField(rule.Field) {
			missing = append(missing, rule.Field)
		}
	}
	return missing
}
