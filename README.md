# Divolte Collector (Go rewrite)

A from-scratch Go rewrite of [Divolte Collector](https://github.com/divolte/divolte-collector)
(archived since 2022), a clickstream ingestion server for a web tracking
pixel. Speaks the same wire protocol as the original Java/Undertow server,
encodes to the same Avro schema, and publishes to the same kind of Kafka
topic - existing NiFi/Druid consumers downstream don't need to change.

For day-to-day schema/mapping/output editing, see
**[docs/admin-ui-guide.md](docs/admin-ui-guide.md)**. For how to actually
instrument a website with the tracking tag, see
**[docs/tracking-tag.md](docs/tracking-tag.md)**. For a side-by-side
comparison against the original Java server, see
**[docs/legacy-vs-nextgen.md](docs/legacy-vs-nextgen.md)**. This document
covers architecture, deployment, and the supporting tooling.

## What it does

A browser loads the tracking tag (`divolte_ng.js`) and fires a 1x1 GIF beacon
request (query-string-encoded event data, including a compact custom
binary encoding called "mincode" for arbitrary event parameters) for every
page view/interaction. The server:

1. Serves the cached GIF/304 response immediately - this never blocks on
   anything below.
2. Off the response path, parses the beacon and hands it to a worker pool
   keyed by party ID (so one visitor's events always process on the same
   worker, in order).
3. Evaluates a declarative YAML mapping against the event, producing a
   flat `map[string]interface{}`.
4. Encodes that as **naked Avro** (no Confluent wire-format header) against
   the current schema - or as plain JSON, or both, depending on which
   Kafka output targets are currently enabled (see below).
5. Publishes it to one or more Kafka targets, keyed by party ID.

## Repo layout

```
cmd/
  divolte-collector/  the production server (beacon + admin UI)
  paritycheck/        diffs legacy vs Go rewrite output field-by-field
  smoketest/          fires one event end-to-end, verifies it lands in Kafka
  topicwatch/         read-only Kafka topic inspector (tail/stats/compare)
  trafficmirror/      transparent tee proxy for shadow-testing against real traffic

internal/
  event/       beacon wire-protocol parsing, checksum, base36, murmur3
  mincode/     the custom "u" param encoding (arbitrary event parameters)
  mapping/     the declarative schema-mapping engine (see below)
  avroenc/     naked Avro encoding, Jackson-compatible coercion quirks
  jsonenc/     plain JSON encoding of the same mapped fields (sibling to avroenc)
  kafkasink/   Kafka publish - Manager fans a mapped event out to N
               independently configured targets (Avro and/or JSON), hot-
               swappable with no restart, app-level retry/backoff per target
  pool/        the affinity-routed worker pool
  httpserver/  the beacon HTTP handler
  store/       MariaDB/MySQL-backed schema/mapping/sync-target storage,
               shared across every instance
  adminui/     the schema/mapping/output editor web UI (see the admin guide)
  nifi/        generic NiFi REST/lifecycle mechanics (mTLS, Parameter Context updates)
  nifiavro/    the NiFi schema-sync plugin built on internal/nifi
  druid/       the Druid Kafka-supervisor dimensionsSpec-sync plugin
  syncplugin/  the Plugin interface + RunAll fan-out for the sync plugins above
  ldapauth/    optional Active Directory login for the admin UI
  config/      bootstrap YAML config loading
  uaparse/     User-Agent parsing

configs/example/  a small, synthetic schema.avsc + mapping.yaml - just
                   enough to build/run/demo out of the box; point at your
                   own real schema/mapping for actual production use
assets/           embedded tracking-tag JS + 1x1 GIF
reference/        a full copy of the original Java source, for reference
```

## The mapping engine

Legacy used a general-purpose Groovy DSL for mapping event data to Avro
fields. A typical production mapping only ever uses a small, fixed set of
building blocks - built-in event metadata, a `userAgent()` accessor chain,
scalar/path lookups into the custom event parameters, and `int32`/`fp64`
coercion - so `internal/mapping` replaces the DSL with a static YAML rule
list (`FieldRule`: `Builtin` / `EventParam` / `EventParamPath` / `Coerce` /
`Default`) instead of needing an expression engine.

Deferred, confirmed-real-but-unused legacy DSL features (see
`internal/mapping`'s package doc for the full list and why): `cookie()`,
`header()`, regex matching, digest/hashing, `ip2geo()`, and non-numeric
`parse().to(...)` coercions. Build the specific one that's actually needed,
against real data that needs it, if this ever changes.

## Multiple Kafka output targets (Avro and/or JSON)

`internal/kafkasink.Manager` fans each mapped event out to any number of
independently configured Kafka targets, managed live via the admin UI's
**Kafka output targets** page (`/kafka-targets` - see the admin guide) -
no restart required to add, edit, enable/disable, or remove one. Each
target has its own topic, broker list, and format (`avro` - naked, no
Confluent wire header - or `json`, via `internal/jsonenc`). A schema/
mapping change published through the admin UI, or a Kafka target edit,
takes effect on the very next event, fleet-wide, via the **config
watchdog** described below - not just on whichever instance happened to
handle the request.

Every event is encoded only once per format actually needed (not once per
target), and sent to all live targets concurrently, so one target's
producer being slow/down never blocks another's.

## Why "naked" Avro matters

A typical Kafka sink for this kind of pipeline publishes raw Avro bytes
with **no schema ID header** (unlike Confluent's usual wire format). That
means every consumer must already have the exact matching schema to
decode a message correctly - there's no per-message self-description to
fall back on. This is the root constraint behind the NiFi/Druid schema-
sync feature described below: NiFi's `AvroReader` needs to be using the
same schema Divolte encoded with, or decoding breaks outright, not just
gracefully drops new fields. (The JSON output format above sidesteps this
entirely for any consumer that would rather not deal with binary Avro.)

## Fleet-wide config sync: NiFi/Druid schema push + the config watchdog

**NiFi/Druid schema sync** (`internal/syncplugin`, `internal/nifiavro`,
`internal/druid`): clicking **Publish** in the admin UI, in one atomic
action, saves the schema/mapping in the DB, pushes the updated Avro schema
into every enabled NiFi target's Parameter Context, and updates every
enabled Druid target's Kafka-supervisor `dimensionsSpec`. There can be more
than one NiFi cluster and more than one Druid cluster configured
simultaneously (`/nifi-targets`, `/druid-targets` - same list/edit UX as
schema fields), each independently enabled/tested. This is deliberately a
**human-triggered, one-shot action** - NiFi sync stops/disables/re-enables
live processors to apply the update, a real side effect on shared infra,
so it only ever runs from an explicit Publish click, never automatically.

**The config watchdog** (`internal/config`'s
`ConfigWatchdogIntervalSeconds`, default 30s, in `cmd/divolte-collector`):
runs identically on every instance, polling the shared DB for a published
schema/mapping change or a Kafka output target change and applying it
live (hot-swapping the in-process mapping/codec, or reconciling the live
Kafka sink set) - with no restart. Without this, only the ONE instance
that actually handled a given admin action would pick it up; every sibling
instance shares the same DB row but would otherwise keep serving whatever
it loaded at its own boot, indefinitely. **Deliberately does NOT extend to
NiFi/Druid sync** - see the reasoning above; auto-triggering that
periodically, from every instance, would risk concurrent pushes racing
against the same live NiFi processors.

## Admin login: shared credentials + optional LDAP, host-agnostic primary

The DB-shared admin username/password (`internal/store`'s
`AdminSettings`, editable via `/settings`) works identically from any
instance - there's no per-instance login. LDAP/Active Directory login
(`internal/ldapauth`) is available as an *additional* way in alongside
it, gated to specific AD groups, config also DB-shared and re-read fresh
on every login attempt (no restart needed to change it).

Exactly one instance is designated the **primary** admin host
(`AdminSettings.PrimaryURL`, editable via `/settings`) - every other
instance's admin UI transparently redirects there, so there's one
unambiguous place to make edits. This is genuinely host-agnostic: any
instance can be the primary, not hardcoded to a specific box (each just
needs `admin.uri_prefix: "/admin"` set and matching reverse-proxy `/admin`
routing - both required on every instance that might ever become primary,
not only the current one). Changing the primary only affects the admin
auth-routed pages - never the beacon/Kafka-producing path on any instance.

## Example deployment topology

A typical multi-instance deployment looks like this (adjust names/counts
freely - none of this is hardcoded):

- **Collector instances** (e.g. `collector-01/02/03.example.com`), each
  behind its own reverse proxy (HAProxy, nginx, etc.) on port 80/443,
  running this Go rewrite, all publishing to the same real Kafka topic and
  pointing at the **same shared MariaDB** (see "Shared storage" below).
  Any one of them can be the designated admin-UI primary - see above.
- **NiFi** (a separate cluster from any "plain" NiFi you might also run):
  consumes the Kafka topic, converts naked Avro to JSON (via an
  `AvroReader`/`JsonRecordSetWriter` pair backed by a NiFi
  `AvroSchemaRegistry` controller service storing the schema as a single
  named parameter, kept in sync by the Publish button above), republishes
  JSON to a downstream topic.
- **Druid**: consumes that JSON topic via a Kafka supervisor. Its
  `dimensionsSpec` is a flat, explicit field list, kept in sync by the
  same Publish action.

A legacy Java deployment and this Go rewrite can coexist during a
migration - `cmd/trafficmirror` lets you shadow-test the Go rewrite
against real production traffic (mirroring requests to a scratch Kafka
topic) before cutting anything over for real.

### Shared storage

Every instance points at the **same** MariaDB database - there is exactly
one copy of the schema/mapping/sync-target/Kafka-output-target
configuration, not one independent copy per instance. Each instance still
loads its own in-memory copy of the schema/mapping/Kafka sink set at
startup - the config watchdog above is what keeps that in-memory copy
current afterward without needing a restart.

## Configuration

`configs/server.yaml` is the bootstrap config (flat YAML, not a HOCON port
of the original `divolte-collector.conf` - only the wire protocol and
schema/mapping content need to stay compatible, not the bootstrap format
itself). Key fields:

```yaml
listen_addr: ":8290"          # beacon HTTP listener
prefix: "/webstats/"          # tag/beacon URL prefix
script_name: "divolte_ng.js"
event_suffix: "csc-event"

schema_file: "..."            # only read to SEED the DB store on first boot ever -
mapping_file: "..."           # the shared DB is the live source of truth after that

kafka:
  # Legacy, one-time seed only: on first boot, if no Kafka output targets
  # exist yet in the shared DB, this becomes a single target named
  # "legacy" (format avro) - the DB is authoritative from then on. Manage
  # real output targets via /kafka-targets, not this block.
  brokers: [...]              # or DIVOLTE_KAFKA_BROKER_LIST (comma-separated)
  client_id: "..."            # or DIVOLTE_KAFKA_CLIENT_ID - identifies THIS instance's producer(s)
  topic: "..."

workers: 4                    # worker pool size
queue_size: 10000             # per-worker queue depth before dropping
duplicate_memory_size: 1000000
shutdown_delay_seconds: 5     # /ping fails this long before the real drain starts
config_watchdog_interval_seconds: 30  # how often to poll the DB for a fleet-wide change

admin:
  listen_addr: ":8291"
  db:                          # shared MariaDB/MySQL - every instance uses the SAME one
    host: "..."
    port: 3306
    name: "..."
    username: "..."
    password: "..."           # or DIVOLTE_DB_PASSWORD
  username: "admin"           # DB-shared seed - actual value is DB-authoritative after first boot
  password: "..."             # or DIVOLTE_ADMIN_PASSWORD (seed only, same caveat)
  schema_namespace: "..."
  schema_record_name: "..."
  uri_prefix: "/admin"        # reachable behind a reverse-proxy path that strips this prefix -
                               # required on EVERY instance that might ever be primary, not just one
  ldap:                       # optional, additional login method alongside the shared password
    enabled: false
    servers: [...]
    manager_dn: "..."
    manager_password: "..."   # or DIVOLTE_LDAP_BIND_PASSWORD
    user_search_base: "..."
    user_search_filter: "..."
    allowed_groups: [...]     # required non-empty when enabled - see internal/ldapauth
```

Pinned dependency versions (`go.mod`) shouldn't be upgraded casually - the
Go toolchain is pinned at 1.19, and several deps (`hamba/avro/v2`,
`IBM/sarama`, `go-sql-driver/mysql`, `gin-gonic/gin`, `golang.org/x/crypto`,
`go-ldap/ldap/v3`, `youmark/pkcs8`) are pinned to specific versions this
project has been verified against.

## Building & running

```
go build ./cmd/divolte-collector
./divolte-collector -config configs/server.yaml
```

This ships with `configs/example/` (a small synthetic schema/mapping) and
`configs/server.yaml` defaults pointing at a local Kafka broker and
MariaDB on `localhost` - bring up both locally (or point the config at
your own) to actually run it end-to-end.

`go test ./...` runs the full suite; `go vet ./...` should be clean.
Pre-push checklist for any change: `gofmt -w .`, `go vet ./...`,
`go test -race ./...`.

## Ops tooling

All of these are dev/ops tools, not part of the production binary:

- **`cmd/trafficmirror`** - a transparent tee proxy. Forwards every request
  to a primary backend (the only response the real client ever sees) and
  separately fires a best-effort copy at a mirror target. Used to shadow-test
  the Go rewrite against real traffic with zero risk to production - a
  mirror-leg failure never affects the primary response.
- **`cmd/paritycheck`** - diffs legacy vs Go rewrite output field-by-field.
  `-mode=replay` fires a fixture of captured requests at two standalone
  instances with request-unique ID rewriting (safe to run against a topic
  real traffic is also writing to); `-mode=live` passively correlates two
  topics a `trafficmirror` setup already feeds identically (no requests
  fired). See `-h` for the full flag set (brokers/topics/schema/allowlist/
  timing all overridable).
- **`cmd/topicwatch`** - read-only Kafka topic inspector (`-mode=tail` prints
  records live, `-mode=stats` aggregates field non-null rates and
  categorical distributions over a sampling window, `-mode=compare` does
  both side-by-side for two topics at once). Uses sarama's low-level
  `PartitionConsumer` directly, no consumer group - never perturbs another
  consumer's offsets, safe against live production topics. **On a
  low-volume topic, prefer polling the broker's own end-offset
  (`kafka-run-class.sh kafka.tools.GetOffsetShell`) over a short
  `topicwatch` sample** - a genuinely low rate and a broken producer can
  look identical in one short sampling window.
- **`cmd/smoketest`** - fires one realistic event at a running instance and
  consumes it back off Kafka to verify the whole pipeline end-to-end.

## Known, resolved-as-non-bugs quirks

Found and root-caused during live parity testing against legacy - not
things to "fix":

- **`remoteHost` differs when testing via a `trafficmirror` mirror leg** -
  the mirror hop is a real extra network hop, so the mirrored copy sees a
  different source IP than the primary leg legacy handled directly. Not
  present in a real (non-mirrored) deployment.
- **`userAgent*` fields sometimes differ** (e.g. "Chrome Mobile" vs
  "Chrome", "OS X" vs "Mac OS X") - legacy bundles a UA database frozen at
  2014-10-24 (confirmed via its own startup log); the Go rewrite's
  classifier is more current. An expected reporting discontinuity across a
  real cutover, not a decode bug.

## Deferred features

Legacy has real, working code for all of these, but they're unused in
every deployment checked: Confluent Kafka sink mode, GCP Pub/Sub sink, GCS
sink, the JSON HTTP event source (as opposed to the JS-tag beacon), and
several mapping-DSL producers (see above). Each is documented in its
relevant package's doc comment. Deliberate scope decision: build the
specific one that's actually needed, against real data that needs it, if
a real use case shows up - not speculatively ahead of time.

See **[docs/legacy-vs-nextgen.md](docs/legacy-vs-nextgen.md)** for the
full side-by-side comparison against the original Java server.
