# Legacy Divolte vs. next-gen (Go rewrite)

A side-by-side comparison of the original Java/Undertow Divolte Collector
("legacy") against this Go rewrite ("next-gen"). See
[README.md](../README.md) for architecture/deployment detail and
[admin-ui-guide.md](admin-ui-guide.md) for how to actually use the new
capabilities described below.

## What stayed identical (by design)

Next-gen was built to be a drop-in replacement at the protocol/data level -
nothing downstream needs to change to accept its output:

| | Legacy | Next-gen |
|---|---|---|
| Beacon wire protocol | Query-string + "mincode" custom param encoding | **Identical** - same tag, same beacon URL shape, same mincode |
| Avro schema | `.avsc` file | **Same schema**, same field types/defaults |
| Kafka sink encoding | Naked Avro (no Confluent wire header) | **Identical** - same naked format, same partition key (partyId) |
| Real Kafka topic | Whatever topic legacy already publishes to | **Same topic** - both can write to it during a migration |

## Architecture

| | Legacy | Next-gen |
|---|---|---|
| Language/runtime | Java, Undertow | Go, `net/http` |
| Config format | HOCON (`divolte-collector.conf`) | Flat YAML bootstrap file (`server.yaml`) - only a bootstrap seed, see storage below |
| Mapping engine | General-purpose Groovy DSL | Static, declarative rule list (`Builtin`/`EventParam`/`EventParamPath`/`Coerce`/`Default`) - a typical production mapping only ever uses a small fixed set of building blocks, so no expression engine is needed |
| Schema/mapping storage | `.avsc`/`.groovy` files on disk, per-instance | Shared MariaDB/MySQL database - **one** copy across the whole fleet, not one per instance |
| Applying a config change | Edit files, **restart the process** | Edit via web UI, click Publish - **hot-swaps live, zero restart, zero dropped requests** |
| Kafka producer resilience | Unbounded app-level retry+backpressure loop | Bounded exponential backoff (up to 20s) per target - a deliberate, documented tradeoff to fit a clean shutdown window |

## Admin/operational capabilities - the biggest set of differences

Legacy had **no live admin surface at all**. Every one of these is new
capability, not a like-for-like port:

| Capability | Legacy | Next-gen |
|---|---|---|
| Schema/mapping editing | Hand-edit `.avsc`/`.groovy`, restart | Web UI, live hot-swap, smoke-tested before going live, Revert to last-published |
| Multi-instance consistency | Independent per-instance files - could silently drift | Shared DB is the single source of truth; a **config watchdog** polls every ~30s and applies a change fleet-wide with no restart, on every instance |
| Kafka output | One hardcoded sink, one topic, Avro only, config-file-only | **Any number** of independently enabled Kafka targets via a web UI (`/kafka-targets`), each its own topic/brokers, each **Avro or JSON**, hot-swappable live, encoded once per format regardless of target count |
| NiFi schema sync | **Manual** - someone edits NiFi's `AvroReader`/schema registry by hand whenever the Avro schema changes | One click (Publish) pushes the new schema into every enabled NiFi Parameter Context automatically - can target more than one NiFi cluster at once |
| Druid schema sync | **Manual** - `dimensionsSpec` hand-maintained, no auto-discovery of new fields | Same Publish click also updates every enabled Druid supervisor's `dimensionsSpec` |
| Auth | None built in (relied on network placement) | Shared DB-backed username/password + optional AD/LDAP (group-restricted), usable from any instance |
| Which instance is authoritative | N/A - no admin surface to be authoritative over | A designated **primary** (a config value, changeable, not hardcoded to one box) - every other instance transparently redirects there |
| Public schema discovery | None | `GET /schema` - unauthenticated, returns the current published Avro schema as JSON |
| Config validation | Fails at runtime on a bad config, or silently mismatches | Publish smoke-tests the new schema+mapping against a realistic synthetic event before going live - a bad combination is refused, nothing changes |

## Known, resolved-as-non-bugs behavioral differences

Found and root-caused during live parity testing - genuine output
differences, but confirmed not bugs:

- **`userAgent*` fields sometimes differ** (e.g. "Chrome Mobile" -> "Chrome",
  "OS X" -> "Mac OS X") - legacy's bundled UA database is frozen at
  2014-10-24 (confirmed via its own startup log); next-gen's classifier is
  simply more current. An expected reporting discontinuity across a real
  cutover, not a decode defect.
- **`remoteHost` differs only under test tooling** - when traffic is
  routed through `cmd/trafficmirror` for parity testing, the mirror hop is
  a genuine extra network hop next-gen sees that a direct (non-mirrored)
  deployment never would.
- **"Only next-gen published" is a win, not a bug** - an event next-gen
  successfully processed that legacy silently dropped. The metric that
  actually matters is "only legacy published" (next-gen losing data legacy
  kept), which should be 0 in a clean parity run.

## Deliberately deferred (legacy has it, next-gen doesn't - yet)

A scope decision, not an oversight - each is documented in its relevant
package's doc comment (`internal/kafkasink`, `internal/mapping`,
`internal/httpserver`). Build the specific one that's actually needed,
against real data that needs it, if a real use case ever shows up:

- Confluent Kafka sink mode (5-byte schema-registry wire header) - a
  naked-Avro sink is the common case this rewrite targets.
- Google Cloud Pub/Sub sink, Google Cloud Storage sink.
- JSON HTTP event source (as opposed to the JS-tag GIF beacon) - now
  partially covered anyway by the JSON Kafka output format above.
- Mapping DSL producers: `cookie()`, `header()`, regex matching,
  digest/hashing, `ip2geo()`, non-numeric `parse().to(...)` coercions -
  real in legacy's code, but not needed by a typical production mapping.
