# Admin UI guide

The admin UI lets you view and edit the Avro schema and mapping rules
(which fields exist, what type they are, and where their value comes
from), manage where events get published (one or more Kafka topics, Avro
and/or JSON), and keep NiFi/Druid's own copies of the schema in sync -
all live, with no restart. This is new functionality versus the original
Java server, which only supported hand-editing `.avsc`/`.groovy`/HOCON
files on disk and restarting the process, with NiFi/Druid updated by hand
separately.

## Accessing it

Every instance shares the same login and the same
underlying data - but exactly one is designated the **primary**, and
every other instance's admin UI automatically redirects you there (see
"Primary instance" below). Just go to whichever instance you know about;
you'll land in the right place either way:

```
https://collector-01.example.com/admin/
```

Username `admin`; get the password from whoever manages the server - it's
stored in the shared database (editable via `/settings`), not written
down here, and does **not** necessarily match any single instance's own
bootstrap config file, since it's DB-authoritative and may have been
changed since any instance was last deployed. An AD/LDAP login is also
available as an alternative, if enabled - see "Login" below.

## Login

Two ways in, either works:

- **Shared username/password** - one set of credentials, stored in the
  database, shared by every instance. Editable via `/settings`.
- **AD/LDAP** (optional, if enabled) - log in with your normal AD
  credentials. Restricted to specific AD groups (configured via
  `/settings`, at least one group required whenever LDAP is enabled - this
  UI can edit live schema/mapping/output config, so it never authenticates
  "anyone who can log into the domain" the way some looser integrations
  do). Re-checked fresh on every login attempt - a config change here
  (servers, search base, allowed groups) takes effect immediately, no
  restart needed.

## The field list

The home page lists every Avro field in the schema, in the order they'll
appear in the record, with:

- **Type** - the raw Avro type JSON (e.g. `"string"`, `["null",{"type":"array","items":"string"}]`).
- **Default** - the schema-level default used when a field is absent from an
  event, if one exists.
- **Source** - a one-line summary of the mapping rule: which builtin,
  event parameter, or event-parameter-path this field's value comes from
  (or "none" if the field has no active rule and will always be absent).

Use the **filter box** above the table to narrow down a long field list by
name, type, or source text - purely a display filter, doesn't touch the
server.

### Reordering fields

Field order matters (it's the Avro record's field order). Two ways to
reorder:

- **Drag the grip** (⋮⋮) on the left of a row to move it anywhere.
- **Select consecutive rows** (checkboxes) and use **Move up** / **Move
  down** to shift the whole selected block past its one immediate
  neighbor. The selection must already be a contiguous run in the current
  order - the server rejects a non-consecutive selection.

## Adding or editing a field

**+ New field** or clicking a field name opens the field form:

1. **Avro field name** - fixed once created (read-only when editing).
2. **Value type** - pick a primitive type, optionally check "holds a list
   of values" for an array type, and "can be empty / left out of the
   event" for a nullable field (with a default: none, null, or a specific
   value).
3. **Advanced: raw Avro type JSON** - only needed for a shape the builder
   above can't express (records, enums, maps, multi-branch unions).
   Filling this in overrides everything above it.
4. **Where does this field's value come from?** - exactly one of:
   - **A value the server already knows** (a *builtin*: identity/session
     IDs, page/request info, parsed User-Agent fields, etc.) - no tracking
     tag configuration needed.
   - **A single value the page sends** (an *event parameter* - the exact
     key name the tracking tag uses).
   - **A list of values the page sends** (an *event parameter path* - same
     idea, for values that arrive as a list, e.g. several SKUs in a cart).
5. **Convert to a number?** - if the page sends this as text but the field
   should store a number, coerce to a whole number or decimal.
6. **Fixed fallback value** - if the page doesn't send this, substitute a
   fixed value instead of leaving the field absent.

Saving writes to the store immediately, but **does not go live** until you
Publish.

## Publish and Revert

The field list banner shows **"You have changes that haven't been
published yet"** whenever the store differs from what's actually running.

- **Publish**: before anything goes live, the server smoke-tests the new
  schema+mapping against a realistic synthetic event (every custom
  parameter and builtin field populated) - this catches a schema/mapping
  combination that parses fine on its own but doesn't actually fit
  together (e.g. a coercion mode that doesn't match the field's declared
  type). If that fails, the publish is refused and nothing changes; the
  live server keeps running its previous, working config. If it passes,
  the new config hot-swaps into the running server with no restart and no
  dropped in-flight requests, AND is pushed to every enabled NiFi/Druid
  sync target (see below) in the same click - one button does all three.
  Every other instance in the fleet picks up the same change within one
  **config watchdog** interval (default 30s), with no restart - see
  "Fleet-wide propagation" below.
- **Revert**: discards every edit made since the last Publish (or server
  boot), restoring the store to exactly match what's currently live. Only
  available when there are unpublished changes.

Neither action is undo-able once confirmed - Revert especially, since it
discards edits, not just unpublishes them.

## Kafka output targets

**`/kafka-targets`** manages where every incoming event actually gets
published - independent of the schema/mapping Publish flow above, since
"what fields exist" and "where do they go" are separate concerns. There
can be any number of targets, each:

- **Enabled/disabled** independently - a disabled target receives nothing.
- A **format**: `avro` (naked, no Confluent wire header - matches the
  original production sink) or `json` (plain JSON of the same mapped
  fields).
- Its own **topic** and **brokers** - a target can point at a completely
  different Kafka cluster than another target.

A saved change (add/edit/enable/disable/delete) takes effect on the very
next event on whichever instance handled the save, and on every other
instance within one config-watchdog interval - no restart, anywhere, ever
required for a Kafka output change. **Test connection** opens a throwaway
connection to the typed-in brokers and confirms the topic is reachable,
without publishing anything.

## NiFi and Druid sync targets

**`/nifi-targets`** and **`/druid-targets`** manage which NiFi
Parameter Contexts and Druid Kafka-supervisor `dimensionsSpec`s get
updated by Publish. There can be more than one of each kind (e.g. separate
dev and prod NiFi/Druid clusters) - every ENABLED target gets updated on
every Publish, each reported independently in the publish result message.
**Test connection** validates connectivity/credentials without applying
any change. Unlike Kafka output targets, this sync is deliberately
**only** triggered by an explicit Publish click - never automatically -
since applying it involves briefly stopping/disabling/re-enabling live
NiFi processors, a real side effect on shared infrastructure that should
only ever happen when someone deliberately asks for it.

## The public `/schema` endpoint

`GET /schema` (no login required, deliberately public) returns the
currently-published Avro schema as JSON - for any tooling that needs to
discover the schema without admin credentials or DB access.

## Primary instance

Every Divolte instance shares the same database - there is exactly one
copy of the schema/mapping/sync-target/Kafka-output-target configuration,
not one independent copy per instance. Exactly one instance is designated
the **primary** admin host (`/settings`, `Primary URL`) so there's one
unambiguous place people land when they go looking for the admin UI -
every other instance transparently 302-redirects there (any path,
preserving the query string), so you never need to remember which
specific host to use.

This is genuinely host-agnostic - any instance can be the primary, and
it's just a config value, not a hardcoded role. An instance needs two
things before it can actually serve as primary, though: `admin.uri_prefix:
"/admin"` set in its own `server.yaml`, and matching HAProxy `/admin`
path-routing on that host - without both, changing the primary to that
instance would redirect people to a host that can't actually render
working links (the landing page loads, but every generated link/redirect
on it 404s).

**`/settings` itself is always reachable directly on every instance**,
regardless of what the primary is currently set to - so a wrong/
unreachable primary URL can never strand you with no way to fix it back.

## Fleet-wide propagation: the config watchdog

Publishing a schema/mapping change, or editing a Kafka output target,
only directly updates the ONE instance that handled that specific
request (in practice, whichever instance is currently primary, since
that's where every admin action lands). Every other instance polls the
shared database every `config_watchdog_interval_seconds` (default 30) and
applies the same change itself - hot-swapping its own in-process
schema/mapping, or reconciling its own live Kafka sink set - with no
restart needed anywhere. Look for `config watchdog: picked up a
schema/mapping change` in an instance's own logs to confirm it caught a
particular update.

This does **not** extend to NiFi/Druid sync (see above) - that stays
exclusively tied to an explicit Publish click.

## Troubleshooting

- **A page 302-redirects somewhere else** - you hit a non-primary
  instance; you've been sent to the current primary, which is expected
  and not an error.
- **Publish refused with an encoding error** - the smoke test caught a
  genuine mismatch between the mapping and schema for the field named in
  the error. Fix that field's coercion/type/source before trying again;
  nothing went live.
- **A schema/Kafka-target change doesn't seem to have reached another
  instance yet** - give it up to one full `config_watchdog_interval_seconds`
  (default 30s); it's polling-based, not instant, by design.
- **NiFi/Druid still show the old schema after a Publish** - check the
  publish result message for which targets actually succeeded (a down/
  unreachable target is reported, not silently skipped) - re-publish once
  it's reachable again to catch it up. Unlike schema/mapping and Kafka
  targets, there's no watchdog for this - it only ever applies on an
  explicit Publish click, by design (see "NiFi and Druid sync targets"
  above for why).
