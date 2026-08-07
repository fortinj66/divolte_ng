# Divolte end-to-end test framework

This directory contains the repo-owned integration test framework for the full
Divolte pipeline:

1. Divolte collector HTTP surface:
   - `/ping`
   - `/webstats/divolte_ng.js`
   - `/webstats/csc-event`
   - `/schema`
2. Browser tracking tag behavior from `assets/divolte_ng.js`:
   - script loads
   - `window.divolte` is created
   - `signal(type, params)` sends a beacon
   - `whenCommitted(cb, timeout)` waits for delivery
3. Kafka output:
   - Avro target receives naked Avro
   - JSON target receives matching mapped fields
   - add/edit/delete Kafka targets take effect live
4. Admin schema/mapping lifecycle:
   - baseline schema is published and readable
   - add a field
   - publish
   - verify the new field appears in `/schema` and output records
   - delete the field
   - publish
   - verify the field disappears
5. NiFi sync:
   - secure NiFi is reachable with a client certificate
   - NiFi target add/test/delete works through the admin UI/API
   - Publish pushes the Avro schema to NiFi's Parameter Context
   - dependency-chain stop/disable/update/restore behavior works
6. Multi-node Divolte:
   - run a second collector against the same MariaDB/Kafka
   - publish on node 1
   - verify node 2's config watchdog applies the change within the interval
7. Druid ingestion:
   - provision Kafka supervisors for direct Divolte JSON and NiFi-decoded Avro
   - verify a newly generated, uniquely identified row through both paths

## Prerequisites

The full runner expects Linux with `bash`, Go, Podman with Compose support,
`curl`, `jq`, `openssl`, `python3`, `rg`, and a Chromium-compatible browser.
The browser command defaults to `chromium-browser`; set `CHROMIUM` to another
executable such as `chromium` or `google-chrome` when needed. The runner pulls
container images on the first run, so registry/network access is required then.

The checked-in E2E baselines intentionally use:

- Apache Kafka 3.7.0
- Apache NiFi 1.19.1
- Apache Druid 24.0.2 (the production-compatible baseline)
- PostgreSQL 17.6 and ZooKeeper 3.5.10 for the Druid test stack

## Local endpoints

The full clean-room runner creates every required network and service. For a
narrower runner, first start the dev stack from the repo root as described in
the main README:

```bash
podman network exists druid-cluster_default || podman network create druid-cluster_default
podman compose up -d --build
```

The scripts use these defaults unless their documented environment variables
override them:

- beacon collector: `http://localhost:8290`
- admin UI: `http://localhost:8291`
- admin credentials: `admin` / `change-me`
- Kafka: `localhost:9092`
- MariaDB: `localhost:3306`

## Baseline runner

### Full clean-room suite

Run the complete unattended test from empty container volumes:

```bash
test/e2e/scripts/run-clean-e2e.sh
```

This removes only the known Divolte, NiFi, and Druid E2E containers, pods,
networks, and volumes. It then rebuilds the project images and recreates every
dependency before running:

1. Go unit and integration tests
2. collector/admin HTTP, tracking-tag, browser, Kafka, and schema lifecycle
3. second-collector watchdog schema add/remove propagation
4. NiFi certificate generation and zero-state REST provisioning
5. NiFi Avro-to-JSON and live schema synchronization
6. Druid supervisor provisioning and fresh-row SQL validation for both the
   direct JSON and NiFi-decoded Avro paths

The stack remains running after success for inspection. On failure, the script
prints the scoped container state and leaves the environment available for
diagnosis; the next run still starts by removing it.

Generated responses, schemas, and NiFi certificates—including private test
keys—are written below `test/e2e/tmp/`. That directory is gitignored and can be
discarded between runs.

### Divolte baseline only

```bash
test/e2e/scripts/run-divolte-e2e.sh
```

This checks the live collector and admin surfaces, validates `divolte_ng.js`,
executes the tracking tag in headless Chromium, verifies its browser-created
event in the JSON Kafka topic, runs the existing Go smoke test against Kafka,
and exercises schema add/publish/delete/publish.

To exercise only the real browser tracking path:

```bash
test/e2e/scripts/exercise-divolte-js.sh
```

The browser test serves `test/e2e/browser/divolte-ng.html` locally, loads the
collector-provided script, calls `signal()` and `whenCommitted()`, checks the
generated IDs and cookies, then consumes and validates the matching Kafka JSON
record.

## Secure NiFi

Generate local test certificates:

```bash
test/e2e/nifi/generate-certs.sh
```

Start the secure NiFi container:

```bash
podman compose -f test/e2e/nifi/compose.nifi.yml up -d
```

Provision the test flow:

```bash
test/e2e/nifi/provision-flow.sh
```

The generated client material is written under `test/e2e/tmp/nifi-certs/`.
The admin NiFi target form wants PEM values from:

- `client.cert.pem`
- `client.key.pem`
- `ca.cert.pem`

The provisioner writes the target IDs and cert paths to
`test/e2e/tmp/nifi-target.env`.

It also creates and starts a NiFi-native inspection flow:

```text
ConsumeKafkaRecord_2_6 (naked Avro + AvroSchemaRegistry)
  -> JsonRecordSetWriter
  -> PublishKafkaRecord_2_6
  -> divolte_example_event-json-debug
```

Validate a new event through the complete collector -> Avro Kafka -> NiFi ->
JSON Kafka path:

```bash
test/e2e/nifi/validate-avro-json-flow.sh
```

Validate a real Divolte schema change through the entire live path—including
NiFi's dependency-chain stop/update/restart handling—and then restore the
baseline schema:

```bash
test/e2e/nifi/validate-schema-sync-flow.sh
```

This registers the enabled `nifi-e2e` target, temporarily publishes
`e2eNifiSchemaField`, verifies the updated NiFi parameter and JSON output,
deletes the field, republishes, and verifies both systems return to baseline.

NiFi image bootstrapping is slower than the Divolte stack; wait for:

```bash
curl -k --cert test/e2e/tmp/nifi-certs/client.cert.pem \
  --key test/e2e/tmp/nifi-certs/client.key.pem \
  https://localhost:9443/nifi-api/system-diagnostics
```

## Druid

The full runner starts the five Druid roles plus PostgreSQL and ZooKeeper,
registers both supervisor definitions from `test/e2e/druid/ingestion/`, and
runs `test/e2e/scripts/validate-druid-flow.sh`. The validator publishes a
fresh correlated event and waits for SQL results in both datasources:

- `divolte_direct_json` consumes the collector's JSON Kafka target directly.
- `divolte_nifi_avro_json` consumes JSON produced by NiFi after decoding the
  collector's naked Avro record.

The Druid stack is deliberately split into independently health-checked roles
because Druid 24.0.2's service scripts and ports are role-specific.

## Notes

The NiFi compose file intentionally uses `apache/nifi:1.19.1` to match the
current production baseline. The provisioner creates a root-group Parameter
Context with `NiFiAvroSchema`, an `AvroSchemaRegistry` controller service that
references it, and the dependent consume/convert/publish processor flow. NiFi
joins the external `divolte_ng_default` Podman network and reaches Kafka at
`kafka:9094`. Parse and publish failures are retained in named NiFi queues.
