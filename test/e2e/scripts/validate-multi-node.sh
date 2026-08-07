#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

PRIMARY_ADMIN_URL="${PRIMARY_ADMIN_URL:-http://localhost:8291}"
NODE2_COLLECTOR_URL="${NODE2_COLLECTOR_URL:-http://localhost:18390}"
NODE2_ADMIN_URL="${NODE2_ADMIN_URL:-http://localhost:18391}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASS="${ADMIN_PASS:-change-me}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-divolte-ng-kafka}"
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
KAFKA_CLI_BROKER="${KAFKA_CLI_BROKER:-localhost:9092}"
AVRO_TOPIC="${AVRO_TOPIC:-divolte_example_event}"
JSON_TOPIC="${JSON_TOPIC:-divolte_ng_json}"
TEMP_FIELD="${TEMP_FIELD:-e2eNode2SchemaField}"
TEMP_VALUE="${TEMP_VALUE:-node2-hot-reload-ok}"

TMP_DIR="$(mktemp -d)"
COOKIE_JAR="$TMP_DIR/cookies"
FIELD_CREATED=false

db_query() {
  podman exec divolte-ng-mariadb mariadb -N -B \
    -udivolte -pchange-me divolte_config -e "$1"
}

csrf_token() {
  curl -fsS -u "$ADMIN_USER:$ADMIN_PASS" -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
    "$PRIMARY_ADMIN_URL/" >/dev/null
  awk '$6 == "csrf_token" {print $7}' "$COOKIE_JAR" | tail -1
}

admin_post() {
  local path="$1"
  shift
  local token status
  token="$(csrf_token)"
  status="$(curl -fsS -u "$ADMIN_USER:$ADMIN_PASS" -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
    -o /dev/null -w '%{http_code}' -X POST -d "csrf_token=$token" "$@" \
    "$PRIMARY_ADMIN_URL$path")"
  [[ "$status" == "302" || "$status" == "303" ]]
}

field_exists() {
  [[ "$(db_query "SELECT COUNT(*) FROM schema_fields WHERE name = '$TEMP_FIELD'")" != "0" ]]
}

delete_field() {
  if field_exists; then
    admin_post "/fields/$TEMP_FIELD/delete"
    admin_post /publish
  fi
  FIELD_CREATED=false
}

cleanup() {
  local status=$?
  set +e
  if [[ "$FIELD_CREATED" == true ]]; then
    delete_field
  fi
  rm -r "$TMP_DIR"
  exit "$status"
}
trap cleanup EXIT

for url in "$NODE2_COLLECTOR_URL/ping" "$NODE2_ADMIN_URL/schema"; do
  ready=false
  for _ in {1..60}; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then
    echo "node 2 did not become ready at $url" >&2
    podman logs --tail 160 divolte-ng-collector-node2 >&2
    exit 1
  fi
done

test/e2e/scripts/provision-kafka-targets.sh >/dev/null
if field_exists; then
  delete_field
fi

admin_post /fields \
  --data-urlencode "name=$TEMP_FIELD" \
  --data-urlencode base_type=string \
  --data-urlencode is_nullable=on \
  --data-urlencode default_mode=null \
  --data-urlencode source_kind=event_param \
  --data-urlencode "event_param=$TEMP_FIELD"
FIELD_CREATED=true
admin_post /publish
curl -fsS "$PRIMARY_ADMIN_URL/schema" >"$TMP_DIR/schema-with-field.avsc"

exercise_node2() {
  local expectation="$1"
  local matched=false
  for _ in {1..15}; do
    start_offset="$(podman exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server "$KAFKA_CLI_BROKER" --topic "$JSON_TOPIC" |
      awk -F: '$2 == 0 {print $3; exit}')"

    go run ./cmd/smoketest \
      -server "$NODE2_COLLECTOR_URL" \
      -brokers "$KAFKA_BROKERS" \
      -topic "$AVRO_TOPIC" \
      -schema "$TMP_DIR/schema-with-field.avsc" \
      -custom "$TEMP_FIELD=$TEMP_VALUE" >"$TMP_DIR/smoketest.log" 2>&1

    record="$(podman exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server "$KAFKA_CLI_BROKER" --topic "$JSON_TOPIC" \
      --partition 0 --offset "$start_offset" --max-messages 1 --timeout-ms 10000 \
      2>"$TMP_DIR/kafka-consumer.log")"

    if [[ "$expectation" == present ]] &&
       jq -e --arg field "$TEMP_FIELD" --arg value "$TEMP_VALUE" \
         '.[$field] == $value' <<<"$record" >/dev/null; then
      matched=true
      break
    fi
    if [[ "$expectation" == absent ]] &&
       jq -e --arg field "$TEMP_FIELD" 'has($field) | not' \
         <<<"$record" >/dev/null; then
      matched=true
      break
    fi
    sleep 1
  done

  if [[ "$matched" != true ]]; then
    echo "node 2 output did not make $TEMP_FIELD $expectation" >&2
    podman logs --tail 160 divolte-ng-collector-node2 >&2
    return 1
  fi
}

exercise_node2 present
delete_field
exercise_node2 absent

echo "Validated node 2 watchdog schema add/remove through live Kafka JSON output"
