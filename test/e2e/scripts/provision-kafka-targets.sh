#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

ADMIN_URL="${ADMIN_URL:-http://localhost:8291}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASS="${ADMIN_PASS:-change-me}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-divolte-ng-kafka}"
KAFKA_BROKER="${KAFKA_BROKER:-kafka:9094}"
KAFKA_CLI_BROKER="${KAFKA_CLI_BROKER:-localhost:9092}"
JSON_TARGET_ID="${JSON_TARGET_ID:-e2e-json}"
JSON_TOPIC="${JSON_TOPIC:-divolte_ng_json}"

COOKIE_JAR="$(mktemp)"
cleanup() {
  rm -f "$COOKIE_JAR"
}
trap cleanup EXIT

db_query() {
  podman exec divolte-ng-mariadb mariadb -N -B \
    -udivolte -pchange-me divolte_config -e "$1"
}

csrf_token() {
  curl -fsS -u "$ADMIN_USER:$ADMIN_PASS" -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
    "$ADMIN_URL/kafka-targets" >/dev/null
  awk '$6 == "csrf_token" {print $7}' "$COOKIE_JAR" | tail -1
}

podman exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$KAFKA_CLI_BROKER" \
  --create --if-not-exists --topic "$JSON_TOPIC" \
  --partitions 1 --replication-factor 1 >/dev/null

if [[ "$(db_query "SELECT COUNT(*) FROM kafka_output_targets WHERE id = '$JSON_TARGET_ID'")" == "0" ]]; then
  target_path=/kafka-targets
  id_args=(--data-urlencode "id=$JSON_TARGET_ID")
else
  target_path="/kafka-targets/$JSON_TARGET_ID"
  id_args=()
fi

token="$(csrf_token)"
status="$(curl -fsS -u "$ADMIN_USER:$ADMIN_PASS" -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
  -o /dev/null -w '%{http_code}' -X POST \
  -d "csrf_token=$token" \
  "${id_args[@]}" \
  -d enabled=on \
  --data-urlencode format=json \
  --data-urlencode "topic=$JSON_TOPIC" \
  --data-urlencode "brokers=$KAFKA_BROKER" \
  "$ADMIN_URL$target_path")"
if [[ "$status" != "302" && "$status" != "303" ]]; then
  echo "Kafka target provisioning returned HTTP $status" >&2
  exit 1
fi

actual="$(db_query "SELECT CONCAT(enabled, '|', format, '|', topic, '|', brokers) FROM kafka_output_targets WHERE id = '$JSON_TARGET_ID'")"
if [[ "$actual" != "1|json|$JSON_TOPIC|$KAFKA_BROKER" ]]; then
  echo "Kafka JSON target has unexpected configuration: $actual" >&2
  exit 1
fi

podman exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$KAFKA_CLI_BROKER" \
  --describe --topic "$JSON_TOPIC" >/dev/null

echo "Provisioned Kafka JSON target $JSON_TARGET_ID -> $JSON_TOPIC"
