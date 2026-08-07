#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

COLLECTOR_URL="${COLLECTOR_URL:-http://localhost:8290}"
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-divolte-ng-kafka}"
AVRO_TOPIC="${AVRO_TOPIC:-divolte_example_event}"
DEBUG_TOPIC="${DEBUG_TOPIC:-divolte_example_event-json-debug}"
SCHEMA_FILE="${SCHEMA_FILE:-test/e2e/tmp/live-schema.avsc}"
OUT_FILE="${OUT_FILE:-test/e2e/tmp/nifi-json-debug.json}"
ADMIN_URL="${ADMIN_URL:-http://localhost:8291}"
EXTRA_FIELD_NAME="${EXTRA_FIELD_NAME:-}"
EXTRA_FIELD_VALUE="${EXTRA_FIELD_VALUE:-}"

mkdir -p "$(dirname "$OUT_FILE")"
curl -fsS "$ADMIN_URL/schema" > "$SCHEMA_FILE"

# Capture the current end offset first. Reading back from this exact offset
# after the beacon avoids consumer-group join timing races and proves that the
# JSON record traversed NiFi during this run.
start_offset="$(podman exec "$KAFKA_CONTAINER" \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic "$DEBUG_TOPIC" |
  awk -F: '$2 == 0 {print $3; exit}')"

smoketest_args=(
  -server "$COLLECTOR_URL"
  -brokers "$KAFKA_BROKERS"
  -topic "$AVRO_TOPIC"
  -schema "$SCHEMA_FILE"
)
if [[ -n "$EXTRA_FIELD_NAME" ]]; then
  smoketest_args+=( -custom "$EXTRA_FIELD_NAME=$EXTRA_FIELD_VALUE" )
fi

event_start_millis="$(date +%s%3N)"
go run ./cmd/smoketest \
  "${smoketest_args[@]}"

matched_record=""
for _ in $(seq 1 30); do
  end_offset="$(podman exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 \
    --topic "$DEBUG_TOPIC" |
    awk -F: '$2 == 0 {print $3; exit}')"
  if (( end_offset > start_offset )); then
    message_count=$((end_offset - start_offset))
    records="$(podman exec "$KAFKA_CONTAINER" \
      /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 \
      --topic "$DEBUG_TOPIC" \
      --partition 0 \
      --offset "$start_offset" \
      --max-messages "$message_count" \
      --timeout-ms 10000 2>/dev/null)"
    matched_record="$(jq -cs --argjson start "$event_start_millis" '
      [.[] |
        (if type == "array" then .[] else . end) |
        select(
          .timestamp >= $start and
          .pageViewId == "smoketest-pageview" and
          .eventType == "pageView" and
          .customLabel == "smoketest-label"
        )
      ] | last // empty
    ' <<<"$records")"
    if [[ -n "$matched_record" ]]; then
      printf '%s\n' "$matched_record" >"$OUT_FILE"
      break
    fi
  fi
  sleep 1
done

if [[ -z "$matched_record" ]]; then
  echo "No matching JSON record arrived on $DEBUG_TOPIC" >&2
  exit 1
fi

jq -e --arg extraName "$EXTRA_FIELD_NAME" --arg extraValue "$EXTRA_FIELD_VALUE" '
  (if type == "array" then .[0] else . end) as $record |
  ($record.partyId | startswith("0:")) and
  ($record.sessionId | startswith("0:")) and
  $record.pageViewId == "smoketest-pageview" and
  $record.eventType == "pageView" and
  $record.location == "https://example.com/product/123" and
  $record.userAgentFamily == "Chrome" and
  $record.detectedDuplicate == false and
  $record.customLabel == "smoketest-label" and
  ($extraName == "" or $record[$extraName] == $extraValue)
' "$OUT_FILE" >/dev/null

echo "Validated NiFi Avro-to-JSON flow on $DEBUG_TOPIC"
jq 'if type == "array" then .[0] else . end' "$OUT_FILE"
