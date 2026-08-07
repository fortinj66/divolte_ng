#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

COLLECTOR_URL="${COLLECTOR_URL:-http://localhost:8290}"
FIXTURE_HOST="${FIXTURE_HOST:-127.0.0.1}"
FIXTURE_PORT="${FIXTURE_PORT:-18292}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-divolte-ng-kafka}"
KAFKA_BROKER="${KAFKA_BROKER:-localhost:9094}"
JSON_TOPIC="${JSON_TOPIC:-divolte_ng_json}"
CHROMIUM="${CHROMIUM:-chromium-browser}"

TMP_DIR="$(mktemp -d)"
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -r "$TMP_DIR"
}
trap cleanup EXIT

for command in curl jq podman python3 "$CHROMIUM"; do
  command -v "$command" >/dev/null || {
    echo "required command not found: $command" >&2
    exit 1
  }
done

python3 -m http.server "$FIXTURE_PORT" --bind "$FIXTURE_HOST" \
  --directory test/e2e/browser >"$TMP_DIR/http-server.log" 2>&1 &
SERVER_PID="$!"

FIXTURE_URL="http://$FIXTURE_HOST:$FIXTURE_PORT/divolte-ng.html"
FIXTURE_READY=false
for _ in {1..20}; do
  if curl -fsS -o /dev/null "$FIXTURE_URL" 2>/dev/null; then
    FIXTURE_READY=true
    break
  fi
  sleep 0.1
done
if [[ "$FIXTURE_READY" != true ]]; then
  echo "browser fixture did not start at $FIXTURE_URL" >&2
  sed -n '1,160p' "$TMP_DIR/http-server.log" >&2
  exit 1
fi

START_OFFSET="$(
  podman exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server "$KAFKA_BROKER" --topic "$JSON_TOPIC" |
    awk -F: 'END {print $3}'
)"
if [[ ! "$START_OFFSET" =~ ^[0-9]+$ ]]; then
  echo "could not determine starting offset for $JSON_TOPIC" >&2
  exit 1
fi

LABEL="browser-e2e-$(date +%s%N)"
PAGE_URL="$FIXTURE_URL?label=$LABEL&collector=$COLLECTOR_URL"

if ! DOM="$("$CHROMIUM" \
  --headless --no-sandbox --disable-gpu --disable-background-networking \
  --disable-component-update --no-first-run \
  --user-data-dir="$TMP_DIR/chromium-profile" \
  --virtual-time-budget=4000 --dump-dom "$PAGE_URL" \
  2>"$TMP_DIR/chromium.log")"; then
  sed -n '1,160p' "$TMP_DIR/chromium.log" >&2
  exit 1
fi

printf '%s\n' "$DOM" | rg 'data-state="committed"' >/dev/null
printf '%s\n' "$DOM" | rg '"eventId": "0:[^"]+"' >/dev/null
printf '%s\n' "$DOM" | rg '"partyId": "0:[^"]+"' >/dev/null
printf '%s\n' "$DOM" | rg '"sessionId": "0:[^"]+"' >/dev/null
printf '%s\n' "$DOM" | rg '"pageViewId": "0:[^"]+"' >/dev/null
printf '%s\n' "$DOM" | rg '_dvp=' >/dev/null
printf '%s\n' "$DOM" | rg '_dvs=' >/dev/null

if ! RECORD="$(podman exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_BROKER" --topic "$JSON_TOPIC" --partition 0 \
  --offset "$START_OFFSET" --max-messages 1 --timeout-ms 10000 \
  2>"$TMP_DIR/kafka-consumer.log")"; then
  sed -n '1,160p' "$TMP_DIR/kafka-consumer.log" >&2
  exit 1
fi

printf '%s\n' "$RECORD" | jq -e \
  --arg label "$LABEL" \
  '.customLabel == $label and .eventType == "browserExercise" and
   .userAgentFamily == "Chrome" and .detectedDuplicate == false' >/dev/null

echo "divolte_ng.js browser exercise passed"
echo "  label:  $LABEL"
echo "  event:  $(printf '%s\n' "$DOM" | sed -n 's/.*"eventId": "\([^"]*\)".*/\1/p' | head -1)"
echo "  Kafka:  $JSON_TOPIC partition 0 offset $START_OFFSET"
