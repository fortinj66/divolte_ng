#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

COLLECTOR_URL="${COLLECTOR_URL:-http://localhost:8290}"
ADMIN_URL="${ADMIN_URL:-http://localhost:8291}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASS="${ADMIN_PASS:-change-me}"
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
AVRO_TOPIC="${AVRO_TOPIC:-divolte_example_event}"
SCHEMA_FILE="${SCHEMA_FILE:-configs/example/schema.avsc}"
WATCHDOG_SETTLE_SECONDS="${WATCHDOG_SETTLE_SECONDS:-0}"

COOKIE_JAR="$(mktemp)"
ORIGINAL_PRIMARY_URL=""

restore_primary_url() {
  if [[ -n "$ORIGINAL_PRIMARY_URL" ]]; then
    mysql -h 127.0.0.1 -P 3306 -udivolte -pchange-me divolte_config \
      -e "UPDATE admin_settings SET primary_url = '${ORIGINAL_PRIMARY_URL//\'/\'\'}' WHERE id = 1" >/dev/null
  fi
}

trap restore_primary_url EXIT

log() {
  printf '\n== %s ==\n' "$*"
}

require_http() {
  local url="$1"
  local want="${2:-200}"
  local got
  got="$(curl -fsS -o /dev/null -w '%{http_code}' "$url")"
  if [[ "$got" != "$want" ]]; then
    echo "HTTP check failed for $url: got $got, want $want" >&2
    exit 1
  fi
}

csrf_token() {
  curl -fsS -u "$ADMIN_USER:$ADMIN_PASS" -c "$COOKIE_JAR" -b "$COOKIE_JAR" "$ADMIN_URL/" >/dev/null
  awk '$6 == "csrf_token" { print $7 }' "$COOKIE_JAR" | tail -1
}

admin_post() {
  local path="$1"
  shift
  local token
  token="$(csrf_token)"
  curl -fsS -u "$ADMIN_USER:$ADMIN_PASS" -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
    -o /dev/null -w '%{http_code}' -X POST \
    -d "csrf_token=$token" "$@" "$ADMIN_URL$path"
}

require_redirect() {
  local status="$1"
  local label="$2"
  if [[ "$status" != "302" && "$status" != "303" ]]; then
    echo "$label returned $status" >&2
    exit 1
  fi
}

log "collector health"
require_http "$COLLECTOR_URL/ping" 200
require_http "$ADMIN_URL/schema" 200

log "Kafka test targets"
test/e2e/scripts/provision-kafka-targets.sh

ORIGINAL_PRIMARY_URL="$(mysql -N -B -h 127.0.0.1 -P 3306 -udivolte -pchange-me divolte_config \
  -e 'SELECT primary_url FROM admin_settings WHERE id = 1' 2>/dev/null || true)"
if [[ -n "$ORIGINAL_PRIMARY_URL" ]]; then
  mysql -h 127.0.0.1 -P 3306 -udivolte -pchange-me divolte_config \
    -e "UPDATE admin_settings SET primary_url = '' WHERE id = 1" >/dev/null
fi

log "tracking tag"
curl -fsS "$COLLECTOR_URL/webstats/divolte_ng.js" -o test/e2e/tmp/divolte_ng.js
rg 'window\\.divolte|whenCommitted|signal' test/e2e/tmp/divolte_ng.js >/dev/null
curl -fsS -H 'Accept-Encoding: gzip' -D test/e2e/tmp/divolte_ng.headers \
  "$COLLECTOR_URL/webstats/divolte_ng.js" -o test/e2e/tmp/divolte_ng.js.gz
rg -i '^Content-Encoding: gzip' test/e2e/tmp/divolte_ng.headers >/dev/null

log "tracking tag browser execution"
test/e2e/scripts/exercise-divolte-js.sh

log "baseline Kafka Avro smoke test"
go run ./cmd/smoketest \
  -server "$COLLECTOR_URL" \
  -brokers "$KAFKA_BROKERS" \
  -topic "$AVRO_TOPIC" \
  -schema "$SCHEMA_FILE"

log "schema add"
status="$(admin_post /fields \
  --data-urlencode 'name=e2eTempField' \
  --data-urlencode 'base_type=string' \
  --data-urlencode 'is_nullable=on' \
  --data-urlencode 'default_mode=null' \
  --data-urlencode 'source_kind=event_param' \
  --data-urlencode 'event_param=e2eTempField')"
require_redirect "$status" "field create"

status="$(admin_post /publish)"
require_redirect "$status" "publish after add"
curl -fsS "$ADMIN_URL/schema" | tee test/e2e/tmp/schema-after-add.json | rg '"e2eTempField"' >/dev/null
if [[ "$WATCHDOG_SETTLE_SECONDS" != "0" ]]; then
  sleep "$WATCHDOG_SETTLE_SECONDS"
fi

log "schema delete"
status="$(admin_post /fields/e2eTempField/delete)"
require_redirect "$status" "field delete"

status="$(admin_post /publish)"
require_redirect "$status" "publish after delete"
if [[ "$WATCHDOG_SETTLE_SECONDS" != "0" ]]; then
  sleep "$WATCHDOG_SETTLE_SECONDS"
fi
if curl -fsS "$ADMIN_URL/schema" | tee test/e2e/tmp/schema-after-delete.json | rg '"e2eTempField"' >/dev/null; then
  echo "e2eTempField still present after delete publish" >&2
  exit 1
fi

log "done"
