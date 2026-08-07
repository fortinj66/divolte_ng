#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

DRUID_MASTER_URL="${DRUID_MASTER_URL:-http://localhost:8081}"
DRUID_ROUTER_URL="${DRUID_ROUTER_URL:-http://localhost:8888}"
DIRECT_SPEC="${DIRECT_SPEC:-test/e2e/druid/ingestion/divolte-json-supervisor.json}"
NIFI_SPEC="${NIFI_SPEC:-test/e2e/druid/ingestion/nifi-avro-json-supervisor.json}"

wait_http() {
  local url="$1"
  local name="$2"
  for _ in {1..180}; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "$name did not become ready at $url" >&2
  return 1
}

wait_http "$DRUID_MASTER_URL/status/health" "Druid master"
wait_http "http://localhost:8082/status/health" "Druid broker"
wait_http "$DRUID_ROUTER_URL/status/health" "Druid router"

curl -fsS -X POST -H 'Content-Type: application/json' \
  --data-binary @"$DIRECT_SPEC" \
  "$DRUID_MASTER_URL/druid/indexer/v1/supervisor" >/dev/null
curl -fsS -X POST -H 'Content-Type: application/json' \
  --data-binary @"$NIFI_SPEC" \
  "$DRUID_MASTER_URL/druid/indexer/v1/supervisor" >/dev/null

for supervisor in divolte_direct_json divolte_nifi_avro_json; do
  running=false
  for _ in {1..180}; do
    status="$(curl -fsS \
      "$DRUID_MASTER_URL/druid/indexer/v1/supervisor/$supervisor/status" \
      2>/dev/null || true)"
    if jq -e '.payload.healthy == true and .payload.state == "RUNNING" and
      .payload.detailedState == "RUNNING"' <<<"$status" >/dev/null 2>&1; then
      running=true
      break
    fi
    sleep 1
  done
  if [[ "$running" != true ]]; then
    echo "Druid supervisor $supervisor did not reach RUNNING" >&2
    printf '%s\n' "$status" >&2
    podman logs --tail 160 druid-master >&2
    podman logs --tail 160 druid-middlemanager >&2
    exit 1
  fi
done

START_MILLIS="$(date +%s%3N)"
test/e2e/scripts/exercise-divolte-js.sh >/dev/null
test/e2e/nifi/validate-avro-json-flow.sh >/dev/null

sql_count() {
  local query="$1"
  jq -cn --arg query "$query" '{query:$query}' |
    curl -fsS -H 'Content-Type: application/json' --data-binary @- \
      "$DRUID_ROUTER_URL/druid/v2/sql" |
    jq -r '.[0].count // 0'
}

DIRECT_QUERY="SELECT COUNT(*) AS \"count\" FROM \"divolte_direct_json\" WHERE \"__time\" >= MILLIS_TO_TIMESTAMP($START_MILLIS) AND \"eventType\" = 'browserExercise' AND \"userAgentFamily\" = 'Chrome' AND \"detectedDuplicate\" = FALSE"
NIFI_QUERY="SELECT COUNT(*) AS \"count\" FROM \"divolte_nifi_avro_json\" WHERE \"__time\" >= MILLIS_TO_TIMESTAMP($START_MILLIS) AND \"eventType\" = 'pageView' AND \"customLabel\" = 'smoketest-label' AND \"userAgentFamily\" = 'Chrome'"

direct_count=0
nifi_count=0
for _ in {1..120}; do
  direct_count="$(sql_count "$DIRECT_QUERY" 2>/dev/null || printf '0')"
  nifi_count="$(sql_count "$NIFI_QUERY" 2>/dev/null || printf '0')"
  if (( direct_count >= 1 && nifi_count >= 1 )); then
    echo "Validated Druid ingestion from direct JSON and NiFi-decoded Avro"
    echo "  divolte_direct_json:          $direct_count fresh row(s)"
    echo "  divolte_nifi_avro_json:       $nifi_count fresh row(s)"
    exit 0
  fi
  sleep 1
done

echo "Druid did not expose both fresh validation rows" >&2
echo "  direct count: $direct_count" >&2
echo "  NiFi count:   $nifi_count" >&2
for supervisor in divolte_direct_json divolte_nifi_avro_json; do
  curl -fsS "$DRUID_MASTER_URL/druid/indexer/v1/supervisor/$supervisor/status" |
    jq '{id, state:.payload.state, detailedState:.payload.detailedState,
      healthy:.payload.healthy, aggregateLag:.payload.aggregateLag}' >&2
done
exit 1
