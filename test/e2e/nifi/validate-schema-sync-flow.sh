#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

ADMIN_URL="${ADMIN_URL:-http://localhost:8291}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASS="${ADMIN_PASS:-change-me}"
NIFI_URL_FROM_COLLECTOR="${NIFI_URL_FROM_COLLECTOR:-https://nifi:9443}"
TARGET_ID="${TARGET_ID:-nifi-e2e}"
TEMP_FIELD="${TEMP_FIELD:-e2eNifiSchemaField}"
TEMP_VALUE="${TEMP_VALUE:-schema-sync-ok}"
ENV_FILE="${ENV_FILE:-test/e2e/tmp/nifi-target.env}"
CERT_DIR="${CERT_DIR:-test/e2e/tmp/nifi-certs}"
COOKIE_JAR="$(mktemp)"
ORIGINAL_PRIMARY_URL=""
FIELD_CREATED=false

# shellcheck disable=SC1090
source "$ENV_FILE"

db_query() {
  podman exec divolte-ng-mariadb mariadb -N -B -udivolte -pchange-me divolte_config -e "$1"
}

csrf_token() {
  curl -fsS -u "$ADMIN_USER:$ADMIN_PASS" -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
    "$ADMIN_URL/settings" >/dev/null
  awk '$6 == "csrf_token" {print $7}' "$COOKIE_JAR" | tail -1
}

admin_post() {
  local path="$1"
  shift
  local token status
  token="$(csrf_token)"
  status="$(curl -fsS -u "$ADMIN_USER:$ADMIN_PASS" -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
    -o /dev/null -w '%{http_code}' -X POST -d "csrf_token=$token" "$@" "$ADMIN_URL$path")"
  if [[ "$status" != "302" && "$status" != "303" ]]; then
    echo "Admin POST $path returned HTTP $status" >&2
    return 1
  fi
}

nifi_api() {
  curl -fsSk --cert "$NIFI_CLIENT_CERT" --key "$NIFI_CLIENT_KEY" "$NIFI_BASE_URL$1"
}

nifi_schema() {
  nifi_api "/nifi-api/parameter-contexts/$NIFI_PARAMETER_CONTEXT_ID" |
    jq -r --arg name "$NIFI_PARAMETER_NAME" \
      '.component.parameters[] | select(.parameter.name == $name) | .parameter.value'
}

field_is_published() {
  curl -fsS "$ADMIN_URL/schema" | jq -e --arg name "$TEMP_FIELD" \
    'any(.fields[]; .name == $name)' >/dev/null
}

wait_for_nifi_schema() {
  local expected="$1"
  local schema
  for _ in $(seq 1 60); do
    schema="$(nifi_schema)"
    if [[ "$expected" == "present" ]] && jq -e --arg name "$TEMP_FIELD" \
      'any(.fields[]; .name == $name)' <<<"$schema" >/dev/null; then
      printf '%s\n' "$schema"
      return 0
    fi
    if [[ "$expected" == "absent" ]] && ! jq -e --arg name "$TEMP_FIELD" \
      'any(.fields[]; .name == $name)' <<<"$schema" >/dev/null; then
      printf '%s\n' "$schema"
      return 0
    fi
    sleep 1
  done
  echo "NiFi schema did not make field $TEMP_FIELD $expected" >&2
  return 1
}

wait_for_processors() {
  for _ in $(seq 1 60); do
    if [[ "$(nifi_api "/nifi-api/processors/$NIFI_CONSUMER_PROCESSOR_ID" | jq -r '.component.state')" == "RUNNING" ]] &&
       [[ "$(nifi_api "/nifi-api/processors/$NIFI_PUBLISHER_PROCESSOR_ID" | jq -r '.component.state')" == "RUNNING" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "NiFi processors did not return to RUNNING" >&2
  return 1
}

delete_test_field() {
  if field_is_published || db_query "SELECT COUNT(*) FROM schema_fields WHERE name = '$TEMP_FIELD'" | rg -q '^1$'; then
    admin_post "/fields/$TEMP_FIELD/delete" || return 1
    admin_post /publish || return 1
    wait_for_nifi_schema absent >/dev/null || return 1
    wait_for_processors || return 1
  fi
  FIELD_CREATED=false
}

cleanup() {
  local status=$?
  set +e
  if [[ "$FIELD_CREATED" == "true" ]]; then
    delete_test_field
  fi
  if [[ -n "$ORIGINAL_PRIMARY_URL" ]]; then
    escaped_primary_url="${ORIGINAL_PRIMARY_URL//\'/\'\'}"
    db_query "UPDATE admin_settings SET primary_url = '$escaped_primary_url' WHERE id = 1" >/dev/null
  fi
  rm -f "$COOKIE_JAR"
  exit "$status"
}
trap cleanup EXIT

mkdir -p test/e2e/tmp

ORIGINAL_PRIMARY_URL="$(db_query 'SELECT primary_url FROM admin_settings WHERE id = 1')"
db_query "UPDATE admin_settings SET primary_url = '' WHERE id = 1" >/dev/null

client_cert="$(<"$CERT_DIR/client.cert.pem")"
client_key="$(<"$CERT_DIR/client.key.pem")"
ca_cert="$(<"$CERT_DIR/ca.cert.pem")"
target_path=/nifi-targets
if [[ "$(db_query "SELECT COUNT(*) FROM nifi_avro_targets WHERE id = '$TARGET_ID'")" != "0" ]]; then
  target_path="/nifi-targets/$TARGET_ID"
fi

token="$(csrf_token)"
test_response="$(curl -fsS -u "$ADMIN_USER:$ADMIN_PASS" -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
  -H "X-CSRF-Token: $token" -X POST \
  --data-urlencode "base_url=$NIFI_URL_FROM_COLLECTOR" \
  --data-urlencode "client_cert=$client_cert" \
  --data-urlencode "client_key=$client_key" \
  --data-urlencode "ca_cert=$ca_cert" \
  --data-urlencode "parameter_context_id=$NIFI_PARAMETER_CONTEXT_ID" \
  --data-urlencode "parameter_name=$NIFI_PARAMETER_NAME" \
  --data-urlencode "controller_service_id=$NIFI_CONTROLLER_SERVICE_ID" \
  "$ADMIN_URL/nifi-targets/_new/test")"
jq -e '.ok == true' <<<"$test_response" >/dev/null

admin_post "$target_path" \
  --data-urlencode "id=$TARGET_ID" \
  -d enabled=on \
  --data-urlencode "base_url=$NIFI_URL_FROM_COLLECTOR" \
  --data-urlencode "client_cert=$client_cert" \
  --data-urlencode "client_key=$client_key" \
  --data-urlencode "ca_cert=$ca_cert" \
  --data-urlencode "parameter_context_id=$NIFI_PARAMETER_CONTEXT_ID" \
  --data-urlencode "parameter_name=$NIFI_PARAMETER_NAME" \
  --data-urlencode "controller_service_id=$NIFI_CONTROLLER_SERVICE_ID"

if field_is_published || [[ "$(db_query "SELECT COUNT(*) FROM schema_fields WHERE name = '$TEMP_FIELD'")" != "0" ]]; then
  delete_test_field
fi

admin_post /fields \
  --data-urlencode "name=$TEMP_FIELD" \
  --data-urlencode 'base_type=string' \
  --data-urlencode 'is_nullable=on' \
  --data-urlencode 'default_mode=null' \
  --data-urlencode 'source_kind=event_param' \
  --data-urlencode "event_param=$TEMP_FIELD"
FIELD_CREATED=true
admin_post /publish

curl -fsS "$ADMIN_URL/schema" | jq -e --arg name "$TEMP_FIELD" \
  'any(.fields[]; .name == $name)' >/dev/null
wait_for_nifi_schema present > test/e2e/tmp/nifi-schema-after-add.json
wait_for_processors

EXTRA_FIELD_NAME="$TEMP_FIELD" EXTRA_FIELD_VALUE="$TEMP_VALUE" \
  OUT_FILE=test/e2e/tmp/nifi-json-after-schema-add.json \
  test/e2e/nifi/validate-avro-json-flow.sh

delete_test_field
wait_for_nifi_schema absent > test/e2e/tmp/nifi-schema-after-delete.json

test/e2e/nifi/validate-avro-json-flow.sh >/dev/null

echo "Validated Divolte schema publish -> NiFi update -> live JSON flow -> schema removal"
echo "  target:          $TARGET_ID"
echo "  temporary field: $TEMP_FIELD=$TEMP_VALUE"
echo "  final state:     field removed; NiFi processors running"
