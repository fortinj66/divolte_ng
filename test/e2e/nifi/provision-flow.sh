#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

NIFI_URL="${NIFI_URL:-https://localhost:9443}"
CERT_DIR="${CERT_DIR:-test/e2e/tmp/nifi-certs}"
CLIENT_CERT="${CLIENT_CERT:-$CERT_DIR/client.cert.pem}"
CLIENT_KEY="${CLIENT_KEY:-$CERT_DIR/client.key.pem}"
SCHEMA_FILE="${SCHEMA_FILE:-configs/example/schema.avsc}"
OUT_FILE="${OUT_FILE:-test/e2e/tmp/nifi-target.env}"
PARAMETER_CONTEXT_NAME="${PARAMETER_CONTEXT_NAME:-Divolte E2E}"
PARAMETER_NAME="${PARAMETER_NAME:-NiFiAvroSchema}"
SCHEMA_REGISTRY_NAME="${SCHEMA_REGISTRY_NAME:-Divolte E2E AvroSchemaRegistry}"
KAFKA_BROKERS="${KAFKA_BROKERS:-kafka:9094}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-divolte-ng-kafka}"
KAFKA_CLI_BROKER="${KAFKA_CLI_BROKER:-localhost:9092}"
AVRO_TOPIC="${AVRO_TOPIC:-divolte_example_event}"
DEBUG_TOPIC="${DEBUG_TOPIC:-divolte_example_event-json-debug}"
CONSUMER_GROUP="${CONSUMER_GROUP:-nifi-divolte-avro-json-inspect}"
AVRO_READER_NAME="${AVRO_READER_NAME:-Divolte Avro Reader}"
JSON_WRITER_NAME="${JSON_WRITER_NAME:-Divolte JSON Writer}"
JSON_READER_NAME="${JSON_READER_NAME:-Divolte JSON Reader}"
CONSUMER_NAME="${CONSUMER_NAME:-Consume Divolte Avro}"
PUBLISHER_NAME="${PUBLISHER_NAME:-Publish Divolte JSON Debug}"
FAILURE_FUNNEL_NAME="${FAILURE_FUNNEL_NAME:-Divolte Conversion Failures}"

api() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -fsSk --cert "$CLIENT_CERT" --key "$CLIENT_KEY" \
      -H 'Content-Type: application/json' -X "$method" --data @"$body" \
      "$NIFI_URL$path"
  else
    curl -fsSk --cert "$CLIENT_CERT" --key "$CLIENT_KEY" -X "$method" "$NIFI_URL$path"
  fi
}

wait_for_nifi() {
  for _ in $(seq 1 90); do
    if api GET /nifi-api/flow/about >/dev/null 2>&1; then
      return 0
    fi
    sleep 5
  done
  echo "NiFi did not become ready at $NIFI_URL" >&2
  return 1
}

ensure_policy() {
  local resource="$1"
  local action="$2"
  local user_id="$3"
  local lookup_action="$action"
  [[ "$lookup_action" == "read" ]] && lookup_action="read"
  [[ "$lookup_action" == "write" ]] && lookup_action="write"

  if api GET "/nifi-api/policies/$lookup_action$resource" >/dev/null 2>&1; then
    return 0
  fi

  local body
  body="$(mktemp)"
  jq -n --arg resource "$resource" --arg action "$action" --arg uid "$user_id" \
    '{revision:{version:0},component:{resource:$resource,action:$action,users:[{id:$uid}],userGroups:[]}}' > "$body"
  api POST /nifi-api/policies "$body" >/dev/null
  rm -f "$body"
}

poll_parameter_update() {
  local context_id="$1"
  local request_id="$2"
  for _ in $(seq 1 60); do
    local resp complete failure
    resp="$(api GET "/nifi-api/parameter-contexts/$context_id/update-requests/$request_id")"
    complete="$(jq -r '.request.complete' <<<"$resp")"
    failure="$(jq -r '.request.failureReason // ""' <<<"$resp")"
    if [[ "$complete" == "true" ]]; then
      api DELETE "/nifi-api/parameter-contexts/$context_id/update-requests/$request_id" >/dev/null || true
      if [[ -n "$failure" ]]; then
        echo "NiFi parameter update failed: $failure" >&2
        return 1
      fi
      return 0
    fi
    sleep 1
  done
  echo "NiFi parameter update did not complete" >&2
  return 1
}

set_controller_service_state() {
  local service_id="$1"
  local state="$2"
  local current
  current="$(api GET "/nifi-api/controller-services/$service_id")"
  if [[ "$(jq -r '.component.state' <<<"$current")" == "$state" ]]; then
    return 0
  fi
  local version body
  version="$(jq -r '.revision.version' <<<"$current")"
  body="$(mktemp)"
  jq -n --arg id "$service_id" --arg state "$state" --argjson ver "$version" \
    '{revision:{version:$ver},component:{id:$id,state:$state}}' > "$body"
  api PUT "/nifi-api/controller-services/$service_id" "$body" >/dev/null
  rm -f "$body"
  for _ in $(seq 1 30); do
    if [[ "$(api GET "/nifi-api/controller-services/$service_id" | jq -r '.component.state')" == "$state" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "Controller service $service_id did not reach $state" >&2
  return 1
}

ensure_controller_service() {
  local root_id="$1"
  local name="$2"
  local type="$3"
  local artifact="$4"
  local id body response
  id="$(api GET "/nifi-api/flow/process-groups/$root_id/controller-services?includeAncestorGroups=true" |
    jq -r --arg name "$name" '.controllerServices[]? | select(.component.name == $name) | .id' | head -1)"
  if [[ -n "$id" ]]; then
    printf '%s\n' "$id"
    return
  fi
  body="$(mktemp)"
  jq -n --arg name "$name" --arg type "$type" --arg artifact "$artifact" \
    '{revision:{version:0},component:{name:$name,type:$type,bundle:{group:"org.apache.nifi",artifact:$artifact,version:"1.19.1"}}}' > "$body"
  response="$(api POST "/nifi-api/process-groups/$root_id/controller-services" "$body")"
  rm -f "$body"
  jq -r '.id' <<<"$response"
}

configure_controller_service() {
  local id="$1"
  local properties="$2"
  local current version body
  current="$(api GET "/nifi-api/controller-services/$id")"
  if [[ "$(jq -r '.component.state' <<<"$current")" != "DISABLED" ]]; then
    set_controller_service_state "$id" DISABLED
    current="$(api GET "/nifi-api/controller-services/$id")"
  fi
  version="$(jq -r '.revision.version' <<<"$current")"
  body="$(mktemp)"
  jq -n --arg id "$id" --argjson ver "$version" --argjson props "$properties" \
    '{revision:{version:$ver},component:{id:$id,properties:$props}}' > "$body"
  api PUT "/nifi-api/controller-services/$id" "$body" >/dev/null
  rm -f "$body"
  set_controller_service_state "$id" ENABLED
}

ensure_processor() {
  local root_id="$1"
  local name="$2"
  local type="$3"
  local x="$4"
  local y="$5"
  local id body response
  id="$(api GET "/nifi-api/process-groups/$root_id/processors" |
    jq -r --arg name "$name" '.processors[]? | select(.component.name == $name) | .id' | head -1)"
  if [[ -n "$id" ]]; then
    printf '%s\n' "$id"
    return
  fi
  body="$(mktemp)"
  jq -n --arg name "$name" --arg type "$type" --argjson x "$x" --argjson y "$y" \
    '{revision:{version:0},component:{name:$name,type:$type,bundle:{group:"org.apache.nifi",artifact:"nifi-kafka-2-6-nar",version:"1.19.1"},position:{x:$x,y:$y}}}' > "$body"
  response="$(api POST "/nifi-api/process-groups/$root_id/processors" "$body")"
  rm -f "$body"
  jq -r '.id' <<<"$response"
}

configure_processor() {
  local id="$1"
  local properties="$2"
  local auto_terminated="$3"
  local current version body
  current="$(api GET "/nifi-api/processors/$id")"
  if [[ "$(jq -r '.component.state' <<<"$current")" == "RUNNING" ]]; then
    set_processor_state "$id" STOPPED
    current="$(api GET "/nifi-api/processors/$id")"
  fi
  version="$(jq -r '.revision.version' <<<"$current")"
  body="$(mktemp)"
  jq -n --arg id "$id" --argjson ver "$version" --argjson props "$properties" --argjson auto "$auto_terminated" \
    '{revision:{version:$ver},component:{id:$id,config:{properties:$props,autoTerminatedRelationships:$auto}}}' > "$body"
  api PUT "/nifi-api/processors/$id" "$body" >/dev/null
  rm -f "$body"
}

set_processor_state() {
  local id="$1"
  local state="$2"
  local current version body
  current="$(api GET "/nifi-api/processors/$id")"
  if [[ "$(jq -r '.component.state' <<<"$current")" == "$state" ]]; then
    return
  fi
  version="$(jq -r '.revision.version' <<<"$current")"
  body="$(mktemp)"
  jq -n --arg state "$state" --argjson ver "$version" \
    '{revision:{version:$ver},state:$state,disconnectedNodeAcknowledged:false}' > "$body"
  api PUT "/nifi-api/processors/$id/run-status" "$body" >/dev/null
  rm -f "$body"
  for _ in $(seq 1 30); do
    if [[ "$(api GET "/nifi-api/processors/$id" | jq -r '.component.state')" == "$state" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "Processor $id did not reach $state" >&2
  return 1
}

ensure_funnel() {
  local root_id="$1"
  local x="$2"
  local y="$3"
  local id body response
  id="$(api GET "/nifi-api/process-groups/$root_id/funnels" | jq -r '.funnels[0].id // empty')"
  if [[ -n "$id" ]]; then
    printf '%s\n' "$id"
    return
  fi
  body="$(mktemp)"
  jq -n --argjson x "$x" --argjson y "$y" \
    '{revision:{version:0},component:{position:{x:$x,y:$y}}}' > "$body"
  response="$(api POST "/nifi-api/process-groups/$root_id/funnels" "$body")"
  rm -f "$body"
  jq -r '.id' <<<"$response"
}

ensure_connection() {
  local root_id="$1"
  local name="$2"
  local source_id="$3"
  local source_type="$4"
  local destination_id="$5"
  local destination_type="$6"
  local relationship="$7"
  local id body
  id="$(api GET "/nifi-api/process-groups/$root_id/connections" |
    jq -r --arg name "$name" '.connections[]? | select(.component.name == $name) | .id' | head -1)"
  if [[ -n "$id" ]]; then
    return
  fi
  body="$(mktemp)"
  jq -n --arg name "$name" --arg group "$root_id" \
    --arg source "$source_id" --arg sourceType "$source_type" \
    --arg destination "$destination_id" --arg destinationType "$destination_type" \
    --arg relationship "$relationship" \
    '{revision:{version:0},component:{name:$name,source:{id:$source,type:$sourceType,groupId:$group},destination:{id:$destination,type:$destinationType,groupId:$group},selectedRelationships:[$relationship],flowFileExpiration:"0 sec",backPressureObjectThreshold:10000,backPressureDataSizeThreshold:"1 GB"}}' > "$body"
  api POST "/nifi-api/process-groups/$root_id/connections" "$body" >/dev/null
  rm -f "$body"
}

for topic in "$AVRO_TOPIC" "$DEBUG_TOPIC"; do
  podman exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$KAFKA_CLI_BROKER" \
    --create --if-not-exists --topic "$topic" \
    --partitions 1 --replication-factor 1 >/dev/null
done

wait_for_nifi

root_id="$(api GET /nifi-api/flow/process-groups/root | jq -r '.processGroupFlow.id')"
user_id="$(api GET /nifi-api/tenants/users | jq -r --arg cert "$(openssl x509 -in "$CLIENT_CERT" -noout -subject -nameopt RFC2253 -nameopt sep_comma_plus_space | sed 's/^subject=//')" '.users[] | select(.component.identity == $cert) | .id' | head -1)"
if [[ -z "$user_id" ]]; then
  echo "Could not find NiFi user for client certificate identity" >&2
  exit 1
fi

ensure_policy "/process-groups/$root_id" read "$user_id"
ensure_policy "/process-groups/$root_id" write "$user_id"

pc_id="$(api GET /nifi-api/flow/process-groups/root | jq -r '.processGroupFlow.parameterContext.id // empty')"
if [[ -z "$pc_id" ]]; then
  body="$(mktemp)"
  jq -n --arg name "$PARAMETER_CONTEXT_NAME" --arg param "$PARAMETER_NAME" \
    '{revision:{version:0},component:{name:$name,description:"Divolte e2e schema sync target",parameters:[{parameter:{name:$param,value:"{}",sensitive:false}}]}}' > "$body"
  pc_resp="$(api POST /nifi-api/parameter-contexts "$body")"
  rm -f "$body"
  pc_id="$(jq -r '.id' <<<"$pc_resp")"

  pg="$(api GET "/nifi-api/process-groups/$root_id")"
  body="$(mktemp)"
  jq -n --arg id "$root_id" --arg pc "$pc_id" --argjson ver "$(jq -r '.revision.version' <<<"$pg")" \
    '{revision:{version:$ver},component:{id:$id,parameterContext:{id:$pc}}}' > "$body"
  api PUT "/nifi-api/process-groups/$root_id" "$body" >/dev/null
  rm -f "$body"
fi

cs_id="$(api GET "/nifi-api/flow/process-groups/$root_id/controller-services" | jq -r --arg name "$SCHEMA_REGISTRY_NAME" '.controllerServices[]? | select(.component.name == $name) | .id' | head -1)"
if [[ -z "$cs_id" ]]; then
  body="$(mktemp)"
  jq -n --arg name "$SCHEMA_REGISTRY_NAME" \
    '{revision:{version:0},component:{type:"org.apache.nifi.schemaregistry.services.AvroSchemaRegistry",bundle:{group:"org.apache.nifi",artifact:"nifi-registry-nar",version:"1.19.1"},name:$name,position:{x:0,y:0}}}' > "$body"
  cs_resp="$(api POST "/nifi-api/process-groups/$root_id/controller-services" "$body")"
  rm -f "$body"
  cs_id="$(jq -r '.id' <<<"$cs_resp")"
fi

# A repeat run may find the complete flow active. Stop the processors and the
# reader that depends on the schema registry before updating the registry or
# its parameter; NiFi rejects the reverse order with HTTP 409.
for processor_name in "$CONSUMER_NAME" "$PUBLISHER_NAME"; do
  existing_processor_id="$(api GET "/nifi-api/process-groups/$root_id/processors" |
    jq -r --arg name "$processor_name" '.processors[]? | select(.component.name == $name) | .id' | head -1)"
  if [[ -n "$existing_processor_id" ]]; then
    set_processor_state "$existing_processor_id" STOPPED
  fi
done
existing_avro_reader_id="$(api GET "/nifi-api/flow/process-groups/$root_id/controller-services?includeAncestorGroups=true" |
  jq -r --arg name "$AVRO_READER_NAME" '.controllerServices[]? | select(.component.name == $name) | .id' | head -1)"
if [[ -n "$existing_avro_reader_id" ]]; then
  set_controller_service_state "$existing_avro_reader_id" DISABLED
fi

cs="$(api GET "/nifi-api/controller-services/$cs_id")"
if [[ "$(jq -r '.component.state' <<<"$cs")" != "DISABLED" ]]; then
  set_controller_service_state "$cs_id" DISABLED
  cs="$(api GET "/nifi-api/controller-services/$cs_id")"
fi
body="$(mktemp)"
jq -n --arg id "$cs_id" --arg param "#{$PARAMETER_NAME}" --argjson ver "$(jq -r '.revision.version' <<<"$cs")" \
  '{revision:{version:$ver},component:{id:$id,properties:{"avro-reg-validated-field-names":"true","divolte":$param}}}' > "$body"
api PUT "/nifi-api/controller-services/$cs_id" "$body" >/dev/null
rm -f "$body"

schema="$(jq -c . "$SCHEMA_FILE")"
pc="$(api GET "/nifi-api/parameter-contexts/$pc_id")"
body="$(mktemp)"
jq -n --arg id "$pc_id" --arg name "$PARAMETER_NAME" --arg value "$schema" --argjson ver "$(jq -r '.revision.version' <<<"$pc")" \
  '{id:$id,revision:{version:$ver},component:{id:$id,parameters:[{parameter:{name:$name,value:$value,sensitive:false}}]}}' > "$body"
request_id="$(api POST "/nifi-api/parameter-contexts/$pc_id/update-requests" "$body" | jq -r '.request.requestId')"
rm -f "$body"
poll_parameter_update "$pc_id" "$request_id"

set_controller_service_state "$cs_id" ENABLED

avro_reader_id="$(ensure_controller_service "$root_id" "$AVRO_READER_NAME" \
  org.apache.nifi.avro.AvroReader nifi-record-serialization-services-nar)"
json_writer_id="$(ensure_controller_service "$root_id" "$JSON_WRITER_NAME" \
  org.apache.nifi.json.JsonRecordSetWriter nifi-record-serialization-services-nar)"
json_reader_id="$(ensure_controller_service "$root_id" "$JSON_READER_NAME" \
  org.apache.nifi.json.JsonTreeReader nifi-record-serialization-services-nar)"

configure_controller_service "$avro_reader_id" "$(jq -cn --arg registry "$cs_id" \
  '{"schema-access-strategy":"schema-name","schema-registry":$registry,"schema-name":"divolte"}')"
configure_controller_service "$json_writer_id" \
  '{"schema-access-strategy":"inherit-record-schema","Schema Write Strategy":"no-schema","Pretty Print JSON":"false","output-grouping":"output-array"}'
configure_controller_service "$json_reader_id" '{"schema-access-strategy":"infer-schema"}'

consumer_id="$(ensure_processor "$root_id" "$CONSUMER_NAME" \
  org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6 300 100)"
publisher_id="$(ensure_processor "$root_id" "$PUBLISHER_NAME" \
  org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6 700 100)"
failure_funnel_id="$(ensure_funnel "$root_id" 700 400)"

configure_processor "$consumer_id" "$(jq -cn \
  --arg brokers "$KAFKA_BROKERS" --arg topic "$AVRO_TOPIC" --arg group "$CONSUMER_GROUP" \
  --arg reader "$avro_reader_id" --arg writer "$json_writer_id" \
  '{"bootstrap.servers":$brokers,"topic":$topic,"group.id":$group,"record-reader":$reader,"record-writer":$writer,"auto.offset.reset":"earliest"}')" '[]'
configure_processor "$publisher_id" "$(jq -cn \
  --arg brokers "$KAFKA_BROKERS" --arg topic "$DEBUG_TOPIC" \
  --arg reader "$json_reader_id" --arg writer "$json_writer_id" \
  '{"bootstrap.servers":$brokers,"topic":$topic,"record-reader":$reader,"record-writer":$writer,"use-transactions":"false"}')" '["success"]'

ensure_connection "$root_id" "Decoded JSON" "$consumer_id" PROCESSOR "$publisher_id" PROCESSOR success
ensure_connection "$root_id" "Avro parse failures" "$consumer_id" PROCESSOR "$failure_funnel_id" FUNNEL parse.failure
ensure_connection "$root_id" "Kafka publish failures" "$publisher_id" PROCESSOR "$failure_funnel_id" FUNNEL failure

set_processor_state "$publisher_id" RUNNING
set_processor_state "$consumer_id" RUNNING

mkdir -p "$(dirname "$OUT_FILE")"
cat > "$OUT_FILE" <<EOF
NIFI_BASE_URL=$NIFI_URL
NIFI_PARAMETER_CONTEXT_ID=$pc_id
NIFI_PARAMETER_NAME=$PARAMETER_NAME
NIFI_CONTROLLER_SERVICE_ID=$cs_id
NIFI_AVRO_READER_ID=$avro_reader_id
NIFI_JSON_WRITER_ID=$json_writer_id
NIFI_JSON_READER_ID=$json_reader_id
NIFI_CONSUMER_PROCESSOR_ID=$consumer_id
NIFI_PUBLISHER_PROCESSOR_ID=$publisher_id
NIFI_DEBUG_TOPIC=$DEBUG_TOPIC
NIFI_CLIENT_CERT=$CLIENT_CERT
NIFI_CLIENT_KEY=$CLIENT_KEY
NIFI_CA_CERT=$CERT_DIR/ca.cert.pem
EOF

echo "Provisioned NiFi e2e flow:"
echo "  parameter context: $pc_id"
echo "  schema registry:   $cs_id"
echo "  Avro consumer:     $consumer_id"
echo "  JSON publisher:    $publisher_id"
echo "  debug topic:       $DEBUG_TOPIC"
echo "  env file:          $OUT_FILE"
