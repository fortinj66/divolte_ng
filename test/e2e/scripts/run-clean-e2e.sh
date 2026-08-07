#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

NIFI_COMPOSE=test/e2e/nifi/compose.nifi.yml
NODE2_COMPOSE=test/e2e/compose.node2.yml
DRUID_COMPOSE=test/e2e/druid/compose.druid.yml

log() {
  printf '\n== %s ==\n' "$*"
}

wait_http() {
  local url="$1"
  local name="$2"
  local attempts="${3:-120}"
  for _ in $(seq 1 "$attempts"); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "$name did not become ready at $url" >&2
  return 1
}

diagnose_failure() {
  local status=$?
  if (( status != 0 )); then
    echo >&2
    echo "Clean E2E failed; scoped container state:" >&2
    podman ps -a --format '{{.Names}}\t{{.Status}}' |
      rg '^(divolte-|druid-)' >&2 || true
  fi
  exit "$status"
}
trap diagnose_failure EXIT

log "teardown prior test environment"
podman compose -f "$NODE2_COMPOSE" down -v >/dev/null 2>&1 || true
podman compose -f "$NIFI_COMPOSE" down -v >/dev/null 2>&1 || true
podman compose -f compose.yml down -v >/dev/null 2>&1 || true
podman compose -f "$DRUID_COMPOSE" down -v >/dev/null 2>&1 || true

for pod in pod_e2e pod_nifi pod_divolte_ng pod_druid pod_druid-cluster; do
  podman pod rm -f "$pod" >/dev/null 2>&1 || true
done
for container in \
  divolte-ng-collector-node2 divolte-e2e-nifi \
  divolte-ng-collector divolte-ng-kafka divolte-ng-mariadb \
  druid-postgres druid-zookeeper druid-master druid-historical \
  druid-middlemanager druid-broker druid-router druid-data druid-query; do
  podman rm -f "$container" >/dev/null 2>&1 || true
done
for volume in \
  divolte_ng_mariadb_data divolte_ng_kafka_data \
  nifi_nifi_state nifi_nifi_database_repository nifi_nifi_flowfile_repository \
  nifi_nifi_content_repository nifi_nifi_provenance_repository \
  druid_metadata_data druid_master_var druid_historical_var \
  druid_middlemanager_var druid_broker_var druid_router_var druid_druid_shared \
  druid-cluster_metadata_data druid-cluster_master_var druid-cluster_data_var \
  druid-cluster_query_var druid-cluster_druid_shared; do
  podman volume rm "$volume" >/dev/null 2>&1 || true
done
podman network rm nifi_default divolte_ng_default druid-cluster_default \
  >/dev/null 2>&1 || true

if podman ps -a --format '{{.Names}}' | rg -q '^(divolte-|druid-)'; then
  echo "scoped containers remain after teardown" >&2
  podman ps -a --format '{{.Names}}\t{{.Status}}' |
    rg '^(divolte-|druid-)' >&2
  exit 1
fi

log "start Druid infrastructure"
podman compose -f "$DRUID_COMPOSE" up -d

log "build and start Divolte, MariaDB, and Kafka"
podman compose -f compose.yml up -d --build
wait_http http://localhost:8290/ping "Divolte collector"
wait_http http://localhost:8291/schema "Divolte admin"

log "Go unit and integration tests"
go test ./...

log "Divolte baseline, browser, Kafka, and schema lifecycle"
test/e2e/scripts/run-divolte-e2e.sh

log "start and validate second collector"
podman compose -f "$NODE2_COMPOSE" up -d
test/e2e/scripts/validate-multi-node.sh

log "generate NiFi certificates and start NiFi"
test/e2e/nifi/generate-certs.sh
podman compose -f "$NIFI_COMPOSE" up -d

log "provision NiFi from empty state"
test/e2e/nifi/provision-flow.sh

log "validate NiFi Avro-to-JSON flow"
test/e2e/nifi/validate-avro-json-flow.sh

log "validate Divolte-to-NiFi schema synchronization"
test/e2e/nifi/validate-schema-sync-flow.sh

log "validate Druid supervisors and both ingestion paths"
test/e2e/scripts/validate-druid-flow.sh

trap - EXIT
log "clean E2E passed"
podman ps --format '{{.Names}}\t{{.Status}}' |
  rg '^(divolte-|druid-)' | sort
