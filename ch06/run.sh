#!/usr/bin/env bash
# Copyright 2026 Spoorthi Basu
# SPDX-License-Identifier: Apache-2.0
#
# Runs Chapter 6's Try It Yourself end to end for one Flink CDC release. It
# saves the outputs of Examples 6-1, 6-2, 6-3, and 6-5 to results/<version>/,
# and its logs and other raw output to run-logs/<version>/, which git ignores.
#
#   ./run.sh 3.6.0    the release before the fix
#   ./run.sh 3.7.0    the release with the fix
#
# Needs Docker with the Compose plugin (docker compose), bash, curl, and
# python3 on the host. Set FLINK_UI_PORT if port 8081 is taken.
set -euo pipefail

usage() { echo "usage: $0 <3.6.0|3.7.0>" >&2; exit 2; }
[ $# -eq 1 ] || usage
VERSION="$1"
case "${VERSION}" in
  3.6.0 | 3.7.0) ;;
  *) usage ;;
esac

cd "$(dirname "$0")"
export FLINK_CDC_VERSION="${VERSION}"
export COMPOSE_PROJECT_NAME=eosw-ch06 COMPOSE_FILE=docker-compose.yml

for tool in docker curl python3; do
  command -v "${tool}" > /dev/null || { echo "run.sh needs ${tool} on the PATH" >&2; exit 1; }
done
python3 -c '' || { echo "python3 is on the PATH but does not run" >&2; exit 1; }
docker info > /dev/null 2>&1 || { echo "Cannot reach the Docker daemon (docker info failed)" >&2; exit 1; }
docker compose version > /dev/null 2>&1 || { echo "run.sh needs the Docker Compose plugin (docker compose)" >&2; exit 1; }

RESULTS="results/${VERSION}"
OUT="run-logs/${VERSION}"
REST="http://localhost:${FLINK_UI_PORT:-8081}"
TABLE_DIR="/opt/warehouse/crm/customers"
JOB=""
FIRST=0

rm -rf "${RESULTS}" "${OUT}"
mkdir -p "${RESULTS}" "${OUT}/logs"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "${OUT}/run.log"; }
# Terminal only, so that run.log holds no host paths.
say_dir() { echo "The paths and docker compose commands above are relative to $(pwd)."; }

save_logs() {
  local service
  for service in jobmanager taskmanager mysql; do
    docker compose logs --no-color "${service}" > "${OUT}/logs/${service}.log" 2>&1 || true
  done
}

on_error() {
  log "FAILED at line $1. Logs are in ${OUT}/logs/. Stop the stack with: docker compose down -v"
  save_logs
  say_dir
}
on_interrupt() {
  log "Interrupted. The stack may still be running; stop it with: docker compose down -v"
  save_logs
  say_dir
  exit 130
}
trap 'on_error ${LINENO}' ERR
trap on_interrupt INT TERM

json() { python3 -c 'import json, sys; d = json.load(sys.stdin); print(eval(sys.argv[1]))' "$1"; }

rest() { curl -sf --noproxy '*' --max-time 10 "${REST}$1" || return; }

compose() {  # compose <args...>: docker compose, logged; shows the end of the log on failure
  docker compose "$@" >> "${OUT}/logs/compose.log" 2>&1 \
    || { tail -n 5 "${OUT}/logs/compose.log" >&2; return 1; }
}

latest_completed_checkpoint() {
  rest "/jobs/${JOB}/checkpoints" \
    | json '(d["latest"]["completed"] or {}).get("id", 0)' 2>/dev/null || echo 0
}

# Highest checkpoint the sink has marked as committed in the Iceberg table.
committed_checkpoint() {
  local v
  v=$(docker compose exec -T jobmanager cat "${TABLE_DIR}/metadata/version-hint.text" 2>/dev/null) || { echo 0; return; }
  docker compose exec -T jobmanager cat "${TABLE_DIR}/metadata/v${v}.metadata.json" \
    | json 'max([int(s["summary"].get("flink.max-committed-checkpoint-id", 0)) for s in d.get("snapshots", [])], default=0)' \
    2>/dev/null || echo 0
}

slots_available() { rest /overview | json 'd["slots-available"]' | grep -qv '^0$'; }
job_running() { [ "$(rest "/jobs/${JOB}" | json 'd["state"]')" = "RUNNING" ]; }
first_checkpoint() { FIRST=$(latest_completed_checkpoint); (( FIRST > 0 )); }
checkpoint_after() { (( $(latest_completed_checkpoint) > $1 )); }
committed_at_least() { (( $(committed_checkpoint) >= $1 )); }

wait_for() {  # wait_for <description> <timeout seconds> <command...>
  local what="$1" timeout="$2"; shift 2
  local start=${SECONDS}
  until "$@" > /dev/null 2>&1; do
    if (( SECONDS - start > timeout )); then
      log "Timed out after ${timeout}s waiting for ${what}"
      return 1
    fi
    sleep 1
  done
}

spark_sql() {  # spark_sql <sql file under sql/> <output file>
  docker compose exec -T spark /opt/spark/bin/spark-sql -f "/opt/ch06/sql/$1" \
    > "$2" 2>> "${OUT}/logs/spark-sql.log" || return
}

log "Flink CDC ${VERSION}: starting from a fresh stack"
compose down -v --remove-orphans
log "Building images (the first build downloads the base images and the JARs)"
compose build
compose up -d

wait_for "the Flink REST API and a TaskManager" 300 slots_available
wait_for "the crm.customers table in MySQL" 300 \
  docker compose exec -T mysql mysql -uroot -pcdc-demo -e "SELECT 1 FROM crm.customers LIMIT 1"
log "MySQL, Flink, and Spark are up"

log "Submitting the pipeline: ./bin/flink-cdc.sh customers-to-iceberg.yaml"
docker compose exec -T jobmanager ./bin/flink-cdc.sh customers-to-iceberg.yaml 2>&1 | tee "${OUT}/submit.txt"
JOB=$(sed -n 's/^Job ID: *//p' "${OUT}/submit.txt" | tr -d '\r')
[ -n "${JOB}" ] || { log "No Job ID in ${OUT}/submit.txt"; false; }
log "Job ID ${JOB}"

wait_for "the job to be RUNNING" 120 job_running
rest "/jobs/${JOB}/checkpoints/config" > "${OUT}/checkpoint-config.json"
log "Job is RUNNING, checkpoint interval $(json 'd["interval"]' < "${OUT}/checkpoint-config.json") ms"

log "Waiting for a checkpoint to complete (Flink starts the first one up to one interval after the job starts)"
wait_for "a checkpoint to complete" 600 first_checkpoint
log "Checkpoint ${FIRST} completed; running Example 6-5 now"

log "Example 6-5 start"
docker compose exec -T mysql mysql -uroot -pcdc-demo -vvv < sql/example-6-5.sql > "${RESULTS}/example-6-5.txt" 2>&1
log "Example 6-5 end"
docker compose exec -T mysql mysql -uroot -pcdc-demo -e "SELECT * FROM crm.customers" \
  > "${OUT}/mysql-customers.txt" 2>&1

NEXT=$((FIRST + 1))
log "Waiting for checkpoint ${NEXT} to complete"
wait_for "checkpoint ${NEXT} to complete" 600 checkpoint_after "${FIRST}"
log "Checkpoint $(latest_completed_checkpoint) completed"

wait_for "the sink to commit checkpoint ${NEXT} to Iceberg" 120 committed_at_least "${NEXT}"
log "Iceberg table records checkpoint ${NEXT} as committed"
rest "/jobs/${JOB}/checkpoints" > "${OUT}/checkpoints.json"
rest "/jobs/${JOB}/exceptions" > "${OUT}/job-exceptions.json"
containers=$(docker compose ps -q | tr '\n' ' ')
# shellcheck disable=SC2086  # one argument per container ID
[ -n "${containers// }" ] && docker stats --no-stream --format '{{.Name}}  {{.MemUsage}}  {{.CPUPerc}}' ${containers} \
  > "${OUT}/docker-stats.txt" 2>&1 || true

log "Running Examples 6-1, 6-2, and 6-3 in Spark SQL"
spark_sql example-6-1.sql "${RESULTS}/example-6-1.txt"
spark_sql example-6-2.sql "${RESULTS}/example-6-2.txt"
spark_sql example-6-3.sql "${RESULTS}/example-6-3.txt"
spark_sql evidence/table.sql "${OUT}/evidence-table.txt"

mkdir -p "${OUT}/iceberg-metadata"
docker compose exec -T jobmanager sh -c "cd ${TABLE_DIR}/metadata && tar -cf - version-hint.text *.metadata.json" \
  | tar -xf - -C "${OUT}/iceberg-metadata"

{
  echo "flink-cdc lib: $(docker compose exec -T jobmanager ls /opt/flink-cdc/lib | tr '\n' ' ')"
  echo "flink lib: $(docker compose exec -T jobmanager ls /opt/flink/lib | tr '\n' ' ')"
  echo "flink: $(docker compose exec -T jobmanager /opt/flink/bin/flink --version 2>/dev/null | tr -d '\r')"
  echo "mysql: $(docker compose exec -T mysql mysql -uroot -pcdc-demo -N -e 'SELECT VERSION()' 2>/dev/null)"
  echo "spark: $(docker compose exec -T spark ls /opt/spark/jars | grep -E '^(spark-core|iceberg-spark)' | tr '\n' ' ')"
  echo "base images: $(grep -h '^FROM' flink/Dockerfile spark/Dockerfile | tr '\n' ' ')$(grep -E '^ *image: mysql' docker-compose.yml | tr -s ' ')"
} > "${OUT}/versions.txt"

save_logs
duplicated=$(tail -n +2 "${RESULTS}/example-6-1.txt" | grep -c . || true)
log "Example 6-1 returned ${duplicated} duplicated key(s):"
tail -n +2 "${RESULTS}/example-6-1.txt" | tee -a "${OUT}/run.log"
problems=$(json 'd["counts"]["restored"] + d["counts"]["failed"]' < "${OUT}/checkpoints.json")
if (( problems > 0 )); then
  log "Warning: the job restored or failed a checkpoint during the run (see ${OUT}/checkpoints.json)"
fi
log "Done. Example outputs are in ${RESULTS}/, logs and raw output in ${OUT}/. The stack is still running; stop it with: docker compose down -v"
say_dir
