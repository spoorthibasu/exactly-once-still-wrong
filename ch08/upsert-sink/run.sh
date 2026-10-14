#!/usr/bin/env bash
# Copyright 2026 Spoorthi Basu
# SPDX-License-Identifier: Apache-2.0
#
# An upsert keeps the last write it receives. Two versions of customer 4412
# arrive out of order: the update first, then the insert. A plain upsert keeps
# the insert. The same upsert with a version check keeps the update.
#
#   ./run.sh      saves its output to results/
set -euo pipefail
cd "$(dirname "$0")"
export COMPOSE_PROJECT_NAME=eosw-ch08-upsert COMPOSE_FILE=docker-compose.yml
OUT=results
rm -rf "${OUT}"; mkdir -p "${OUT}"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "${OUT}/run.log"; }
psql() { docker compose exec -T postgres psql -U postgres -v ON_ERROR_STOP=1 --echo-all "$@"; }

log "Starting PostgreSQL with pgvector"
docker compose down -v > /dev/null 2>&1 || true
docker compose up -d --wait > /dev/null 2>&1
{ psql -At -c 'SELECT version()'; psql -At -c "SELECT 'pgvector ' || default_version FROM pg_available_extensions WHERE name = 'vector'"; } \
  | grep -v '^SELECT' > "${OUT}/versions.txt"
grep -h 'image:' docker-compose.yml | sed 's/^ *//' >> "${OUT}/versions.txt"

for case in plain-upsert versioned-upsert; do
  log "Running sql/${case}.sql on a fresh table"
  psql < sql/schema.sql > /dev/null 2>&1
  psql < "sql/${case}.sql" > "${OUT}/${case}.txt" 2>&1
  log "Stored row: $(sed -n '/^SELECT customer_id, plan, source_version FROM customer_docs;/,/^(1 row)/p' "${OUT}/${case}.txt" | sed -n 4p | tr -s ' ')"
done
docker compose down -v > /dev/null 2>&1
log "Done"
