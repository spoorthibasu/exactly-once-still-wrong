#!/usr/bin/env bash
# Copyright 2026 Spoorthi Basu
# SPDX-License-Identifier: Apache-2.0
#
# A compacted topic keeps the record with the highest offset for each key.
# Three writers put three versions of customer 4412 on one partition: the
# insert (source version 1), the update (source version 2), and a backfill
# that replays the insert after the update. After the log cleaner runs, the
# topic keeps only the replayed insert, and the update is gone.
#
#   ./run.sh      saves its output to results/
set -euo pipefail
cd "$(dirname "$0")"
export COMPOSE_PROJECT_NAME=eosw-ch08-kafka COMPOSE_FILE=docker-compose.yml
OUT=results
TOPIC=customers
BIN=/opt/kafka/bin
rm -rf "${OUT}"; mkdir -p "${OUT}"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "${OUT}/run.log"; }
k() { docker compose exec -T kafka "$@"; }
consume() {  # every record in the topic, with offsets and timestamps
  k ${BIN}/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "${TOPIC}" \
    --from-beginning --timeout-ms 5000 \
    --formatter-property print.offset=true --formatter-property print.key=true \
    --formatter-property print.timestamp=true 2>/dev/null || true
}
produce() {  # produce <key> <value>, as its own producer, with the client's default settings
  printf '%s\t%s\n' "$1" "$2" | k ${BIN}/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic "${TOPIC}" --reader-property parse.key=true > /dev/null
}

log "Starting Kafka"
docker compose down -v > /dev/null 2>&1 || true
docker compose up -d > /dev/null 2>&1
until k ${BIN}/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do sleep 1; done
k ${BIN}/kafka-topics.sh --version 2>/dev/null | tee "${OUT}/versions.txt"
grep -h 'image:' docker-compose.yml | sed 's/^ *//' >> "${OUT}/versions.txt"

# The cleaner never touches the active segment, so the topic starts with long segments and the
# full history can be read before compaction. segment.ms is lowered afterwards.
log "Creating compacted topic ${TOPIC} (one partition)"
k ${BIN}/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "${TOPIC}" \
  --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact --config segment.ms=600000 \
  --config min.cleanable.dirty.ratio=0.01 --config min.compaction.lag.ms=0 > /dev/null
log "Producing three versions of customer 4412, each from its own producer"
produce 4412 '{"customer_id":4412,"plan":"trial","source_version":1,"writer":"pipeline, the 10:03 insert"}'
produce 4412 '{"customer_id":4412,"plan":"enterprise","source_version":2,"writer":"pipeline, the 10:07 update"}'
produce 4412 '{"customer_id":4412,"plan":"trial","source_version":1,"writer":"backfill, replaying the 10:03 insert"}'
consume > "${OUT}/before-compaction.txt"
log "Before compaction, records for key 4412: $(grep -c $'\t4412\t' "${OUT}/before-compaction.txt")"

log "Lowering segment.ms to 1 second, then producing one record for another key so the segment rolls"
k ${BIN}/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics \
  --entity-name "${TOPIC}" --add-config segment.ms=1000 > /dev/null
sleep 2
produce 9999 '{"customer_id":9999,"plan":"trial","source_version":1,"writer":"pipeline"}'

log "Waiting for the log cleaner (it checks every 15 seconds by default)"
start=${SECONDS}
until [ "$(consume | grep -c $'\t4412\t')" = 1 ]; do
  if (( SECONDS - start > 180 )); then log "The cleaner did not compact key 4412 within 180 s"; break; fi
  sleep 3
done
consume > "${OUT}/after-compaction.txt"
k ${BIN}/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type topics \
  --entity-name "${TOPIC}" > "${OUT}/topic-config.txt"
log "After compaction, records for key 4412: $(grep -c $'\t4412\t' "${OUT}/after-compaction.txt")"

k sh -c "${BIN}/kafka-dump-log.sh --print-data-log --files /tmp/kraft-combined-logs/${TOPIC}-0/00000000000000000000.log" \
  > "${OUT}/segment-dump.txt" 2>&1 || true
k sh -c 'cat /opt/kafka/logs/log-cleaner.log' > "${OUT}/log-cleaner.log" 2>&1 || true

log "Before:"; grep $'\t4412\t' "${OUT}/before-compaction.txt" | tee -a "${OUT}/run.log"
log "After:";  grep $'\t4412\t' "${OUT}/after-compaction.txt" | tee -a "${OUT}/run.log"
docker compose down -v > /dev/null 2>&1
log "Done"
