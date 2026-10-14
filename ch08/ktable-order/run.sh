#!/usr/bin/env bash
# Copyright 2026 Spoorthi Basu
# SPDX-License-Identifier: Apache-2.0
#
# Runs KTableOrder with Kafka Streams' TopologyTestDriver inside a pinned Maven
# image, so no broker is needed. Saves the output to results/.
set -euo pipefail
cd "$(dirname "$0")"
IMAGE=maven:3.9.16-eclipse-temurin-21@sha256:99e61abcff91a9b1333463bd8451fb18495d6eba9250ac66a338b518f8278320
rm -rf results target; mkdir -p results
# The named volume caches Maven downloads between runs; remove it with: docker volume rm eosw-ch08-m2
docker run --rm -v "$PWD":/work -w /work -v eosw-ch08-m2:/root/.m2 "${IMAGE}" \
  sh -c 'java -version 2>&1 | head -1; mvn -version | head -1; mvn -q -B compile exec:java' 2>&1 | tee results/output.txt
rm -rf target
