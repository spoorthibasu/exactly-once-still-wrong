# Copyright 2026 Spoorthi Basu
# SPDX-License-Identifier: Apache-2.0
#
# Spark SQL with the Iceberg runtime, reading the same warehouse as Flink
# through a Hadoop catalog named lake. The base image is pinned by tag and by
# its multi-platform digest.
FROM apache/spark:3.5.8-scala2.12-java17-ubuntu@sha256:9577096950d62dd59994f84cc3a0902fdae994a4bf238933f2fdca22053e5adb

USER root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# /opt/warehouse must be mode 777 here as well as in the Flink image. The
# spark service has no depends_on, so it can start first and create the shared
# volume, which then takes this directory's owner and mode. Flink writes to it
# as uid 9999.
RUN set -eux; \
    jar=/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar; \
    wget -nv --tries=3 --timeout=60 -O "${jar}" https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar; \
    echo "2ff0b9637b671a5a28224c57e8393a1e6a65796ca1fe859b32359f9fe766932534c69d35e4ebcc166b4498242402d9669cea0d6508593c992691c5598089f709  ${jar}" | sha512sum -c -; \
    mkdir -p /opt/spark/conf /opt/warehouse; \
    chmod 777 /opt/warehouse

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
