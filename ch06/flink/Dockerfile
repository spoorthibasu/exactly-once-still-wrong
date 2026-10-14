# Copyright 2026 Spoorthi Basu
# SPDX-License-Identifier: Apache-2.0
#
# Flink 1.20 with the Flink CDC distribution for one release, its MySQL and
# Iceberg pipeline connectors, the MySQL JDBC driver, and the Hadoop JAR a
# Hadoop catalog needs. Build with --build-arg FLINK_CDC_VERSION=3.6.0 or 3.7.0.
# The base image is pinned by tag and by its multi-platform digest.
FROM flink:1.20.3-scala_2.12-java11@sha256:1f3ada93c2d60ae65e8d1de2e099844e8905650788b3e560cfcee48c112eb4d2

ARG FLINK_CDC_VERSION

ENV FLINK_HOME=/opt/flink \
    FLINK_CDC_HOME=/opt/flink-cdc

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN set -eux; \
    case "${FLINK_CDC_VERSION}" in \
      3.6.0) \
        dist_sha512=1d9ea9eeb9f4330e3f70ba5396aaf53e2778ec8863f5c9a811ab6e1fda1a59fb553af31583dee51553eca2d5466cbd185944a14be8a73fe820a0000cef3cabd6; \
        mysql_sha512=8d1670e721d3b3d50a4b626ddcf7d1df950867d795caf5c5d75da527bb43e8e3b9c2b0f87623afca134eb0ea72be43cb3e483824e8d87ebabf2fcc6f1479b1aa; \
        iceberg_sha512=99cde9f117e32ad0d0727433eefc3113e1d4817f4d8dbbba11c2b9239de1b8ab49ca36328c4d16f5404c9ce219897948d5ca8020508e076ab9017284e05694ce ;; \
      3.7.0) \
        dist_sha512=6f6b3db009cff5a0d308ae8b0f0d53c73c57a51996713186f0ea621b63c941b4d81b2cc40847d1787c3260635b3b62d566c6a5d8d4b01653c86b433597b2e610; \
        mysql_sha512=061a089b52beaca57ec5a4d1be5ff3de966c375023be6cabf47c5028150b9aa5e563f64ec8ce5bd6bf65e9664f4aac622f090bf50aca41aa48b6a35bdecccf18; \
        iceberg_sha512=ff82692e1d58d764d42378cb3ef69c8639eaf40c94396e0cf5a437b59afb05d1a9dd693fa11cc1b44dac511f3d2a980c627d8ad1c4ff44a5506011bae100b502 ;; \
      *) echo "FLINK_CDC_VERSION must be 3.6.0 or 3.7.0" >&2; exit 1 ;; \
    esac; \
    fetch() { wget -nv --tries=3 --timeout=60 -O "$2" "$1"; echo "$3  $2" | sha512sum -c -; }; \
    maven=https://repo1.maven.org/maven2; \
    v="${FLINK_CDC_VERSION}-1.20"; \
    fetch "https://archive.apache.org/dist/flink/flink-cdc-${FLINK_CDC_VERSION}/flink-cdc-${v}-bin.tar.gz" /tmp/flink-cdc.tar.gz "${dist_sha512}"; \
    mkdir -p "${FLINK_CDC_HOME}"; \
    tar -xzf /tmp/flink-cdc.tar.gz -C "${FLINK_CDC_HOME}" --strip-components=1; \
    rm /tmp/flink-cdc.tar.gz; \
    fetch "${maven}/org/apache/flink/flink-cdc-pipeline-connector-mysql/${v}/flink-cdc-pipeline-connector-mysql-${v}.jar" \
      "${FLINK_CDC_HOME}/lib/flink-cdc-pipeline-connector-mysql-${v}.jar" "${mysql_sha512}"; \
    fetch "${maven}/org/apache/flink/flink-cdc-pipeline-connector-iceberg/${v}/flink-cdc-pipeline-connector-iceberg-${v}.jar" \
      "${FLINK_CDC_HOME}/lib/flink-cdc-pipeline-connector-iceberg-${v}.jar" "${iceberg_sha512}"; \
    fetch "${maven}/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar" \
      "${FLINK_HOME}/lib/mysql-connector-java-8.0.27.jar" \
      5d271c5fb8812d20975c85341ffe7ab5807690964c509f5b8ec0dd45e1c321bdc4597a72387ba38f4f804d22711165cbc8469726fbe583b283ec42e547bf4337; \
    fetch "${maven}/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" \
      "${FLINK_HOME}/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" \
      c04d217fb53123054c58c5c492cc8d87e75aa72b798e3b4858757cb4d389ccd9866c224662a012e1e9b012c05e481372e2bbc97cd45746f924814733d864591f; \
    chown -R flink:flink "${FLINK_CDC_HOME}"; \
    chown flink:flink "${FLINK_HOME}/lib/mysql-connector-java-8.0.27.jar" "${FLINK_HOME}/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"; \
    mkdir -p /opt/warehouse; \
    chmod 777 /opt/warehouse

WORKDIR /opt/flink-cdc
