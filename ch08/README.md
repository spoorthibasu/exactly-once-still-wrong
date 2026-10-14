# Chapter 8: Last Write Wins, and What Last Means

Hands-on material for Chapter 8. Each reproduction shows what a system that keeps the latest value per key does when two versions of one key arrive out of order. Two of them also show a write that chooses by version instead.

| Directory | What it shows | Pinned to |
| --- | --- | --- |
| [`kafka-compaction/`](kafka-compaction/) | A compacted topic keeps the record with the highest offset, so an older version written later wins and the newer one is erased. | Apache Kafka 4.3.1 |
| [`upsert-sink/`](upsert-sink/) | A plain upsert keeps the last write it receives; the same upsert with a version check keeps the newer version. | PostgreSQL 18.6, pgvector 0.8.7 |
| [`ktable-order/`](ktable-order/) | A KTable applies updates in arrival order; a versioned store keeps the newer version as current, but the late update still goes downstream. | Kafka Streams 4.3.1 |

Each directory has a `run.sh` that saves its output to `results/`, and a README with the result and the source lines behind it.
