# Exactly Once, Still Wrong: companion code

Companion code for *Exactly Once, Still Wrong* by Spoorthi Basu. Each chapter that has hands-on material gets its own directory.

| Directory | Chapter |
| --- | --- |
| [`ch06/`](ch06/) | Chapter 6: What the Guarantees Promise, and Where They Stop. Reproduces FLINK-38450, a duplicate-row bug in the Flink CDC Iceberg sink, on Flink CDC 3.6.0 and shows it fixed on 3.7.0. |
| [`ch08/`](ch08/) | Chapter 8: Last Write Wins, and What Last Means. Shows a compacted Kafka topic keeping an older version, an upsert keeping a late write, and a Kafka Streams KTable applying updates in arrival order, and, for the last two, the version checks that prevent it. |

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
