# A compacted Kafka topic keeps an older version

A compacted topic keeps, for each key, the record with the highest offset. Nothing in compaction looks at what the value says or when the change happened in the source. If an older version of a key reaches the topic after a newer one, compaction keeps the older one and removes the newer one.

## What the run does

One Kafka broker (Apache Kafka 4.3.1, KRaft, the image's default configuration) and one compacted topic with one partition. Three producers write customer 4412, in this order:

| Offset | Writer | Value |
| --- | --- | --- |
| 0 | the pipeline, the 10:03 insert | `trial`, source version 1 |
| 1 | the pipeline, the 10:07 update | `enterprise`, source version 2 |
| 2 | a backfill, replaying the 10:03 insert | `trial`, source version 1 |

The script reads the whole topic, lowers `segment.ms` so the segment holding those records rolls, waits for the log cleaner, and reads the topic again.

## Result (`results/`)

- Before compaction, the topic held all three records for 4412 (`before-compaction.txt`).
- After compaction, it held one: offset 2, the replayed `trial` (`after-compaction.txt`). The `enterprise` record, which the source says came last, is gone, and so is the evidence that it existed.
- The cleaner's own log reports 3 messages in and 1 out for the cleaned section (`log-cleaner.log`). `segment-dump.txt` shows that offsets 0 and 1 remain only as empty batches, which the cleaner keeps to preserve each producer's state.

Each producer here stamps records with its own clock, so the replayed record also has the latest timestamp. The cleaner does not use timestamps at all: it keys on offsets, as the source below shows.

## Where this is in Kafka's source (4.3.1)

- The cleaner builds a map from each key to the highest offset it has seen: [`Cleaner.java` lines 734-736](https://github.com/apache/kafka/blob/4.3.1/storage/src/main/java/org/apache/kafka/storage/internals/log/Cleaner.java#L734-L736).
- A record is kept only if its offset is at least the one in that map: [`Cleaner.java` lines 505-522](https://github.com/apache/kafka/blob/4.3.1/storage/src/main/java/org/apache/kafka/storage/internals/log/Cleaner.java#L505-L522).
- The last batch of each active producer is kept as an empty batch: [`Cleaner.java` lines 335-351](https://github.com/apache/kafka/blob/4.3.1/storage/src/main/java/org/apache/kafka/storage/internals/log/Cleaner.java#L335-L351).

## Run it

```bash
./run.sh
```

It needs Docker with the Compose plugin, takes about a minute, and removes its container and volume at the end.
