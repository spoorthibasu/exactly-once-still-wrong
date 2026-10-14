# A KTable applies updates in arrival order

A Kafka Streams KTable with the default store keeps whatever update arrives last for a key, even when its record timestamp is older. A versioned store keeps the version with the latest timestamp as the current value and keeps the late one as history. In Kafka 4.3.1 the KTable still forwards the late update downstream, marked as not the latest.

## What the run does

A small program (`src/main/java/eosw/KTableOrder.java`) builds a KTable over a `customers` topic and runs it with Kafka Streams' `TopologyTestDriver`, so no broker is needed. Two versions of customer 4412 arrive out of order: `enterprise` with record time 10:07, then `trial` with record time 10:03. The same input runs through a KTable with the default (timestamped key-value) store, and through one with a versioned store that keeps one hour of history. For each, the program prints the store's value and the updates that `toStream()` sends downstream.

## Result (`results/output.txt`)

| | Default store | Versioned store |
| --- | --- | --- |
| Current value for 4412 | `trial` (record time 10:03) | `enterprise` (valid from 10:07) |
| Value as of 10:05 | not kept | `trial` |
| Updates sent downstream | `enterprise`, then `trial` | `enterprise`, then `trial` |

With both stores Kafka Streams logs `Detected out-of-order KTable update` for the late record, then applies it.

## Where this is in Kafka's source (4.3.1)

- `KTableSource` warns when a record's timestamp is older than the stored one, then puts it anyway: [`KTableSource.java` lines 141-171](https://github.com/apache/kafka/blob/4.3.1/streams/src/main/java/org/apache/kafka/streams/kstream/internals/KTableSource.java#L141-L171).
- It forwards every record the store accepted, with a flag saying whether it is the latest: [`KTableSource.java` lines 173-174](https://github.com/apache/kafka/blob/4.3.1/streams/src/main/java/org/apache/kafka/streams/kstream/internals/KTableSource.java#L173-L174). A versioned store accepts a late record into its history, so the late `trial` goes downstream with that flag false. `toStream()` passes it on; table-table joins and `groupBy` check the flag.
- Versioned state stores: [KIP-889](https://cwiki.apache.org/confluence/display/KAFKA/KIP-889%3A+Versioned+State+Stores); their DSL semantics: [KIP-914](https://cwiki.apache.org/confluence/display/KAFKA/KIP-914%3A+DSL+Processor+Semantics+for+Versioned+Stores).

The other two warnings in the output, about the processing exception handler and the state directory, come from the test setup and do not affect the result.

## Run it

```bash
./run.sh
```

It needs Docker. It runs Maven 3.9.16 on Java 21 in a container, against Kafka Streams 4.3.1. Maven downloads are cached in a Docker volume named `eosw-ch08-m2`; remove it with `docker volume rm eosw-ch08-m2`.
