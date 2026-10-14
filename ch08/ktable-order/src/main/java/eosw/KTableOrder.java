/*
 * Copyright 2026 Spoorthi Basu
 * SPDX-License-Identifier: Apache-2.0
 */
package eosw;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;

/**
 * Two versions of customer 4412 reach a KTable out of order: the 10:07 update first, then the
 * 10:03 insert. Each record carries its source time as the record timestamp. The same input runs
 * through a KTable with the default store and through one with a versioned store.
 */
public final class KTableOrder {

    private static final String INPUT = "customers";
    private static final String OUTPUT = "customer-updates";
    private static final String STORE = "customers-store";
    private static final Instant INSERT_TIME = Instant.parse("2026-10-13T10:03:00Z");
    private static final Instant UPDATE_TIME = Instant.parse("2026-10-13T10:07:00Z");
    private static final Instant BETWEEN = Instant.parse("2026-10-13T10:05:00Z");

    public static void main(String[] args) {
        System.out.println("Input, in arrival order:");
        System.out.println("  4412 -> enterprise, record time " + UPDATE_TIME + " (the update)");
        System.out.println("  4412 -> trial, record time " + INSERT_TIME + " (the insert, arriving late)");

        System.out.println();
        System.out.println("KTable with the default (timestamped key-value) store:");
        Materialized<String, String, KeyValueStore<Bytes, byte[]>> plain =
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(STORE);
        try (TopologyTestDriver driver = run(plain)) {
            ValueAndTimestamp<String> latest =
                    driver.<String, String>getTimestampedKeyValueStore(STORE).get("4412");
            System.out.println("  store value: " + latest.value()
                    + ", record time " + Instant.ofEpochMilli(latest.timestamp()));
            System.out.println("  updates sent downstream: " + updates(driver));
        }

        System.out.println();
        System.out.println("KTable with a versioned store (history retention 1 hour):");
        Materialized<String, String, KeyValueStore<Bytes, byte[]>> versioned =
                Materialized.<String, String>as(
                        Stores.persistentVersionedKeyValueStore(STORE, Duration.ofHours(1)));
        try (TopologyTestDriver driver = run(versioned)) {
            VersionedKeyValueStore<String, String> store = driver.getVersionedKeyValueStore(STORE);
            VersionedRecord<String> latest = store.get("4412");
            VersionedRecord<String> asOf = store.get("4412", BETWEEN.toEpochMilli());
            System.out.println("  store value: " + latest.value()
                    + ", valid from " + Instant.ofEpochMilli(latest.timestamp()));
            System.out.println("  store value as of " + BETWEEN + ": " + asOf.value()
                    + ", valid from " + Instant.ofEpochMilli(asOf.timestamp()));
            System.out.println("  updates sent downstream: " + updates(driver));
        }
    }

    private static TopologyTestDriver run(
            Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(INPUT, Consumed.with(Serdes.String(), Serdes.String()),
                        materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
                .toStream()
                .to(OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-order");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props);

        TestInputTopic<String, String> in =
                driver.createInputTopic(INPUT, new StringSerializer(), new StringSerializer());
        in.pipeInput("4412", "enterprise", UPDATE_TIME);
        in.pipeInput("4412", "trial", INSERT_TIME);
        return driver;
    }

    private static String updates(TopologyTestDriver driver) {
        TestOutputTopic<String, String> out =
                driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
        return out.readValuesToList().toString();
    }
}
