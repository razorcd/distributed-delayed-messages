package com.distributedscheduler;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Optional;
import java.util.Properties;

public class App {

    static final String OUTPUT_TOPIC = "distributed-scheduler-output";
    static final String INPUT_TOPIC = "distributed-scheduler-input";

    static final StoreBuilder<KeyValueStore<String, String>> distributedSchedulerStore = Stores
            .keyValueStoreBuilder(
                    Stores.persistentKeyValueStore("distributed-scheduler-store"),
                    Serdes.String(),
                    Serdes.String())
            .withCachingEnabled();

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "distributed-scheduler-appid");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "distributed-scheduler-clientid");

        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Topology topology = getTopology();
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(distributedSchedulerStore);

        final KStream<String, String> input = builder.stream(INPUT_TOPIC);

        input.peek((k,v) -> System.out.println("Input value: "+v))
//             .selectKey((k, v) -> "111")
//             .groupByKey()
//             .reduce((v1, v2) -> v2)
             .transform(new DistributedSchedulerTransformerSupplier(distributedSchedulerStore.name()), distributedSchedulerStore.name())
             .peek((k,v) -> System.out.println("Output value: "+v))
             .to(OUTPUT_TOPIC);

        return builder.build();
    }


    private static final class DistributedSchedulerTransformerSupplier implements TransformerSupplier<String, String, KeyValue<String, String>> {

        final private String stateStoreName;

        DistributedSchedulerTransformerSupplier(final String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public Transformer<String, String, KeyValue<String, String>> get() {
            return new Transformer<String, String, KeyValue<String, String>>() {

                private KeyValueStore<String, String> stateStore;

                @SuppressWarnings("unchecked")
                @Override
                public void init(final ProcessorContext context) {
                    stateStore = (KeyValueStore<String, String>) context.getStateStore(stateStoreName);
                }

                @Override
                public KeyValue<String, String> transform(final String key, final String value) {
                    // For simplification (and unlike the traditional wordcount) we assume that the value is
                    // a single word, i.e. we don't split the value by whitespace into potentially one or more
                    // words.
                    final Optional<String> count = Optional.ofNullable(stateStore.get(value));
                    stateStore.put(value, count.orElse("_"));
                    return KeyValue.pair(value, count.orElse("_"));
                }

                @Override
                public void close() {
                    // Note: The store should NOT be closed manually here via `stateStore.close()`!
                    // The Kafka Streams API will automatically close stores when necessary.
                }
            };
        }

    }


}
