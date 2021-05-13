package com.distributedscheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.function.Function;

public class App {
    static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new JavaTimeModule());

    static final String INPUT_TOPIC = "distributed-scheduler-input";
    static final String OUTPUT_TOPIC = "distributed-scheduler-output";

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
//             .filterNot((k,v) -> v==null)
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
            Function<String, DelayedCommand> deserialize = (json) -> {
                try {
                    return objectMapper.readValue(json, DelayedCommand.class);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Error deserialising json: "+json);
                }
            };


            return new Transformer<String, String, KeyValue<String, String>>() {

                private KeyValueStore<String, String> stateStore;
                private ProcessorContext context;

                @SuppressWarnings("unchecked")
                @Override
                public void init(final ProcessorContext context) {
                    stateStore = (KeyValueStore<String, String>) context.getStateStore(stateStoreName);

                    this.context = context;

                    this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME,
                            timestamp -> {
                                System.out.println(".");
                                try (KeyValueIterator<String, String> iterator = stateStore.all()) {
                                    while (iterator.hasNext()) {
                                        KeyValue<String, String> keyValue = iterator.next();
                                        DelayedCommand command = deserialize.apply(keyValue.value);

//                                        if (command.getPublishAt().isBefore(Instant.now())) {
                                            context.forward(command.getPartitionKey(), command.getMessage());
//                                        }
                                    }
                                }
                            });
                }

                @Override
                public KeyValue<String, String> transform(final String key, final String value) {
                    stateStore.put(key, value);
                    return KeyValue.pair(null,null);
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
