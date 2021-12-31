package com.distributedscheduler;

import com.distributedscheduler.event.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class App {
    static final Clock CLOCK = AppConfig.clock;
    static final Serde serde = new Serde(AppConfig.objectMapper.get());

    static final String INPUT_TOPIC = "distributed-scheduler-input";
    static final String OUTPUT_TOPIC = "distributed-scheduler-output";

    static final StoreBuilder<KeyValueStore<String, String>> distributedSchedulerStore = Stores
            .keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore("distributed-scheduler-store"),
                    Serdes.String(),
                    Serdes.String())
            .withCachingDisabled()
//            .withLoggingDisabled()
            ;

    public static void main(final String[] args) {


        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "distributed-scheduler-appid");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "distributed-scheduler-clientid");

        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, "GROUPSID0");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams2");

        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Topology topology = getTopology(CLOCK);
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static Topology getTopology(Clock clock) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(distributedSchedulerStore);

        final KStream<String, String> input = builder.stream(INPUT_TOPIC);

        input.peek((k,v) -> System.out.println("Input value k: "+k+", v: "+v))
             .transform(new DistributedSchedulerTransformerSupplier(distributedSchedulerStore.name(), clock), distributedSchedulerStore.name())
             .filterNot((k,v) -> v==null)
             .peek((k,v) -> System.out.println("Output value k: "+k+", v: "+v))
             .to(OUTPUT_TOPIC);

        return builder.build();
    }


    private static final class DistributedSchedulerTransformerSupplier implements TransformerSupplier<String, String, KeyValue<String, String>> {

        private static final Duration SCHEDULER_PERIOD = Duration.ofSeconds(10);

        private final String stateStoreName;
        private final Clock clock;

        DistributedSchedulerTransformerSupplier(final String stateStoreName, final Clock clock) {
            this.stateStoreName = stateStoreName;
            this.clock = clock;
        }

        @Override
        public Transformer<String, String, KeyValue<String, String>> get() {
            return new Transformer<String, String, KeyValue<String, String>>() {

                private KeyValueStore<String, String> stateStore;
                private ProcessorContext context;
                private TaskId taskId;

                @SuppressWarnings("unchecked")
                @Override
                public void init(final ProcessorContext context) {
                    stateStore = (KeyValueStore<String, String>) context.getStateStore(stateStoreName);

                    this.context = context;
                    this.taskId = context.taskId();

                    this.context.schedule(SCHEDULER_PERIOD, PunctuationType.WALL_CLOCK_TIME, timestamp -> {

                        long processingStartTimeMs = System.currentTimeMillis();
                        System.out.println(". "+taskId);
                        int count = 0;
                        try (KeyValueIterator<String, String> iterator = stateStore.all()) {
                            while (iterator.hasNext()) {
                                KeyValue<String, String> keyValue = iterator.next();
                                Data data = serde.deserialize(keyValue.value).getData();
                                Data.MetaData metaData = data.getMetaData();

                                Instant now = Instant.now(clock);
                                if (metaData.getStartAt().isBefore(now) || metaData.getStartAt().equals(now)) {
                                    context.forward(keyValue.key, data.getSerializedJsonData());
                                    stateStore.delete(keyValue.key);
                                }
//                                System.out.print(keyValue+", ");
                                count++;
                            }
                        }
                        System.out.println("Count: "+count);
                        long processDurationMs = System.currentTimeMillis() - processingStartTimeMs;
                        if (processDurationMs > SCHEDULER_PERIOD.toMillis()) System.out.println("Warning: Scheduler processing duration took "+processDurationMs+" ms, when SCHEDULER_PERIOD="+SCHEDULER_PERIOD.toMillis()+" ms.");
                    });

                    this.stateStore.flush();
                }

                @Override
                public KeyValue<String, String> transform(final String key, final String value) {
                    stateStore.put(key, value);
                    return KeyValue.pair(null,null);
                }

                @Override
                public void close() {
                    stateStore.flush();
                    context.commit();
                    // Note: The store should NOT be closed manually here via `stateStore.close()`!
                    // The Kafka Streams API will automatically close stores when necessary.
                }
            };
        }

    }


}
