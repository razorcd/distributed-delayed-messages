package com.distributeddelayedmessages.topology;


import com.distributeddelayedmessages.Serde;
import com.distributeddelayedmessages.event.Data;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
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

public final class DelayedMessagesTransformerSupplier implements TransformerSupplier<String, String, KeyValue<String, Data>> {

    private static final Duration SCHEDULER_PERIOD = Duration.ofSeconds(10);

    @Getter
    private final Clock clock;
    private final Serde serde;

    @Getter
    private final String stateStoreName = "distributed-scheduler";

    public DelayedMessagesTransformerSupplier(final Clock clock, final Serde serde) {
        this.clock = clock;
        this.serde = serde;
    }

    public StoreBuilder<KeyValueStore<String, String>> getStateStoreBuilder() {
        return Stores
                .keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(stateStoreName),
                        Serdes.String(),
                        Serdes.String())
                .withCachingDisabled()
//            .withLoggingDisabled()
                ;
    }

    @Override
    public Transformer<String, String, KeyValue<String, Data>> get() {
        return new Transformer<String, String, KeyValue<String, Data>>() {

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
                                context.forward(keyValue.key, data);
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
            public KeyValue<String, Data> transform(final String key, final String value) {
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
