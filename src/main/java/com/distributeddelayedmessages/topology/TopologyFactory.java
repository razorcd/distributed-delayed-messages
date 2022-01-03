package com.distributeddelayedmessages.topology;


import com.distributeddelayedmessages.event.Data;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.List;

public class TopologyFactory {

    private final DelayedMessagesTransformerSupplier delayedMessagesTransformerSupplier;


    public TopologyFactory(DelayedMessagesTransformerSupplier delayedMessagesTransformerSupplier) {
        this.delayedMessagesTransformerSupplier = delayedMessagesTransformerSupplier;
    }

    public Topology build(String inputTopic,
                          List<String> outputTopics) {
        final StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, String>> stateStoreBuilder = delayedMessagesTransformerSupplier.getStateStoreBuilder();
        builder.addStateStore(stateStoreBuilder);

        final KStream<String, String> input = builder.stream(inputTopic);

        KStream<String, Data> output = input.peek((k, v) -> System.out.println("Input value k: " + k + ", v: " + v))
                .transform(delayedMessagesTransformerSupplier, delayedMessagesTransformerSupplier.getStateStoreName())
                .filterNot((k, v) -> v == null)
                .peek((k, v) -> System.out.println("Output value k: " + k + ", v: " + v));

        outputTopics.forEach(topic ->
                output.filter((k,v) ->
                        topic.equals(v.getMetaData().getOutputTopic()))
                        .mapValues((k,v) -> v.getMessage())
                        .to(topic)
        );

        return builder.build();
    }




}
