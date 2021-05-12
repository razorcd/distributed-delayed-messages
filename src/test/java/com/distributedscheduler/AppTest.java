package com.distributedscheduler;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
class AppTest {

    @Test
    public void shouldSumEvenNumbers() {
        final List<Integer> inputValues = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        final List<KeyValue<Integer, Integer>> expectedValues = Arrays.asList(
                new KeyValue<>(1, 1),
                new KeyValue<>(1, 4),
                new KeyValue<>(1, 9),
                new KeyValue<>(1, 16),
                new KeyValue<>(1, 25)
        );

        //
        // Step 1: Configure and start the processor topology.
        //
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-lambda-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        // Use a temporary directory for storing state, which will be automatically removed after the test.
//        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp");


        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(App.getTopology(), streamsConfiguration)) {
            //
            // Step 2: Setup input and output topics.
            //
            final TestInputTopic<Integer, Integer> input = topologyTestDriver
                    .createInputTopic(App.NUMBERS_TOPIC,
                            new IntegerSerializer(),
                            new IntegerSerializer());
            final TestOutputTopic<Integer, Integer> output = topologyTestDriver
                    .createOutputTopic(App.SUM_OF_ODD_NUMBERS_TOPIC,
                            new IntegerDeserializer(),
                            new IntegerDeserializer());

            //
            // Step 3: Produce some input data to the input topic.
            //
            input.pipeValueList(inputValues);

            //
            // Step 4: Verify the application's output data.
            //
            assertEquals(expectedValues, output.readKeyValuesToList());
        }
    }

}