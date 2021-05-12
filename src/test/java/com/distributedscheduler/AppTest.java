package com.distributedscheduler;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
class AppTest {

    @Test
    public void test1() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "distributed-scheduler-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/distributed-scheduler-test");


        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(App.getTopology(), streamsConfiguration)) {
            final TestInputTopic<String, String> input = topologyTestDriver
                    .createInputTopic(App.INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            final TestOutputTopic<String, String> output = topologyTestDriver
                    .createOutputTopic(App.OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());

            input.pipeInput("111", "{}");

            assertEquals("_", output.readValue());
        }
    }

}