package com.distributedscheduler;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppTest {

    @Test
    public void givenEmptyStore_whenReceivingDelayedCommand_shouldEmitMessageByPublishTime() throws InterruptedException {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "distributed-scheduler-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/distributed-scheduler-test");

        Clock clock = Clock.systemUTC();

        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(App.getTopology(clock), streamsConfiguration)) {
            final TestInputTopic<String, String> input = topologyTestDriver
                    .createInputTopic(App.INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.EPOCH, Duration.ofMillis(1));

            final TestOutputTopic<String, String> output = topologyTestDriver
                    .createOutputTopic(App.OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());


            String event = "{" +
                        "\"type\": \"Delayed\"," +
                        "\"publishAt\": \""+Instant.now(clock).plusSeconds(2)+"\"," +
                        "\"message\": \"message1\"," +
                        "\"partitionKey\": \"42\"" +
                    "}";

            input.pipeInput("111", event);
            topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(10));

            assertTrue(output.isEmpty());

            Thread.sleep(2001);
            topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(10));

            assertEquals(KeyValue.pair("42", "message1"), output.readKeyValue());
            assertTrue(output.isEmpty());
        }
    }

}