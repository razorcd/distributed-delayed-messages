package com.distributedscheduler;

import com.distributedscheduler.topology.DistributedSchedulerTransformerSupplier;
import com.distributedscheduler.topology.TopologyFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class App {
    static final Clock CLOCK = AppConfig.clock;
    static final ObjectMapper MAPPER = AppConfig.objectMapper.get();

    public static void main(final String[] args) {
        final String inputTopicName = "distributed-scheduler-input";
        final List<String> outputTopicNames = Arrays.asList("distributed-scheduler-output1", "distributed-scheduler-output2");
        final Properties streamsConfiguration = getStreamsConfiguration();


        DistributedSchedulerTransformerSupplier distributedSchedulerTransformerSupplier = new DistributedSchedulerTransformerSupplier(CLOCK, new Serde(MAPPER));

        final TopologyFactory topologyFactory = new TopologyFactory(distributedSchedulerTransformerSupplier);
        final Topology topology = topologyFactory.build(inputTopicName, outputTopicNames);
        try (KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration)) {
            streams.cleanUp();
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }
    }


    private static Properties getStreamsConfiguration() {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "distributed-scheduler-appid");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "distributed-scheduler-clientid");

        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, "GROUPSID0");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams2");

        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        return streamsConfiguration;
    }
}
