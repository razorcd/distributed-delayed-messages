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


    static final String INPUT_TOPIC = "distributed-scheduler-input";
    static final List<String> OUTPUT_TOPICS = Arrays.asList("distributed-scheduler-output1", "distributed-scheduler-output2");



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


        DistributedSchedulerTransformerSupplier distributedSchedulerTransformerSupplier = new DistributedSchedulerTransformerSupplier(CLOCK, new Serde(MAPPER));

        final TopologyFactory topologyFactory = new TopologyFactory(distributedSchedulerTransformerSupplier);
        final Topology topology = topologyFactory.build(INPUT_TOPIC, OUTPUT_TOPICS);
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


}
