package com.distributedscheduler;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class App {

    static final String SUM_OF_ODD_NUMBERS_TOPIC = "sum-of-odd-numbers-topic";
    static final String NUMBERS_TOPIC = "numbers-topic";

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "sum-lambda-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Topology topology = getTopology();
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        // We assume the input topic contains records where the values are Integers.
        // We don't really care about the keys of the input records;  for simplicity, we assume them
        // to be Integers, too, because we will re-key the stream later on, and the new key will be
        // of type Integer.
        final KStream<Void, Integer> input = builder.stream(NUMBERS_TOPIC);

        final KTable<Integer, Integer> sumOfOddNumbers = input
                .peek((k,v) -> System.out.println("Input value: "+v))
                // We are only interested in odd numbers.
                .filter((k, v) -> v % 2 != 0)
                // We want to compute the total sum across ALL numbers, so we must re-key all records to the
                // same key.  This re-keying is required because in Kafka Streams a data record is always a
                // key-value pair, and KStream aggregations such as `reduce` operate on a per-key basis.
                // The actual new key (here: `1`) we pick here doesn't matter as long it is the same across
                // all records.
                .selectKey((k, v) -> 1)
                // no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .groupByKey()
                // Add the numbers to compute the sum.
                .reduce((v1, v2) -> v1 + v2);

        sumOfOddNumbers.toStream()
                .peek((k,v) -> System.out.println("Output value: "+v))
                .to(SUM_OF_ODD_NUMBERS_TOPIC);

        return builder.build();
    }

}
