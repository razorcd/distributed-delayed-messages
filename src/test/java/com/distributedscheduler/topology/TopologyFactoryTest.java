package com.distributedscheduler.topology;

import com.distributedscheduler.AppConfig;
import com.distributedscheduler.Serde;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

class TopologyFactoryTest {
    Instant now = Instant.parse("2021-05-15T21:02:11.333824Z");
    Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
    ObjectMapper mapper = AppConfig.objectMapper.get();

    TestInputTopic<String, String> input;
    String inputTopicName = "topicInput1";

    List<TestOutputTopic<String, String>> outputTopics;
    List<String> outputTopicNames = Arrays.asList("outputTopic1", "outputTopic2");

    TopologyTestDriver topologyTestDriver;
    KeyValueStore<String, String> store;


    @BeforeAll
    static void beforeAll() throws Exception {
    }

    @BeforeEach
    void setUp() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "distributed-scheduler-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/distributed-scheduler-test");

        DistributedSchedulerTransformerSupplier distributedSchedulerTransformerSupplier = new DistributedSchedulerTransformerSupplier(clock, new Serde(mapper));

        final TopologyFactory topologyFactory = new TopologyFactory(distributedSchedulerTransformerSupplier);
        final Topology topology = topologyFactory.build(inputTopicName, outputTopicNames);
        this.topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration);

        input = topologyTestDriver.createInputTopic(inputTopicName, new StringSerializer(), new StringSerializer(), Instant.EPOCH, Duration.ofMillis(1));
        outputTopics = outputTopicNames.stream().map(topic -> topologyTestDriver.createOutputTopic(topic, new StringDeserializer(), new StringDeserializer())).collect(Collectors.toList());


        store = topologyTestDriver.getKeyValueStore(distributedSchedulerTransformerSupplier.getStateStoreName());
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @AfterAll
    static void afterAll() {
    }

    @Test
    void givenEmptyStore_whenReceivingEventsWithPublishOnceAtStartTime_shouldEmitMessageOnceAtStartTime() throws InterruptedException {
        Instant nowPlus20SecInstant = now.plusSeconds(20);
        Instant nowPlus30SecInstant = now.plusSeconds(30);
        Instant nowPlus31SecInstant = now.plusSeconds(31);
        long nowPlus20Sec = nowPlus20SecInstant.getEpochSecond();
        long nowPlus30Sec = nowPlus30SecInstant.getEpochSecond();

        //when
        String event0 = createDelayedEvent(nowPlus20SecInstant, "message0", outputTopicNames.get(0));
        String event0a = createDelayedEvent(nowPlus20SecInstant, "message0again", outputTopicNames.get(0));
        String event1 = createDelayedEvent(nowPlus30SecInstant, "message1", outputTopicNames.get(1));
        input.pipeInput("000", event0);
        input.pipeInput("000", event0a);
        input.pipeInput("111", event1);

        //then don't publish yet
        MockedStatic instantMock = mockStatic(Instant.class);
        instantMock.when(() -> Instant.from(argThat(temporal -> temporal.getLong(ChronoField.INSTANT_SECONDS)==nowPlus20Sec))).thenReturn(nowPlus20SecInstant); //for json deserializer
        instantMock.when(() -> Instant.from(argThat(temporal -> temporal.getLong(ChronoField.INSTANT_SECONDS)==nowPlus30Sec))).thenReturn(nowPlus30SecInstant); //for json deserializer

        instantMock.when(() -> Instant.now(clock)).thenReturn(now);
        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(10));
        assertTrue(outputTopics.stream().allMatch(TestOutputTopic::isEmpty));
        assertNotNull(store.get("000"));
        assertNotNull(store.get("111"));

        //and when
        instantMock.when(() -> Instant.now(clock)).thenReturn(nowPlus20SecInstant);
        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(10));

        //then publish it
        assertEquals(KeyValue.pair("000", "message0again"), outputTopics.get(0).readKeyValue());
        assertTrue(outputTopics.get(1).isEmpty());
        assertNull(store.get("000"));
        assertNotNull(store.get("111"));

        //and when
        instantMock.when(() -> Instant.now(clock)).thenReturn(nowPlus30SecInstant);
        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(10));

        //then publish it
        assertTrue(outputTopics.get(0).isEmpty());
        assertEquals(KeyValue.pair("111", "message1"), outputTopics.get(1).readKeyValue());
        assertNull(store.get("111"));

        //and when
        instantMock.when(() -> Instant.now(clock)).thenReturn(nowPlus31SecInstant);
        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1));

        //then publish nothing else
        assertTrue(outputTopics.stream().allMatch(TestOutputTopic::isEmpty));
        assertFalse(store.all().hasNext());

        //finally
        instantMock.verify(times(5), () -> Instant.now(clock));
    }

    private String createDelayedEvent(Instant startAt, String message, String outputTopic) {
        return "{\"specversion\":\"1.0\",\"id\":\"id1\",\"source\":\"/source\",\"type\":\"DistributedSchedulerEvent\",\"datacontenttype\":\"application/json\",\"dataschema\":null,\"time\":\"2021-12-30T11:54:31.734551Z\"," +
                "\"data\":{\"message\":\""+message+"\"," +
                "\"metaData\":{\"startAt\":\""+startAt+"\",\"outputTopic\":\""+outputTopic+"\"}}}";
    }
}