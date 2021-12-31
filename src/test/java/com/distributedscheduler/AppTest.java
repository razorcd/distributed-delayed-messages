package com.distributedscheduler;

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
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

class AppTest {
    Instant now = Instant.parse("2021-05-15T21:02:11.333824Z");
    Clock clock = Clock.fixed(now, ZoneId.of("UTC"));

    TestInputTopic<String, String> input;
    TestOutputTopic<String, String> output;
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

        this.topologyTestDriver = new TopologyTestDriver(App.getTopology(clock), streamsConfiguration);
        input = topologyTestDriver.createInputTopic(App.INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.EPOCH, Duration.ofMillis(1));
        output = topologyTestDriver.createOutputTopic(App.OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());

        store = topologyTestDriver.getKeyValueStore("distributed-scheduler-store");
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @AfterAll
    static void afterAll() {
    }

    @Test
    void givenEmptyStore_whenReceivingDelayedCommand_shouldEmitMessageByPublishTime() throws InterruptedException {
        Instant nowPlus20SecInstant = now.plusSeconds(20);
        Instant nowPlus30SecInstant = now.plusSeconds(30);
        long nowPlus20Sec = nowPlus20SecInstant.getEpochSecond();

        //when
        String event = createDelayedEvent(nowPlus20SecInstant, "message1", 1, App.OUTPUT_TOPIC);
        input.pipeInput("111", event);

        //then don't publish yet
        MockedStatic instantMock = mockStatic(Instant.class);
        instantMock.when(() -> Instant.from(argThat(temporal -> temporal.getLong(ChronoField.INSTANT_SECONDS)==nowPlus20Sec))).thenReturn(nowPlus20SecInstant); //for json deserializer

        instantMock.when(() -> Instant.now(clock)).thenReturn(now);
        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(10));
        assertTrue(output.isEmpty());
        assertNotNull(store.get("111"));

        //and when
        instantMock.when(() -> Instant.now(clock)).thenReturn(nowPlus20SecInstant);
        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(10));

        //then publish it
        assertEquals(KeyValue.pair("111", "message1"), output.readKeyValue());
        assertNull(store.get("111"));

        //and when
        instantMock.when(() -> Instant.now(clock)).thenReturn(nowPlus30SecInstant);
        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(10));

        //then publish nothing else
        assertTrue(output.isEmpty());
        assertNull(store.get("111"));

        //finally
        instantMock.verify(times(2), () -> Instant.now(clock));
    }

    private String createDelayedEvent(Instant startAt, String message, int times, String outputTopic) {
        return "{\"specversion\":\"1.0\",\"id\":\"id1\",\"source\":\"/source\",\"type\":\"DistributedSchedulerEvent\",\"datacontenttype\":\"application/json\",\"dataschema\":null,\"time\":\"2021-12-30T11:54:31.734551Z\"," +
                "\"data\":{\"serializedJsonData\":\""+message+"\"," +
                    "\"metaData\":{\"startAt\":\""+startAt+"\",\"times\":"+times+",\"outputTopic\":\""+outputTopic+"\"}}}";
    }
}