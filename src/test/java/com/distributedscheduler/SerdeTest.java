package com.distributedscheduler;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
class SerdeTest {

    private final Clock fixedClock = Clock.fixed(Instant.parse("2021-12-30T11:54:31.734551Z"), ZoneId.of("UTC"));


    @Test
    void serializerTest() throws Exception {
        //given
        EventBuilder.CloudEventV1 event = new EventBuilder(fixedClock).buildEvent("{\"test\":1}");

        //when
        String jsonSerializedEvent = new Serde().serialize(event);

        //when
        assertThat(jsonSerializedEvent).isEqualTo("{\"specversion\":\"1.0\",\"id\":\"id1\",\"source\":\"/source\",\"type\":\"DistributedSchedulerEvent\",\"datacontenttype\":\"application/json\",\"dataschema\":null,\"time\":\"2021-12-30T11:54:31.734551Z\",\"data\":{\"serializedJsonData\":\"{\\\"test\\\":1}\",\"metaData\":{\"startAt\":\"2021-12-30T11:54:31.734551Z\",\"times\":1,\"topic\":\"topic1\"}}}");
    }

    @Test
    void deserializerTest() throws Exception {
        //given
        String jsonSerializedEvent = "{\"specversion\":\"1.0\",\"id\":\"id1\",\"source\":\"/source\",\"type\":\"DistributedSchedulerEvent\",\"datacontenttype\":\"application/json\",\"dataschema\":null,\"time\":\"2021-12-30T11:54:31.734551Z\",\"data\":{\"serializedJsonData\":\"{\\\"test1\\\":1}\",\"metaData\":{\"startAt\":\"2021-12-30T11:54:31.734551Z\",\"times\":1,\"topic\":\"topic1\"}}}";

        //when
        EventBuilder.CloudEventV1 deserializedEvent = new Serde().deserialize(jsonSerializedEvent);

        //then
        EventBuilder.DistributedSchedulerMetaData expectedMetaData = new EventBuilder.DistributedSchedulerMetaData(Instant.parse("2021-12-30T11:54:31.734551Z"), 1, "topic1");

        EventBuilder.CloudEventV1 expectedCloudEvent = new EventBuilder.CloudEventV1(
                "id1",
                URI.create("/source"),
                "DistributedSchedulerEvent",
                "application/json",
                null,
                Instant.parse("2021-12-30T11:54:31.734551Z"),
                new EventBuilder.DistributedSchedulerData("{\"test1\":1}", expectedMetaData)
        );

        assertThat(deserializedEvent)
                .isEqualTo(expectedCloudEvent);

    }
}