package com.distributeddelayedmessages;

import com.distributeddelayedmessages.event.CloudEventV1;
import com.distributeddelayedmessages.event.Data;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
class SerdeTest {

    private final Clock fixedClock = Clock.fixed(Instant.parse("2021-12-30T11:54:31.734551Z"), ZoneId.of("UTC"));

    private final Serde serde = new Serde(AppConfig.objectMapper.get());

    @Test
    void serializerTest() throws Exception {
        //given
        CloudEventV1 event = new EventBuilder(fixedClock).buildEvent("{\"test\":1}", new Data.MetaData(fixedClock.instant(), "topic1"));

        //when
        String jsonSerializedEvent = serde.serialize(event);

        //when
        assertThat(jsonSerializedEvent).isEqualTo("{\"specversion\":\"1.0\",\"id\":\"id1\",\"source\":\"/source\",\"type\":\"DistributedDelayedMessagesEvent\",\"datacontenttype\":\"application/json\",\"dataschema\":null,\"time\":\"2021-12-30T11:54:31.734551Z\",\"data\":{\"message\":\"{\\\"test\\\":1}\",\"metaData\":{\"startAt\":\"2021-12-30T11:54:31.734551Z\",\"outputTopic\":\"topic1\"}}}");
    }

    @Test
    void deserializerTest() throws Exception {
        //given
            String jsonSerializedEvent = "{\"specversion\":\"1.0\",\"id\":\"id1\",\"source\":\"/source\",\"type\":\"DistributedDelayedMessagesEvent\",\"datacontenttype\":\"application/json\",\"dataschema\":null,\"time\":\"2021-12-30T11:54:31.734551Z\",\"data\":{\"message\":\"{\\\"test1\\\":1}\",\"metaData\":{\"startAt\":\"2021-12-30T11:54:31.734551Z\",\"outputTopic\":\"topic1\"}}}";

        //when
        CloudEventV1 deserializedEvent = serde.deserialize(jsonSerializedEvent);

        //then
        Data.MetaData expectedMetaData = new Data.MetaData(Instant.parse("2021-12-30T11:54:31.734551Z"), "topic1");

        CloudEventV1 expectedCloudEvent = new CloudEventV1(
                "id1",
                URI.create("/source"),
                "DistributedDelayedMessagesEvent",
                "application/json",
                null,
                Instant.parse("2021-12-30T11:54:31.734551Z"),
                new Data("{\"test1\":1}", expectedMetaData)
        );

        assertThat(deserializedEvent)
                .isEqualTo(expectedCloudEvent);

    }
}