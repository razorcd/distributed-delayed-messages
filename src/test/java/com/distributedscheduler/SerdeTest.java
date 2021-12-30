package com.distributedscheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.CloudEventUtils;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.jackson.PojoCloudEventDataMapper;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
class SerdeTest {

    private ObjectMapper objectMapper = AppConfig.objectMapper.get();
    private Clock fixedClock = Clock.fixed(Instant.parse("2021-12-30T11:54:31.734551Z"), ZoneId.of("UTC"));


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
        String jsonSerializedEvent = "{\"specversion\":\"1.0\",\"id\":\"id1\",\"source\":\"/source\",\"type\":\"DistributedSchedulerEvent\",\"datacontenttype\":\"application/json\",\"dataschema\":null,\"time\":\"2021-12-30T11:54:31.734551Z\",\"data\":{\"serializedJsonData\":\"{\\\"test\\\":1}\",\"metaData\":{\"startAt\":\"2021-12-30T11:54:31.734551Z\",\"times\":1,\"topic\":\"topic1\"}}}";

        //when
        EventBuilder.CloudEventV1 deserializedEvent = new Serde().deserialize(jsonSerializedEvent);

        //then
        EventBuilder.DistributedSchedulerMetaData expectedMetaData = new EventBuilder.DistributedSchedulerMetaData(Instant.parse("2021-12-30T11:54:31.734551Z"), 1, "topic1");
        EventBuilder.DistributedSchedulerData expectedData = new EventBuilder.DistributedSchedulerData("{\"test1\":1}", expectedMetaData);

        EventBuilder.CloudEventV1 expectedCloudEvent = new EventBuilder.CloudEventV1(
                "id1",
                URI.create("/source"),
                "DistributedSchedulerEvent",
                "application/json",
                null,
                Instant.parse("2021-12-30T11:54:31.734551Z"),
                expectedData
        );

//        CloudEvent expectedCloudEvent = CloudEventBuilder.v1()
//                .withId("id1")
//                .withSource(URI.create("/source"))
//                .withDataContentType("application/json")
//                .withType("DistributedSchedulerEvent")
//                .withTime(Instant.parse("2021-12-30T11:54:31.734551Z").atOffset(ZoneOffset.UTC))
//                .withData(PojoCloudEventData.wrap(expectedData, objectMapper::writeValueAsBytes))
//                .build();

        assertThat(deserializedEvent)
                .usingRecursiveComparison()
                .ignoringFields("data")
                .isEqualTo(expectedCloudEvent);

//        //and data
//        PojoCloudEventData<EventBuilder.DistributedSchedulerData> deserializedCloudEventDataObject = CloudEventUtils.mapData(deserializedEvent, PojoCloudEventDataMapper.from(objectMapper,EventBuilder.DistributedSchedulerData.class));
//        assertEquals(deserializedCloudEventDataObject.getValue(), expectedData);
    }



}