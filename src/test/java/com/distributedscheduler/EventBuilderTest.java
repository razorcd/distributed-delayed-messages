//package com.distributedscheduler;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.cloudevents.CloudEvent;
//import io.cloudevents.core.CloudEventUtils;
//import io.cloudevents.core.builder.CloudEventBuilder;
//import io.cloudevents.core.data.PojoCloudEventData;
//import io.cloudevents.core.format.EventFormat;
//import io.cloudevents.core.provider.EventFormatProvider;
//import io.cloudevents.core.v1.CloudEventV1;
//import io.cloudevents.jackson.JsonFormat;
//import io.cloudevents.jackson.PojoCloudEventDataMapper;
//import org.assertj.core.api.Assert;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Test;
//
//import java.net.URI;
//import java.nio.charset.StandardCharsets;
//import java.time.Clock;
//import java.time.Instant;
//import java.time.ZoneId;
//import java.time.ZoneOffset;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.junit.jupiter.api.Assertions.*;
//class EventBuilderTest {
//
//    private ObjectMapper objectMapper = AppConfig.objectMapper.get();
//    private Clock fixedClock = Clock.fixed(Instant.parse("2021-12-30T11:54:31.734551Z"), ZoneId.of("UTC"));
//    private static final EventFormat eventFormat = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
//
//    @Test
//    void serializerTest() throws Exception {
//        //given
//        CloudEvent event = new EventBuilder(objectMapper, fixedClock).buildEvent("{\"test\":1}");
//
//        //when
//        byte[] serializedEvent = eventFormat.serialize(event);
//        String jsonSerializedEvent = new String(serializedEvent, StandardCharsets.UTF_8);
//
//        //when
//        assertThat(jsonSerializedEvent).isEqualTo("{\"specversion\":\"1.0\",\"id\":\"id1\",\"source\":\"/source\",\"type\":\"DistributedSchedulerEvent\",\"datacontenttype\":\"application/json\",\"time\":\"2021-12-30T11:54:31.734551Z\",\"data\":{\"rawEvent\":\"{\\\"test\\\":1}\",\"metaData\":{\"startAt\":\"2021-12-30T11:54:31.734551Z\",\"times\":1,\"topic\":\"topic1\"}}}");
//    }
//
//    @Test
//    void deserializerTest() throws Exception {
//        //given
//        String jsonSerializedEvent = "{\"specversion\":\"1.0\",\"id\":\"id1\",\"source\":\"/source\",\"type\":\"DistributedSchedulerEvent\",\"datacontenttype\":\"application/json\",\"time\":\"2021-12-30T11:54:31.734551Z\",\"data\":{\"rawEvent\":\"{\\\"test1\\\":1}\",\"metaData\":{\"startAt\":\"2021-12-30T11:54:31.734551Z\",\"times\":1,\"topic\":\"topic1\"}}}";
//
//        //when
//        CloudEvent deserializedEvent = eventFormat.deserialize(jsonSerializedEvent.getBytes());
//
//        //then
//        EventBuilder.DistributedSchedulerMetaData expectedMetaData = new EventBuilder.DistributedSchedulerMetaData(Instant.parse("2021-12-30T11:54:31.734551Z"), 1, "topic1");
//        EventBuilder.DistributedSchedulerData expectedData = new EventBuilder.DistributedSchedulerData("{\"test1\":1}", expectedMetaData);
//
//        CloudEvent expectedCloudEvent = CloudEventBuilder.v1()
//                .withId("id1")
//                .withSource(URI.create("/source"))
//                .withDataContentType("application/json")
//                .withType("DistributedSchedulerEvent")
//                .withTime(Instant.parse("2021-12-30T11:54:31.734551Z").atOffset(ZoneOffset.UTC))
//                .withData(PojoCloudEventData.wrap(expectedData, objectMapper::writeValueAsBytes))
//                .build();
//
//        assertThat(deserializedEvent).usingRecursiveComparison().ignoringFields("data").isEqualTo(expectedCloudEvent);
//
//        //and
//        PojoCloudEventData<EventBuilder.DistributedSchedulerData> deserializedCloudEventDataObject = CloudEventUtils.mapData(deserializedEvent, PojoCloudEventDataMapper.from(objectMapper,EventBuilder.DistributedSchedulerData.class));
//        assertEquals(deserializedCloudEventDataObject.getValue(), expectedData);
//    }
//}