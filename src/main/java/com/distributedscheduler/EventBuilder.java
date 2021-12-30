package com.distributedscheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.core.v1.CloudEventV1;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;

public class EventBuilder {

    private static ObjectMapper objectMapper = AppConfig.objectMapper;

    public static CloudEvent build(String rawInputEvent) {
        DistributedSchedulerData data = new DistributedSchedulerData(rawInputEvent, Instant.now(), 1, "topic1");

        return CloudEventBuilder.v1()
                .withId("id1")
                .withSource(URI.create("/source"))
                .withDataContentType("application/json")
                .withType("DistributedSchedulerEvent")
                .withTime(Instant.now().atOffset(ZoneOffset.UTC))
                .withData(PojoCloudEventData.wrap(data, objectMapper::writeValueAsBytes))
                .build();
    }

    @Value
    @NoArgsConstructor(force = true)
    @RequiredArgsConstructor
    public static class DistributedSchedulerData {
        String event;
        Instant startAt;
        Integer times;
        String topic;
    }
}
