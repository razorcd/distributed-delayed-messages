package com.distributedscheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

@RequiredArgsConstructor
public class EventBuilder {

    private final ObjectMapper objectMapper;
    private final Clock clock;


    public CloudEvent buildEvent(String rawInputEvent) {
        DistributedSchedulerMetaData metaData = new DistributedSchedulerMetaData(clock.instant(), 1, "topic1");
        DistributedSchedulerData data = new DistributedSchedulerData(rawInputEvent, metaData);

        return CloudEventBuilder.v1()
                .withId("id1")
                .withSource(URI.create("/source"))
                .withDataContentType("application/json")
                .withType("DistributedSchedulerEvent")
                .withTime(clock.instant().atOffset(ZoneOffset.UTC))
                .withData(PojoCloudEventData.wrap(data, objectMapper::writeValueAsBytes))
                .build();
    }

    @Value
    @NoArgsConstructor(force = true)
    @RequiredArgsConstructor
    public static class DistributedSchedulerData {
        String rawEvent;
        DistributedSchedulerMetaData metaData;
    }

    @Value
    @NoArgsConstructor(force = true)
    @RequiredArgsConstructor
    public static class DistributedSchedulerMetaData {
        Instant startAt;
        Integer times;
        String topic;
    }
}
