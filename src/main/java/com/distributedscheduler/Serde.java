package com.distributedscheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Serde {

    private final ObjectMapper objectMapper;

    public String serialize(EventBuilder.CloudEventV1 cloudEvent) {
        try {
            return objectMapper.writeValueAsString(cloudEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public EventBuilder.CloudEventV1 deserialize(String jsonCloudEvent) {
        try {
            return objectMapper.readValue(jsonCloudEvent, EventBuilder.CloudEventV1.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
