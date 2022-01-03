package com.distributeddelayedmessages;

import com.distributeddelayedmessages.event.CloudEventV1;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Serde {

    private final ObjectMapper objectMapper;

    public String serialize(CloudEventV1 cloudEvent) {
        try {
            return objectMapper.writeValueAsString(cloudEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public CloudEventV1 deserialize(String jsonCloudEvent) {
        try {
            return objectMapper.readValue(jsonCloudEvent, CloudEventV1.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
