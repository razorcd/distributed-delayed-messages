package com.distributedscheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.jackson.PojoCloudEventDataMapper;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Serde {

    private final ObjectMapper objectMapper = AppConfig.objectMapper.get();
//    private final EventFormat eventFormat = new EventFormat()

    public String serialize(EventBuilder.CloudEventV1 cloudEvent) {
//        byte[] byteSerializedEvent = jsonFormat.serialize(cloudEvent);
//        return new String(byteSerializedEvent, StandardCharsets.UTF_8);
        try {
            return objectMapper.writeValueAsString(cloudEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public EventBuilder.CloudEventV1 deserialize(String jsonCloudEvent) {
//        return jsonFormat.deserialize(jsonCloudEvent.getBytes());
//        return new JsonFormat().deserialize(jsonCloudEvent.getBytes(), PojoCloudEventDataMapper.from(objectMapper, EventBuilder.DistributedSchedulerData.class));
        try {
            return objectMapper.readValue(jsonCloudEvent, EventBuilder.CloudEventV1.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
