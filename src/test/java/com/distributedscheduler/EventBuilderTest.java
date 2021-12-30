package com.distributedscheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
class EventBuilderTest {

    private ObjectMapper objectMapper = AppConfig.objectMapper;

    @Test
    void test() throws Exception {

        CloudEvent event = EventBuilder.build("{\"test\":1}");

        String jsonEvent = objectMapper.writeValueAsString(event);

        assertEquals("{}", jsonEvent);

    }

}