package com.distributedscheduler.event;


import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.time.Instant;

@Value
@NoArgsConstructor(force = true)
@RequiredArgsConstructor
public class Data {
    String message;
    MetaData metaData;


    @Value
    @NoArgsConstructor(force = true)
    @RequiredArgsConstructor
    public static class MetaData {
        Instant startAt;
//        Integer times;
        String outputTopic;
    }
}
