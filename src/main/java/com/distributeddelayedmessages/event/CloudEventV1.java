package com.distributeddelayedmessages.event;

import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.net.URI;
import java.time.Instant;

@Value
@RequiredArgsConstructor
@NoArgsConstructor(force = true)
public class CloudEventV1 {
    String specversion = "1.0";
    String id;
    URI source;
    String type;
    String datacontenttype;
    URI dataschema;
    Instant time;
    Data data;
}