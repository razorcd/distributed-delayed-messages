package com.distributedscheduler;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.jackson.JsonCloudEventData;

public interface CloudEventMixin<T> {

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
            property = "type",
            defaultImpl = Void.class)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = JsonCloudEventData.class, name = "DistributedSchedulerEvent")
    })
    T getData();

    @JsonTypeId
    String getType();
}