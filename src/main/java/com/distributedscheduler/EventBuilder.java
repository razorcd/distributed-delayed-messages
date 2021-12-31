package com.distributedscheduler;

import com.distributedscheduler.event.CloudEventV1;
import com.distributedscheduler.event.Data;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.time.Clock;

@RequiredArgsConstructor
public class EventBuilder {

    private final Clock clock;


    public CloudEventV1 buildEvent(String serializedStringData, Data.MetaData metaData) {
//        DistributedSchedulerMetaData metaData = new DistributedSchedulerMetaData(clock.instant(), 1, "topic1");
        Data data = new Data(serializedStringData, metaData);

        return new CloudEventV1(
                "id1",
                URI.create("/source"),
                "DistributedSchedulerEvent",
                "application/json",
                null,
                clock.instant(),
                data
        );
    }

}
