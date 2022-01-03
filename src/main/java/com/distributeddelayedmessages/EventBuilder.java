package com.distributeddelayedmessages;

import com.distributeddelayedmessages.event.CloudEventV1;
import com.distributeddelayedmessages.event.Data;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.time.Clock;

@RequiredArgsConstructor
public class EventBuilder {

    private final Clock clock;


    public CloudEventV1 buildEvent(String serializedStringData, Data.MetaData metaData) {
        Data data = new Data(serializedStringData, metaData);

        return new CloudEventV1(
                "id1",
                URI.create("/source"),
                "DistributedDelayedMessagesEvent",
                "application/json",
                null,
                clock.instant(),
                data
        );
    }

}
