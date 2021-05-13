package com.distributedscheduler;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.time.Instant;

@Value
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class DelayedCommand {
    String type;
    Instant publishAt;
    String message;
    String partitionKey;
}
