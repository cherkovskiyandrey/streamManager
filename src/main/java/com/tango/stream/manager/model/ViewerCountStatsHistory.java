package com.tango.stream.manager.model;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@Builder
public class ViewerCountStatsHistory {
    long accountId;
    String region;
    int percentile;
    int value;
    Instant timestamp;
}
