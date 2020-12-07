package com.tango.stream.manager.model;

import com.google.common.collect.Maps;
import lombok.*;

import java.util.Map;

@Builder
@Data
@AllArgsConstructor
public class KeepAliveRequest {
    @NonNull String uid;
    @NonNull String extIp;
    @NonNull NodeType nodeType;
    String region;
    @Builder.Default
    @ToString.Exclude
    Map<String, StreamData> activeStreams = Maps.newHashMap();
    @NonNull String version;
    @NonNull Double avgCpuUsage;
    @NonNull Double medCpuUsage;
}
