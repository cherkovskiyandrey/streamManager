package com.tango.stream.manager.model;

import lombok.Builder;
import lombok.Value;

import javax.annotation.Nullable;

@Value
@Builder
public class AvailableEdgeResponse {
    public static final AvailableEdgeResponse NONE = AvailableEdgeResponse.builder().build();
    @Nullable
    String externalIP;
    @Nullable
    String region;

    public static AvailableEdgeResponse none() {
        return NONE;
    }
}
