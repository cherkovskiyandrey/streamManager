package com.tango.stream.manager.model;

import lombok.Builder;
import lombok.Value;

import javax.annotation.Nullable;

@Value
@Builder
public class AvailableGatewayResponse {
    @Nullable
    String externalIP;
    @Nullable
    String region;
}
