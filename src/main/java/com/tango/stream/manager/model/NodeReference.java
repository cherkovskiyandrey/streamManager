package com.tango.stream.manager.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class NodeReference {
    @NonNull
    String nodeUid;
    @NonNull
    String extIp;
    @NonNull
    String region;
}
