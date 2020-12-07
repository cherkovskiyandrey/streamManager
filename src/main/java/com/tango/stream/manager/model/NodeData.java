package com.tango.stream.manager.model;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NodeData {
    @NonNull String uid;
    @NonNull NodeType nodeType;
    @NonNull String extIp;
    @NonNull String region;
    @NonNull String version;
    long lastUpdate;
    double avgCpuUsage;
    double medCpuUsage;

    public NodeDescription toNodeDescription() {
        return NodeDescription.builder()
                .nodeType(nodeType)
                .version(version)
                .region(region)
                .nodeUid(uid)
                .extIp(extIp)
                .build();
    }
}
