package com.tango.stream.manager.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nonnull;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NodeDescription {
    public static final NodeDescription NONE_NODE = NodeDescription.builder().nodeUid("").extIp("").version("").region("").nodeType(NodeType.UNKNOWN).build();
    @Nonnull
    private String nodeUid;
    @Nonnull
    private String extIp;
    @Nonnull
    private String version;
    @Nonnull
    private String region;
    @Nonnull
    private NodeType nodeType;

    public boolean isNone() {
        return NONE_NODE.equals(this);
    }
}
