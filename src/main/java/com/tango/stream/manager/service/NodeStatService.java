package com.tango.stream.manager.service;

import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.NodeType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public interface NodeStatService {
    @Nonnull
    Map<String, Map<String, NodeData>> getNodeData(@Nonnull NodeType nodeType);

    @Nonnull
    Map<String, NodeData> getNodeData(@Nonnull NodeType nodeType, @Nonnull String region);

    @Nullable
    NodeData getNodeData(@Nonnull String uid);
}
