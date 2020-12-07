package com.tango.stream.manager.dao;

import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.NodeDescription;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface NodeDao {
    @Nullable
    NodeData getNodeById(@Nonnull String uid);

    boolean isExist(@Nonnull String uid);

    @Nullable
    NodeDescription getNodeByExtIp(@Nonnull String extIp);

    void save(@Nonnull NodeData newNodeData);

    void removeAsync(@Nonnull NodeData node);

    void remove(@Nonnull NodeData node);
}
