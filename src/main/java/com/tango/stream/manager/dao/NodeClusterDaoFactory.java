package com.tango.stream.manager.dao;

import com.tango.stream.manager.model.NodeType;

import javax.annotation.Nonnull;

public interface NodeClusterDaoFactory {
    @Nonnull
    NodeClusterDao create(@Nonnull String name, @Nonnull NodeType nodeType);
}
