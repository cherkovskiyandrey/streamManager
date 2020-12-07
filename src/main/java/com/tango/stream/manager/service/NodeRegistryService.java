package com.tango.stream.manager.service;

import com.tango.stream.manager.model.KeepAliveRequest;
import com.tango.stream.manager.model.NodeType;

import javax.annotation.Nonnull;

public interface NodeRegistryService {

    @Nonnull
    NodeType nodeType();

    void onKeepAlive(@Nonnull KeepAliveRequest request) throws Exception;

    void unregister(@Nonnull String uid);
}
