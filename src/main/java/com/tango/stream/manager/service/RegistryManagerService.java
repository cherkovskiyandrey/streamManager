package com.tango.stream.manager.service;

import com.tango.stream.manager.model.NodeType;

import javax.annotation.Nonnull;

public interface RegistryManagerService {

    @Nonnull
    NodeRegistryService getNodeRegistryService(@Nonnull NodeType nodeType);
}
