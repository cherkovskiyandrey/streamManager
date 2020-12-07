package com.tango.stream.manager.service;

import com.tango.stream.manager.model.NodeType;

import javax.annotation.Nonnull;

//TODO: use Nicolay service impl here
public interface VersionService {

    @Nonnull
    String getActualVersion(@Nonnull NodeType nodeType, @Nonnull String region);
}
