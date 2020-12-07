package com.tango.stream.manager.dao;

import com.tango.stream.manager.model.NodeType;

import javax.annotation.Nonnull;
import java.util.List;

public interface LiveNodeVersionDao {

    void addVersionAsync(@Nonnull NodeType nodeType, @Nonnull String version);

    void addVersion(@Nonnull NodeType nodeType, @Nonnull String version);

    @Nonnull
    List<String> getAllVersions(@Nonnull NodeType nodeType);
}
