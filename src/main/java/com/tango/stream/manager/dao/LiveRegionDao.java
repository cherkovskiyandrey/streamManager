package com.tango.stream.manager.dao;

import com.tango.stream.manager.model.NodeType;

import javax.annotation.Nonnull;
import java.util.List;

public interface LiveRegionDao {

    void addRegionAsync(@Nonnull NodeType nodeType, @Nonnull String region);

    void addRegion(@Nonnull NodeType nodeType, @Nonnull String region);

    @Nonnull
    List<String> getAllRegions(@Nonnull NodeType nodeType);
}
