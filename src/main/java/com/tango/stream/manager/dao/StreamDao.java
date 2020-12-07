package com.tango.stream.manager.dao;

import com.tango.stream.manager.model.NodeDescription;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface StreamDao<StreamDescription> {

    void addStream(@Nonnull NodeDescription nodeDescription, @Nonnull StreamDescription stream);

    void addStreams(@Nonnull NodeDescription nodeDescription, @Nonnull Collection<StreamDescription> stream);

    @Nonnull
    Optional<StreamDescription> findStream(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey);

    @Nonnull
    Collection<StreamDescription> findStreams(@Nonnull NodeDescription nodeDescription, @Nonnull Collection<String> encryptedStreamKeys);

    @Nonnull
    Collection<StreamDescription> getAllStreams(@Nonnull NodeDescription nodeDescription);

    long size(@Nonnull NodeDescription nodeDescription);

    @Nonnull
    Collection<NodeDescription> getNodesByStream(@Nonnull String encryptedStreamKey);

    @Nonnull
    Collection<NodeDescription> getNodesByStreamInRegion(@Nonnull String encryptedStreamKey, @Nonnull String region);

    void removeStream(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey);

    @Nonnull
    List<StreamDescription> removeStreams(@Nonnull NodeDescription nodeDescription, @Nonnull Collection<String> encryptedStreamKeys);

    Collection<StreamDescription> removeAllStreams(@Nonnull NodeDescription nodeDescription);
}
