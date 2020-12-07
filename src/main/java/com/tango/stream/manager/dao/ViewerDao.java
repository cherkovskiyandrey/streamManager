package com.tango.stream.manager.dao;

import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.tango.stream.manager.model.NodeDescription;

import javax.annotation.Nonnull;
import java.util.Set;

public interface ViewerDao<ViewerIdentifier> {

    void addViewer(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey, @Nonnull ViewerIdentifier viewers);

    void addViewers(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey, @Nonnull Set<ViewerIdentifier> viewers);

    boolean isViewerExists(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey, @Nonnull ViewerIdentifier viewer);

    int viewersSize(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey);

    @Nonnull
    Set<ViewerIdentifier> getNodeAllViewers(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey);

    @Nonnull
    SetMultimap<String, ViewerIdentifier> getNodeAllViewers(@Nonnull NodeDescription nodeDescription, @Nonnull Set<String> encryptedStreamKeys);

    int getAllViewersSize(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey);

    @Nonnull
    Multimap<String, ViewerIdentifier> getNodeAllViewers(@Nonnull NodeDescription nodeDescription);

    Set<String> getAllKnownStreams(@Nonnull NodeDescription nodeDescription);

    int getAllViewersSize(@Nonnull NodeDescription nodeDescription);

    void removeViewers(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey, @Nonnull Set<ViewerIdentifier> viewers);

    void removeAllViewers(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey);

    void removeAllViewers(@Nonnull NodeDescription nodeDescription);
}
