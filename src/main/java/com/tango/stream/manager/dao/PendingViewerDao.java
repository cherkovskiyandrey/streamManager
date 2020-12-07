package com.tango.stream.manager.dao;

import com.tango.stream.manager.model.NodeDescription;

import javax.annotation.Nonnull;
import java.util.Collection;


public interface PendingViewerDao extends ViewerDao<String> {
    @Nonnull
    Collection<String> getAndRemoveExpired(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey);
}
