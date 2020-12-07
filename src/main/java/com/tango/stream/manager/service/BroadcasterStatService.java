package com.tango.stream.manager.service;

import com.tango.stream.manager.model.ViewersCapacity;

import javax.annotation.Nonnull;

public interface BroadcasterStatService {

    void onAddViewer(@Nonnull String encryptedStreamKey,
                     @Nonnull String viewerRegion,
                     @Nonnull String viewerEncryptedId);

    void onRemoveViewer(@Nonnull String encryptedStreamKey,
                        @Nonnull String viewerRegion,
                        @Nonnull String viewerEncryptedId);

    void onStreamTermination(String encryptedStreamKey);

    @Nonnull
    ViewersCapacity getInitialViewersCapacity(long accountId);
}
