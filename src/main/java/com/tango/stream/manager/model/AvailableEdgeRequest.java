package com.tango.stream.manager.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Data
@NoArgsConstructor
public class AvailableEdgeRequest {
    @Nonnull
    @NonNull
    String encryptedStreamKey;
    @Nullable
    String region;
    @Nullable
    Long viewerAccountId;
    @Nullable
    String viewerUsername;
}
