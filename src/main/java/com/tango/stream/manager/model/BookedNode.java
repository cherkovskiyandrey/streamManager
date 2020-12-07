package com.tango.stream.manager.model;

import lombok.Builder;
import lombok.Value;
import lombok.With;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Value
@Builder
public class BookedNode {
    @Nonnull
    NodeDescription nodeDescription;
    @Nonnull
    String encryptedStreamId;
    @With
    int bookedClients;
    @Nullable
    NodeDescription hostNode;
}
