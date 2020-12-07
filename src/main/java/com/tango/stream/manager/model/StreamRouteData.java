package com.tango.stream.manager.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder
@AllArgsConstructor
public class StreamRouteData {
    @Nonnull
    String encryptedStreamKey;
    @Nonnull
    NodeReference nodeReference;
    int maxAvailableClients;
}
