package com.tango.stream.manager.model;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.With;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

@Data
@Builder
public class StreamDescription {
    @NonNull
    String encryptedStreamKey;
    @Nullable
    String commandUid;
    long commandTimestamp;
    @Builder.Default
    @NonNull Map<String, Long> lastFragmentSizeInBytes = new HashMap<>();
    int viewersCount;
    int ingressBitrate;
    int egressBitrate;
    NodeDescription hostNode;
    long startTimeToLostConnection;
    int successKeepsAliveFromLostingHostNode;
    @With
    int maxAvailableClients;
}
