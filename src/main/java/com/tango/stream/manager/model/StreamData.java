package com.tango.stream.manager.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.*;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StreamData {
    @NonNull String encryptedStreamKey;
    @Nullable
    String commandUid;
    long commandTimestamp;
    @Builder.Default
    @NonNull Map<String, Long> lastFragmentSizeInBytes = new HashMap<>();
    int viewersCount;
    int ingressBitrate;
    int egressBitrate;
    NodeReference allowedConnection;
    @Builder.Default
    List<NodeReference> actualConnections = Lists.newArrayList();
    @Builder.Default
    List<NodeReference> failedConnections = Lists.newArrayList();
    long startTimeToLostConnection;
    @Builder.Default
    List<NodeReference> oldConnections = Lists.newArrayList();
    @With
    int maxAvailableClients;
    @Builder.Default
    Set<String> actualViewers = Sets.newHashSet();
}
