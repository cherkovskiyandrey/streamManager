package com.tango.stream.manager.model;

import com.google.common.collect.Sets;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

@Data
@NoArgsConstructor
public class AvailableGatewayRequest {
    long streamerAccountId;
    @Nonnull
    String encryptedStreamKey;
    @Nullable
    String region;
    @Nonnull
    Set<String> excludedGateways = Sets.newHashSet();
//
//    @JsonCreator
//    public AvailableGatewayRequest(
//            long streamerAccountId,
//            @Nonnull String encryptedStreamKey,
//            @Nullable String region,
//            @Nullable Set<String> excludedGateways) {
//        this.streamerAccountId = streamerAccountId;
//        this.encryptedStreamKey = encryptedStreamKey;
//        this.region = region;
//        this.excludedGateways = excludedGateways != null ? excludedGateways : Sets.newHashSet();
//    }
}
