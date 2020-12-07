package com.tango.stream.manager.model;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class LiveStreamData {
    long streamerAccountId;
    long streamId;
    String encryptedStreamKey;
}
