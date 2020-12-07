package com.tango.stream.manager.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.Set;

//edge specific
@Value
@Builder
public class StreamRedirectData {
    @NonNull
    String encryptedStreamKey;

    @NonNull
    NodeReference nodeReference;

    @NonNull
    Set<String> clients;
}
