package com.tango.stream.manager.model;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class StreamKey {
    long streamId;
    long initTime;
}
