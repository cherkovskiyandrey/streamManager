package com.tango.stream.manager.model;

import com.google.common.collect.Lists;
import io.micrometer.core.instrument.Timer;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.Callable;

@Value
@Builder
public class CallSpec<T> {
    /**
     * Ordering of locks is caller responsibility!
     */
    @Nonnull
    @NonNull
    @Builder.Default
    List<String> lockNames = Lists.newArrayList();
    @Nonnull
    @NonNull
    Callable<T> action;
    long waitTimeMs;
    long leaseTimeMs;
    @Nullable
    Callable<T> fallbackAction;
    @Nullable
    Timer outsideTimer;
    @Nullable
    Timer insideLocalTimer;
    @Nullable
    Timer insideRemoteTimer;
}
