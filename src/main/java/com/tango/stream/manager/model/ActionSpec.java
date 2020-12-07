package com.tango.stream.manager.model;

import com.google.common.collect.Lists;
import com.tango.stream.manager.utils.RunnableWithException;
import io.micrometer.core.instrument.Timer;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder
public class ActionSpec<E extends Exception> {
    /**
     * Ordering of locks is caller responsibility!
     */
    @Nonnull
    @NonNull
    @Builder.Default
    List<String> lockNames = Lists.newArrayList();
    @Nonnull
    @NonNull
    RunnableWithException<E> action;
    long waitTimeMs;
    long leaseTimeMs;
    @Nullable
    RunnableWithException<E> fallbackAction;
    @Nullable
    Timer outsideTimer;
    @Nullable
    Timer insideLocalTimer;
    @Nullable
    Timer insideRemoteTimer;
}
