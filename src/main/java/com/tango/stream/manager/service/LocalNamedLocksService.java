package com.tango.stream.manager.service;

import javax.annotation.Nonnull;
import java.util.concurrent.locks.Lock;

public interface LocalNamedLocksService {

    @Nonnull
    Lock getOrCreateLock(@Nonnull String name);
}
