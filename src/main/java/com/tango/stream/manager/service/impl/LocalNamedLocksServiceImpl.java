package com.tango.stream.manager.service.impl;

import com.tango.stream.manager.service.LocalNamedLocksService;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class LocalNamedLocksServiceImpl implements LocalNamedLocksService {
    private final ConcurrentMap<String, Lock> locks = new ConcurrentHashMap<>();

    @Override
    @Nonnull
    public Lock getOrCreateLock(@Nonnull String name) {
        return locks.computeIfAbsent(name, n -> new ReentrantLock());
    }
}
