package com.tango.stream.manager.service.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.tango.stream.manager.exceptions.LockAcquiringFail;
import com.tango.stream.manager.model.ActionSpec;
import com.tango.stream.manager.model.CallSpec;
import com.tango.stream.manager.service.LocksService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.tango.stream.manager.model.Metrics.Counters.UNDER_LOCK_ERRORS;
import static com.tango.stream.manager.model.Metrics.Tags.ERROR_TYPE;

@Slf4j
@Service
public class LocksServiceImpl implements LocksService {
    private final Cache<String, Lock> localNamedLockCache;
    private final RedissonClient redissonClient;
    private final MeterRegistry meterRegistry;

    @Autowired
    public LocksServiceImpl(RedissonClient redissonClient,
                            MeterRegistry meterRegistry) {
        this.redissonClient = redissonClient;
        this.meterRegistry = meterRegistry;
        localNamedLockCache = CacheBuilder.newBuilder()
                .expireAfterAccess(Duration.ofMinutes(1L))
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends Exception> void doUnderLock(@Nonnull ActionSpec<E> actionSpec) throws E {
        if (actionSpec.getOutsideTimer() != null) {
            try {
                actionSpec.getOutsideTimer().recordCallable(() -> {
                    doUnderLocalLock(actionSpec);
                    return null;
                });
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw (E) e;
            }
        } else {
            doUnderLocalLock(actionSpec);
        }
    }

    @SuppressWarnings("unchecked")
    private <E extends Exception> void doUnderLocalLock(@Nonnull ActionSpec<E> actionSpec) throws E {
        List<Lock> localLocks = getOrCreateLocalLocks(actionSpec.getLockNames());
        List<Lock> acquiredLocks = Lists.newArrayList();
        try {
            //to decrease races from concurrent request to amount of nodes streammanager
            tryAcquireLocalLocks(localLocks, actionSpec.getLockNames(), actionSpec.getWaitTimeMs(), acquiredLocks);
            if (actionSpec.getInsideLocalTimer() != null) {
                actionSpec.getInsideLocalTimer().recordCallable(() -> {
                    doUnderDistributedLock(actionSpec);
                    return null;
                });
            } else {
                doUnderDistributedLock(actionSpec);
            }
            return;
        } catch (InterruptedException e) {
            log.error("doUnderLocalLock(): interrupted when work under lock: {}", actionSpec.getLockNames(), e);
            meterRegistry.counter(UNDER_LOCK_ERRORS, Tags.of(ERROR_TYPE, "interrupts")).increment();
            Thread.currentThread().interrupt();
        } catch (LockAcquiringFail e) {
            log.warn("doUnderLocalLock(): couldn't acquire {} lock {}", e.getMessage(), actionSpec.getLockNames(), e);
            meterRegistry.counter(UNDER_LOCK_ERRORS, Tags.of(ERROR_TYPE, e.getMessage())).increment();
        } catch (RuntimeException e) {
            if (actionSpec.getFallbackAction() != null) {
                actionSpec.getFallbackAction().run();
            }
            throw e;
        } catch (Exception e) {
            log.error("doUnderLocalLock(): unexpected error when work under lock {}", actionSpec.getLockNames(), e);
            meterRegistry.counter(UNDER_LOCK_ERRORS, Tags.of(ERROR_TYPE, "logic")).increment();
            if (actionSpec.getFallbackAction() != null) {
                actionSpec.getFallbackAction().run();
            }
            throw (E) e;
        } finally {
            releaseLocks(acquiredLocks);
        }
        if (actionSpec.getFallbackAction() != null) {
            actionSpec.getFallbackAction().run();
        }
    }

    @SuppressWarnings("unchecked")
    private <E extends Exception> void doUnderDistributedLock(@Nonnull ActionSpec<E> actionSpec) throws E {
        List<RLock> distributedLocks = getOrCreateDistributedLocks(actionSpec.getLockNames());
        List<RLock> acquiredLocks = Lists.newArrayList();
        try {
            tryAcquireDistributedLocks(distributedLocks, actionSpec.getLockNames(), actionSpec.getWaitTimeMs(), actionSpec.getLeaseTimeMs(), acquiredLocks);
            if (actionSpec.getInsideRemoteTimer() != null) {
                actionSpec.getInsideRemoteTimer().recordCallable(() -> {
                    actionSpec.getAction().run();
                    return null;
                });
            } else {
                actionSpec.getAction().run();
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw (E) ex;
        } finally {
            releaseLocks(acquiredLocks);
        }
    }

    @Override
    public <T> T doUnderLock(@Nonnull CallSpec<T> callSpec) throws Exception {
        try {
            if (callSpec.getOutsideTimer() != null) {
                return callSpec.getOutsideTimer().recordCallable(() -> doUnderLocalLock(callSpec));
            } else {
                return doUnderLocalLock(callSpec);
            }
        } catch (Exception e) {
            if (callSpec.getFallbackAction() != null) {
                return callSpec.getFallbackAction().call();
            }
            throw e;
        }
    }

    private <T> T doUnderLocalLock(@Nonnull CallSpec<T> callSpec) throws Exception {
        List<Lock> localLocks = getOrCreateLocalLocks(callSpec.getLockNames());
        List<Lock> acquiredLocks = Lists.newArrayList();
        try {
            //to decrease races from concurrent request to amount of nodes streammanager
            tryAcquireLocalLocks(localLocks, callSpec.getLockNames(), callSpec.getWaitTimeMs(), acquiredLocks);
            if (callSpec.getInsideLocalTimer() != null) {
                return callSpec.getInsideLocalTimer().recordCallable(() -> doUnderDistributedLock(callSpec));
            }
            return doUnderDistributedLock(callSpec);
        } catch (InterruptedException e) {
            log.error("doUnderLocalLock(): interrupted when work under lock: {}", callSpec.getLockNames(), e);
            meterRegistry.counter(UNDER_LOCK_ERRORS, Tags.of(ERROR_TYPE, "interrupts")).increment();
            Thread.currentThread().interrupt();
            throw e;
        } catch (LockAcquiringFail e) {
            log.warn("doUnderLocalLock(): couldn't acquire {} lock {}", e.getMessage(), callSpec.getLockNames(), e);
            meterRegistry.counter(UNDER_LOCK_ERRORS, Tags.of(ERROR_TYPE, e.getMessage())).increment();
            throw e;
        } catch (Exception e) {
            log.error("doUnderLocalLock(): unexpected error when work under lock {}", callSpec.getLockNames(), e);
            meterRegistry.counter(UNDER_LOCK_ERRORS, Tags.of(ERROR_TYPE, "logic")).increment();
            throw e;
        } finally {
            releaseLocks(acquiredLocks);
        }
    }

    @SneakyThrows
    private <T> T doUnderDistributedLock(@Nonnull CallSpec<T> callSpec) {
        List<RLock> distributedLocks = getOrCreateDistributedLocks(callSpec.getLockNames());
        List<RLock> acquiredLocks = Lists.newArrayList();
        try {
            tryAcquireDistributedLocks(distributedLocks, callSpec.getLockNames(), callSpec.getWaitTimeMs(), callSpec.getLeaseTimeMs(), acquiredLocks);
            if (callSpec.getInsideRemoteTimer() != null) {
                return callSpec.getInsideRemoteTimer().recordCallable(callSpec.getAction());
            }
            return callSpec.getAction().call();
        } finally {
            releaseLocks(acquiredLocks);
        }
    }

    private void tryAcquireLocalLocks(@Nonnull List<Lock> localLocks,
                                      @Nonnull List<String> lockNames,
                                      long waitTimeMs,
                                      @Nonnull List<Lock> acquiredLocks) throws InterruptedException {
        for (int i = 0; i < localLocks.size(); ++i) {
            Lock lock = localLocks.get(i);
            boolean lockAcquired = lock.tryLock(waitTimeMs, TimeUnit.MILLISECONDS);
            if (!lockAcquired) {
                throw new LockAcquiringFail("try_llock_fail: " + lockNames.get(i));
            }
            acquiredLocks.add(lock);
        }
    }

    private void releaseLocks(@Nonnull List<? extends Lock> localLocks) {
        localLocks.forEach(Lock::unlock);
    }

    @Nonnull
    @SneakyThrows
    private Lock getOrCreateLocalLock(@Nonnull String name) {
        return localNamedLockCache.get(name, ReentrantLock::new);
    }

    @Nonnull
    private List<Lock> getOrCreateLocalLocks(@Nonnull List<String> lockNames) {
        return lockNames.stream().map(this::getOrCreateLocalLock).collect(Collectors.toList());
    }

    @Nonnull
    private List<RLock> getOrCreateDistributedLocks(@Nonnull List<String> lockNames) {
        return lockNames.stream().map(redissonClient::getLock).collect(Collectors.toList());
    }

    private void tryAcquireDistributedLocks(@Nonnull List<RLock> distributedLocks,
                                            @Nonnull List<String> lockNames,
                                            long waitTimeMs,
                                            long leaseTimeMs,
                                            @Nonnull List<RLock> acquiredLocks) throws InterruptedException {
        for (int i = 0; i < distributedLocks.size(); ++i) {
            RLock lock = distributedLocks.get(i);
            boolean lockAcquired = lock.tryLock(waitTimeMs, leaseTimeMs, TimeUnit.MILLISECONDS);
            if (!lockAcquired) {
                log.warn("doUnderDistributedLock(): couldn't acquire distributed lock: " + lockNames.get(i));
                meterRegistry.counter(UNDER_LOCK_ERRORS, Tags.of(ERROR_TYPE, "try_rlock_fail")).increment();
                throw new LockAcquiringFail("try_rlock_fail: " + lockNames.get(i));
            }
            acquiredLocks.add(lock);
        }
    }
}
