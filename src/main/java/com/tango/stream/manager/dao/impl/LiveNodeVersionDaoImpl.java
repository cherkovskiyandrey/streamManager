package com.tango.stream.manager.dao.impl;

import com.google.common.collect.Lists;
import com.tango.stream.manager.dao.LiveNodeVersionDao;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.service.impl.ConfigurationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.api.RSetCache;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Repository;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Repository
@RequiredArgsConstructor
public class LiveNodeVersionDaoImpl implements LiveNodeVersionDao {
    static final Pair<String, Long> LIVE_VERSIONS_TTL = Pair.of("versions.live.sec", 60L);
    private final static String LIVE_VERSIONS_PREFIX = "SRT_LIVE_VERSIONS";
    private final RedissonClient redissonClient;
    private final ConfigurationService configurationService;

    @Override
    public void addVersionAsync(@Nonnull NodeType nodeType, @Nonnull String version) {
        RSetCache<String> liveRegionSet = getCacheSet(nodeType);
        liveRegionSet.addAsync(version, getTtlInSec(), TimeUnit.SECONDS)
                .thenAcceptAsync(result -> liveRegionSet.expireAsync(getTtlInSec(), TimeUnit.SECONDS));
    }

    @Override
    public void addVersion(@Nonnull NodeType nodeType, @Nonnull String version) {
        RSetCache<String> liveRegionSet = getCacheSet(nodeType);
        liveRegionSet.add(version, getTtlInSec(), TimeUnit.SECONDS);
        liveRegionSet.expireAsync(getTtlInSec(), TimeUnit.SECONDS);
    }

    @Nonnull
    @Override
    public List<String> getAllVersions(@Nonnull NodeType nodeType) {
        return Lists.newArrayList(getCacheSet(nodeType).readAll());
    }

    @Nonnull
    private RSetCache<String> getCacheSet(@Nonnull NodeType nodeType) {
        return redissonClient.getSetCache(getKeyName(nodeType));
    }

    private String getKeyName(@Nonnull NodeType nodeType) {
        return String.join(":", LIVE_VERSIONS_PREFIX, nodeType.name());
    }

    private long getTtlInSec() {
        return configurationService.get().getLong(LIVE_VERSIONS_TTL.getLeft(), LIVE_VERSIONS_TTL.getRight());
    }
}
