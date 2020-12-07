package com.tango.stream.manager.dao.impl;

import com.google.common.collect.Lists;
import com.tango.stream.manager.dao.LiveRegionDao;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.service.impl.ConfigurationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Repository;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Repository
@RequiredArgsConstructor
public class LiveRegionDaoImpl implements LiveRegionDao {
    static final Pair<String, Long> LIVE_REGIONS_TTL = Pair.of("regions.live.ttl.sec", 60L);
    private final static String LIVE_REGIONS_PREFIX = "SRT_LIVE_REGIONS";
    private final RedissonClient redissonClient;
    private final ConfigurationService configurationService;

    @Override
    public void addRegionAsync(@Nonnull NodeType nodeType, @Nonnull String region) {
        RSet<String> liveRegionSet = getSet(nodeType);
        liveRegionSet.addAsync(region)
                .thenAcceptAsync(result -> liveRegionSet.expireAsync(getTtlInSec(), TimeUnit.SECONDS));
    }

    @Override
    public void addRegion(@Nonnull NodeType nodeType, @Nonnull String region) {
        RSet<String> liveRegionSet = getSet(nodeType);
        liveRegionSet.add(region);
        liveRegionSet.expireAsync(getTtlInSec(), TimeUnit.SECONDS);
    }

    @Nonnull
    @Override
    public List<String> getAllRegions(@Nonnull NodeType nodeType) {
        return Lists.newArrayList(getSet(nodeType).readAll());
    }

    @Nonnull
    private RSet<String> getSet(@Nonnull NodeType nodeType) {
        return redissonClient.getSet(getKeyName(nodeType));
    }

    private String getKeyName(@Nonnull NodeType nodeType) {
        return String.join(":", LIVE_REGIONS_PREFIX, nodeType.name());
    }

    private long getTtlInSec() {
        return configurationService.get().getLong(LIVE_REGIONS_TTL.getLeft(), LIVE_REGIONS_TTL.getRight());
    }
}
