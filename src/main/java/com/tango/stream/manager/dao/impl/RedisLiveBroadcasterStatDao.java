package com.tango.stream.manager.dao.impl;

import com.google.common.collect.ImmutableMap;
import com.tango.stream.manager.dao.LiveBroadcasterStatDao;
import com.tango.stream.manager.model.LiveStreamData;
import com.tango.stream.manager.model.ViewerCountStatsLive;
import com.tango.stream.manager.service.impl.ConfigurationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RExpirable;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;

@Service
@Slf4j
public class RedisLiveBroadcasterStatDao implements LiveBroadcasterStatDao {
    private static final Pair<String, Long> VIEWERS_TTL_SECONDS = Pair.of("redis.live.broadcaster.viewers.ttl.seconds", 3600L);
    public static final long ACCOUNT_ID_KEY = -1L;

    private final RedissonClient redissonClient;
    private final ConfigurationService configurationService;

    @Autowired
    public RedisLiveBroadcasterStatDao(RedissonClient redissonClient,
                                       ConfigurationService configurationService) {
        this.redissonClient = redissonClient;
        this.configurationService = configurationService;
    }

    @Override
    public void addViewer(LiveStreamData stream, String region) {
        RAtomicLong currentViewers = getCurrentViewers(stream, region);
        long count = currentViewers.incrementAndGet();
        updateTtl(currentViewers);
        recordViewersCount(stream, region, count);
    }

    @Override
    public void removeViewer(LiveStreamData stream, String region) {
        RAtomicLong currentViewers = getCurrentViewers(stream, region);
        long count = currentViewers.decrementAndGet();
        updateTtl(currentViewers);
        recordViewersCount(stream, region, count);
    }

    @Override
    public Optional<ViewerCountStatsLive> fetchStats(long streamId, String region) {
        RMap<Long, Long> stats = getStats(streamId, region);
        if (stats.isEmpty()) {
            return Optional.empty();
        }
        Map<Long, Long> map = stats.readAllMap();
        Long accountId = map.remove(ACCOUNT_ID_KEY);
        if (accountId == null) {
            log.warn("Corrupted viewers count statistics, account id is missing: streamId={}, region={}", streamId, region);
            return Optional.empty();
        }
        return Optional.of(
                ViewerCountStatsLive.builder()
                        .accountId(accountId)
                        .statistics(map)
                        .build()
        );
    }

    private void recordViewersCount(LiveStreamData stream, String region, long count) {
        log.debug("Recording viewer count statistics: stream={} region={}, count={}", stream, region, count);
        RMap<Long, Long> viewersCountStat = getStats(stream.getStreamId(), region);
        //there is a race on write operation. however we don't need last value, we just need any value for the given interval
        viewersCountStat.putAllAsync(
                ImmutableMap.of(
                        ACCOUNT_ID_KEY, stream.getStreamerAccountId(),
                        getTimeslot(), count
                )
        );
        updateTtl(viewersCountStat);
    }

    private RMap<Long, Long> getStats(long streamId, String region) {
        return redissonClient.getMap("VIEWERS_COUNT_STAT:" + streamId + ":" + region);
    }

    private long getTimeslot() {
        long nowMs = Instant.now().toEpochMilli() / 1000;
        long intervalSec = getMonitorIntervalSec();
        return nowMs / intervalSec;
    }

    private RAtomicLong getCurrentViewers(LiveStreamData stream, String region) {
        return redissonClient.getAtomicLong("VIEWERS_COUNT:" + stream.getStreamId() + ":" + region);
    }

    private long getMonitorIntervalSec() {
        return configurationService.get().getLong("stats.viewer.monitor.interval.seconds", 10L);
    }

    private void updateTtl(RExpirable rExpirable) {
        rExpirable.expireAsync(configurationService.get().getLong(VIEWERS_TTL_SECONDS.getKey(), VIEWERS_TTL_SECONDS.getValue()), SECONDS);
    }
}
