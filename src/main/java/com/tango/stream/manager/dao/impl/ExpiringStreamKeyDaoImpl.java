package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.dao.ExpiringStreamDao;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.service.impl.ConfigurationService;
import org.jetbrains.annotations.NotNull;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class ExpiringStreamKeyDaoImpl<T> extends StreamKeyDaoImpl<T> implements ExpiringStreamDao<T> {

    private final Clock clock;
    private final Function<ConfigurationService, Integer> expireAfterSecondsMapper;

    public ExpiringStreamKeyDaoImpl(String daoName,
                                    RedissonClient redissonClient,
                                    ConfigurationService configurationService,
                                    Function<T, String> keyMapper,
                                    Clock clock,
                                    Function<ConfigurationService, Integer> expireAfterSecondsMapper) {
        super(daoName, redissonClient, configurationService, keyMapper);
        this.clock = clock;
        this.expireAfterSecondsMapper = expireAfterSecondsMapper;
    }

    @NotNull
    @Override
    public Collection<String> getAndRemoveExpired(@NotNull NodeDescription nodeDescription) {
        RScoredSortedSet<String> sortedSet = getSortedSetByNode(nodeDescription);
        Instant expirationTimestamp = getExpirationTimestamp();

        Collection<String> expired = sortedSet.valueRange(0, true, expirationTimestamp.toEpochMilli(), true);
        removeStreams(nodeDescription, expired);
        
        return expired;
    }

    @Override
    public void addStream(@NotNull NodeDescription nodeDescription, @NotNull T stream) {
        super.addStream(nodeDescription, stream);
        RScoredSortedSet<String> sortedSet = getSortedSetByNode(nodeDescription);
        sortedSet.add(clock.millis(), keyMapper.apply(stream));
        updateTtl(sortedSet);
    }

    @Override
    public void addStreams(@NotNull NodeDescription nodeDescription, @NotNull Collection<T> streams) {
        super.addStreams(nodeDescription, streams);

        long millis = clock.millis();
        Map<String, Double> scores = streams.stream().collect(Collectors.toMap(keyMapper, k -> ((double) millis)));

        RScoredSortedSet<String> sortedSet = getSortedSetByNode(nodeDescription);
        sortedSet.addAll(scores);
        updateTtl(sortedSet);
    }

    @Override
    public void removeStream(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey) {
        super.removeStream(nodeDescription, encryptedStreamKey);

        RScoredSortedSet<String> sortedSet = getSortedSetByNode(nodeDescription);
        sortedSet.remove(encryptedStreamKey);
        updateTtl(sortedSet);
    }

    @Override
    public @NotNull List<T> removeStreams(@NotNull NodeDescription nodeDescription, @NotNull Collection<String> encryptedStreamKeys) {
        List<T> removed = super.removeStreams(nodeDescription, encryptedStreamKeys);

        RScoredSortedSet<String> sortedSet = getSortedSetByNode(nodeDescription);
        sortedSet.removeAll(encryptedStreamKeys);
        updateTtl(sortedSet);

        return removed;
    }

    @Override
    public Collection<T> removeAllStreams(@NotNull NodeDescription nodeDescription) {
        Collection<T> removed = super.removeAllStreams(nodeDescription);
        getSortedSetByNode(nodeDescription).unlink();
        return removed;
    }

    private RScoredSortedSet<String> getSortedSetByNode(@NotNull NodeDescription nodeDescription) {
        return redissonClient.getScoredSortedSet(String.format("EXPIRING_STREAM_DAO:%s:node:%s:sortedStreams",
                daoName, nodeDescription.getNodeUid()));
    }

    @Nonnull
    private Instant getExpirationTimestamp() {
        return clock.instant().minusSeconds(getExpirationPeriodInSec());
    }

    @Override
    public int getExpirationPeriodInSec() {
        return expireAfterSecondsMapper.apply(configurationService);
    }
}
