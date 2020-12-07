package com.tango.stream.manager.dao.impl;

import com.google.common.collect.*;
import com.tango.stream.manager.dao.PendingViewerDao;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.service.impl.ConfigurationService;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.api.SortOrder;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;

@RequiredArgsConstructor
public class PendingViewerDaoImpl implements PendingViewerDao {
    private final static Pair<String, Integer> PENDING_VIEWERS_EXPIRATION_SEC = Pair.of("viewers.dao.pending.expiration.seconds", 10);
    private final static Pair<String, Integer> PENDING_VIEWERS_TTL_SEC = Pair.of("viewers.dao.pending.viewers.ttl.sec", 3600);

    private final String daoName;
    private final RedissonClient redissonClient;
    private final ConfigurationService configurationService;

    @NotNull
    @Override
    public Collection<String> getAndRemoveExpired(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey) {
        RScoredSortedSet<String> viewersScoredSet = getViewersScoredSet(nodeDescription, encryptedStreamKey);
        Instant expirationTimestamp = getExpirationTimestamp();
        Collection<String> expired = viewersScoredSet.valueRange(0, true, expirationTimestamp.toEpochMilli(), false);
        viewersScoredSet.removeAll(expired);
        viewersScoredSet.expireAsync(getStreamViewersTtlSec(), TimeUnit.SECONDS);
        drainStreamSet(nodeDescription, encryptedStreamKey);
        return expired;
    }

    @Override
    public void addViewer(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey, @NotNull String viewer) {
        addViewers(nodeDescription, encryptedStreamKey, singleton(viewer));
    }

    @Override
    public void addViewers(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey, @NotNull Set<String> viewers) {
        RSet<String> streamsSet = getStreamsSet(nodeDescription);
        streamsSet.add(encryptedStreamKey);
        streamsSet.expireAsync(getStreamViewersTtlSec(), TimeUnit.SECONDS);

        RScoredSortedSet<String> viewersScoredSet = getViewersScoredSet(nodeDescription, encryptedStreamKey);
        long timestamp = Instant.now().toEpochMilli();
        for (String viewer : viewers) {
            if (!viewersScoredSet.contains(viewer)) {
                viewersScoredSet.add(timestamp, viewer);
            }
        }
        viewersScoredSet.expireAsync(getStreamViewersTtlSec(), TimeUnit.SECONDS);
    }

    @Override
    public boolean isViewerExists(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey, @Nonnull String viewer) {
        return getViewersScoredSet(nodeDescription, encryptedStreamKey).contains(viewer);
    }

    @Override
    public int viewersSize(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey) {
        return getViewersScoredSet(nodeDescription, encryptedStreamKey).size();
    }

    @Nonnull
    @Override
    public Set<String> getNodeAllViewers(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey) {
        return getViewersScoredSet(nodeDescription, encryptedStreamKey).readSort(SortOrder.ASC);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    @Override
    public SetMultimap<String, String> getNodeAllViewers(@Nonnull NodeDescription nodeDescription, @Nonnull Set<String> encryptedStreamKeys) {
        return encryptedStreamKeys.stream()
                .map(stream -> Pair.of(stream, getNodeAllViewers(nodeDescription, stream)))
                .collect(Multimaps.flatteningToMultimap(
                        Pair::getLeft,
                        pair -> pair.getRight().stream(),
                        MultimapBuilder.hashKeys().hashSetValues()::build
                ));
    }

    @Override
    public int getAllViewersSize(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey) {
        return getViewersScoredSet(nodeDescription, encryptedStreamKey).size();
    }

    @NotNull
    @Override
    public Multimap<String, String> getNodeAllViewers(@NotNull NodeDescription nodeDescription) {
        ListMultimap<String, String> result = MultimapBuilder.hashKeys().arrayListValues().build();
        RSet<String> streams = getStreamsSet(nodeDescription);
        for (String stream : streams) {
            RScoredSortedSet<String> viewers = getViewersScoredSet(nodeDescription, stream);
            result.putAll(stream, viewers);
        }
        return result;
    }

    @Override
    public Set<String> getAllKnownStreams(@Nonnull NodeDescription nodeDescription) {
        return getStreamsSet(nodeDescription).readAll();
    }

    @Override
    public int getAllViewersSize(@NotNull NodeDescription nodeDescription) {
        return getStreamsSet(nodeDescription).stream()
                .mapToInt(stream -> getAllViewersSize(nodeDescription, stream))
                .sum();
    }

    @Override
    public void removeViewers(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey, @NotNull Set<String> viewers) {
        RScoredSortedSet<String> viewersScoredSet = getViewersScoredSet(nodeDescription, encryptedStreamKey);
        viewersScoredSet.removeAll(viewers);
        viewersScoredSet.expireAsync(getStreamViewersTtlSec(), TimeUnit.SECONDS);
        drainStreamSet(nodeDescription, encryptedStreamKey);
    }

    @Override
    public void removeAllViewers(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey) {
        getViewersScoredSet(nodeDescription, encryptedStreamKey).unlink();
        drainStreamSet(nodeDescription, encryptedStreamKey);
    }

    @Override
    public void removeAllViewers(@NotNull NodeDescription nodeDescription) {
        // need to extract all values at once to avoid concurrent modification
        Set<String> streamsSet = getStreamsSet(nodeDescription).readAll();
        streamsSet.forEach(stream -> removeAllViewers(nodeDescription, stream));
    }

    private RScoredSortedSet<String> getViewersScoredSet(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey) {
        String key = String.join(":", "VIEWERS_SCORED", nodeDescription.getNodeUid(), encryptedStreamKey);
        return redissonClient.getScoredSortedSet(key);
    }

    private RSet<String> getStreamsSet(@NotNull NodeDescription nodeDescription) {
        return redissonClient.getSet("VIEWERS:" + daoName + ":" + nodeDescription.getNodeUid());
    }

    private void drainStreamSet(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey) {
        if (getViewersScoredSet(nodeDescription, encryptedStreamKey).isEmpty()) {
            RSet<String> streamsSet = getStreamsSet(nodeDescription);
            streamsSet.remove(encryptedStreamKey);
            streamsSet.expireAsync(getStreamViewersTtlSec(), TimeUnit.SECONDS);
        }
    }

    @Nonnull
    private Instant getExpirationTimestamp() {
        long expirationSeconds = configurationService.get()
                .getLong(PENDING_VIEWERS_EXPIRATION_SEC.getLeft(), PENDING_VIEWERS_EXPIRATION_SEC.getRight());
        return Instant.now().minusSeconds(expirationSeconds);
    }

    private long getStreamViewersTtlSec() {
        return configurationService.get().getLong(PENDING_VIEWERS_TTL_SEC.getLeft(), PENDING_VIEWERS_TTL_SEC.getRight());
    }
}
