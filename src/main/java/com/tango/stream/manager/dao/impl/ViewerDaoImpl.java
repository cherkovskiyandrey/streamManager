package com.tango.stream.manager.dao.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.tango.stream.manager.dao.ViewerDao;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.service.impl.ConfigurationService;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.redisson.api.RExpirable;
import org.redisson.api.RSetMultimap;
import org.redisson.api.RedissonClient;

import javax.annotation.Nonnull;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;

@RequiredArgsConstructor
public class ViewerDaoImpl<ViewerIdentifier> implements ViewerDao<ViewerIdentifier> {
    private static final Pair<String, Integer> STREAM_TO_VIEWERS_TTL = Pair.of("viewers.dao.stream.to.viewers.ttl.sec", 3600);

    private final String daoName;
    private final RedissonClient redissonClient;
    private final ConfigurationService configurationService;

    @Override
    public void addViewer(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey, @NotNull ViewerIdentifier viewer) {
        RSetMultimap<String, ViewerIdentifier> viewersMap = getStreamToViewersMap(nodeDescription);
        viewersMap.put(encryptedStreamKey, viewer);
        updateTtl(viewersMap);
    }

    @Override
    public void addViewers(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey, @NotNull Set<ViewerIdentifier> viewers) {
        RSetMultimap<String, ViewerIdentifier> viewersMap = getStreamToViewersMap(nodeDescription);
        viewersMap.putAll(encryptedStreamKey, viewers);
        updateTtl(viewersMap);
    }

    @Override
    public boolean isViewerExists(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey, @Nonnull ViewerIdentifier viewer) {
        return getStreamToViewersMap(nodeDescription).get(encryptedStreamKey).contains(viewer);
    }

    @Override
    public int viewersSize(@Nonnull NodeDescription nodeDescription, @Nonnull String encryptedStreamKey) {
        return getStreamToViewersMap(nodeDescription).get(encryptedStreamKey).size();
    }

    @Override
    public @NotNull Set<ViewerIdentifier> getNodeAllViewers(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey) {
        return getStreamToViewersMap(nodeDescription).get(encryptedStreamKey).readAll();
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    @Override
    public SetMultimap<String, ViewerIdentifier> getNodeAllViewers(@Nonnull NodeDescription nodeDescription, @Nonnull Set<String> encryptedStreamKeys) {
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
        return getStreamToViewersMap(nodeDescription).get(encryptedStreamKey).size();
    }

    @Override
    public @NotNull Multimap<String, ViewerIdentifier> getNodeAllViewers(@NotNull NodeDescription nodeDescription) {
        RSetMultimap<String, ViewerIdentifier> streamViewersMap = getStreamToViewersMap(nodeDescription);

        Multimap<String, ViewerIdentifier> result = MultimapBuilder.hashKeys().arrayListValues().build();
        streamViewersMap.entries().forEach(entry -> result.put(entry.getKey(), entry.getValue()));
        return result;
    }

    @Override
    public Set<String> getAllKnownStreams(@Nonnull NodeDescription nodeDescription) {
        return getStreamToViewersMap(nodeDescription).readAllKeySet();
    }

    @Override
    public int getAllViewersSize(@NotNull NodeDescription nodeDescription) {
        return getStreamToViewersMap(nodeDescription).size();
    }

    @Override
    public void removeViewers(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey, @NotNull Set<ViewerIdentifier> viewers) {
        RSetMultimap<String, ViewerIdentifier> viewersMap = getStreamToViewersMap(nodeDescription);
        viewersMap.get(encryptedStreamKey).removeAll(viewers);
        updateTtl(viewersMap);
    }

    @Override
    public void removeAllViewers(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey) {
        RSetMultimap<String, ViewerIdentifier> viewersMap = getStreamToViewersMap(nodeDescription);
        viewersMap.fastRemove(encryptedStreamKey);
        updateTtl(viewersMap);
    }

    @Override
    public void removeAllViewers(@NotNull NodeDescription nodeDescription) {
        getStreamToViewersMap(nodeDescription).unlink();
    }

    private RSetMultimap<String, ViewerIdentifier> getStreamToViewersMap(@NotNull NodeDescription nodeDescription) {
        return redissonClient.getSetMultimap(String.join(":", "STREAM_TO_VIEWERS", daoName, nodeDescription.getNodeUid()));
    }

    private long getStreamToViewersMapTlSec() {
        return configurationService.get().getLong(STREAM_TO_VIEWERS_TTL.getLeft(), STREAM_TO_VIEWERS_TTL.getRight());
    }

    protected void updateTtl(RExpirable rExpirable) {
        rExpirable.expireAsync(getStreamToViewersMapTlSec(), SECONDS);
    }
}
