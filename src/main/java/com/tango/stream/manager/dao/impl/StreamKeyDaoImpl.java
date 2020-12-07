package com.tango.stream.manager.dao.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.tango.stream.manager.dao.StreamDao;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.service.impl.ConfigurationService;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.redisson.api.RExpirable;
import org.redisson.api.RMap;
import org.redisson.api.RSetMultimap;
import org.redisson.api.RedissonClient;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
public class StreamKeyDaoImpl<T> implements StreamDao<T> {
    protected final String daoName;
    protected final RedissonClient redissonClient;
    protected final ConfigurationService configurationService;
    protected final Function<T, String> keyMapper;

    @Override
    public void addStream(@NotNull NodeDescription nodeDescription, @NotNull T stream) {
        String key = keyMapper.apply(stream);

        RMap<String, T> streamsByNode = getStreamsByNode(nodeDescription);
        streamsByNode.put(key, stream);
        updateTtl(streamsByNode);

        RSetMultimap<String, NodeDescription> nodes = getNodesByStreamAndRegion(key);
        nodes.put(nodeDescription.getRegion(), nodeDescription);
        updateTtl(nodes);
    }

    @Override
    public void addStreams(@NotNull NodeDescription nodeDescription, @Nonnull Collection<T> streams) {
        Map<String, T> keyMap = streams.stream()
                .collect(Collectors.toMap(keyMapper, Function.identity()));

        RMap<String, T> streamsByNode = getStreamsByNode(nodeDescription);
        streamsByNode.putAll(keyMap);
        updateTtl(streamsByNode);

        for (String key : keyMap.keySet()) {
            RSetMultimap<String, NodeDescription> nodes = getNodesByStreamAndRegion(key);
            nodes.put(nodeDescription.getRegion(), nodeDescription);
            updateTtl(nodes);
        }
    }

    @NotNull
    @Override
    public Optional<T> findStream(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey) {
        return Optional.ofNullable(getStreamsByNode(nodeDescription).get(encryptedStreamKey));
    }

    @Nonnull
    @Override
    public Collection<T> findStreams(@Nonnull NodeDescription nodeDescription, @Nonnull Collection<String> encryptedStreamKeys) {
        return getStreamsByNode(nodeDescription).getAll(Sets.newHashSet(encryptedStreamKeys)).values();
    }

    @NotNull
    @Override
    public Collection<T> getAllStreams(@NotNull NodeDescription nodeDescription) {
        return getStreamsByNode(nodeDescription).values();
    }

    @Override
    public long size(@NotNull NodeDescription nodeDescription) {
        return getStreamsByNode(nodeDescription).size();
    }

    @NotNull
    @Override
    public Collection<NodeDescription> getNodesByStream(@NotNull String encryptedStreamKey) {
        return getNodesByStreamAndRegion(encryptedStreamKey).values();
    }

    @NotNull
    @Override
    public Collection<NodeDescription> getNodesByStreamInRegion(@NotNull String encryptedStreamKey, @NotNull String region) {
        return getNodesByStreamAndRegion(encryptedStreamKey).get(region).readAll();
    }

    @Override
    public void removeStream(@NotNull NodeDescription nodeDescription, @NotNull String encryptedStreamKey) {
        RMap<String, T> streamsByNode = getStreamsByNode(nodeDescription);
        streamsByNode.remove(encryptedStreamKey);
        updateTtl(streamsByNode);

        RSetMultimap<String, NodeDescription> nodesByStreamAndRegion = getNodesByStreamAndRegion(encryptedStreamKey);
        nodesByStreamAndRegion.remove(nodeDescription.getRegion(), nodeDescription);
        updateTtl(nodesByStreamAndRegion);
    }

    @NotNull
    @Override
    public List<T> removeStreams(@NotNull NodeDescription nodeDescription, @NotNull Collection<String> encryptedStreamKeys) {
        RMap<String, T> streamsByNode = getStreamsByNode(nodeDescription);
        List<T> removed = encryptedStreamKeys.stream()
                .map(encryptedStreamKey -> {
                    RSetMultimap<String, NodeDescription> nodesByStreamAndRegion = getNodesByStreamAndRegion(encryptedStreamKey);
                    nodesByStreamAndRegion.remove(nodeDescription.getRegion(), nodeDescription);
                    updateTtl(nodesByStreamAndRegion);
                    return streamsByNode.remove(encryptedStreamKey);
                })
                .filter(Objects::nonNull)
                .collect(toList());
        updateTtl(streamsByNode);

        return removed;
    }

    @Override
    public Collection<T> removeAllStreams(@NotNull NodeDescription nodeDescription) {
        RMap<String, T> streamsByNode = getStreamsByNode(nodeDescription);
        Collection<T> ids = ImmutableList.copyOf(streamsByNode.values());
        streamsByNode.keySet().forEach(encryptedStreamKey -> {
            RSetMultimap<String, NodeDescription> nodesByStreamAndRegion = getNodesByStreamAndRegion(encryptedStreamKey);
            nodesByStreamAndRegion.remove(nodeDescription.getRegion(), nodeDescription);
        });
        streamsByNode.unlink();
        return ids;
    }

    private RMap<String, T> getStreamsByNode(@NotNull NodeDescription nodeDescription) {
        return redissonClient.getMap(String.format("STREAM_DAO:%s:node:%s:streams", daoName, nodeDescription.getNodeUid()));
    }

    private RSetMultimap<String, NodeDescription> getNodesByStreamAndRegion(String streamKey) {
        return redissonClient.getSetMultimap(String.format("STREAM_DAO:%s:stream:%s:regionNodes", daoName, streamKey));
    }

    protected long getTtlSec() {
        return configurationService.get().getLong("stream.dao.streams.ttl.sec", TimeUnit.HOURS.toSeconds(1L));
    }

    protected void updateTtl(RExpirable rExpirable) {
        rExpirable.expireAsync(getTtlSec(), SECONDS);
    }
}
