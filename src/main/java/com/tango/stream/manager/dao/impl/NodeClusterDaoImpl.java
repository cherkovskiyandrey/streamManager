package com.tango.stream.manager.dao.impl;

import com.google.common.collect.Lists;
import com.tango.stream.manager.dao.NodeClusterDao;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.service.impl.ConfigurationService;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


@RequiredArgsConstructor
class NodeClusterDaoImpl implements NodeClusterDao {
    private static final Pair<String, Long> NODE_CLUSTER_TTL = Pair.of("node.cluster.ttl.sec", 60L);
    private static final String NODE_CLUSTER_PREFIX = "SRT_NODE_CLUSTER";
    private final NodeType nodeType;
    private final String name;
    private final RedissonClient redissonClient;
    private final ConfigurationService configurationService;

    @Override
    public void addNodeToCluster(@Nonnull NodeDescription nodeDescription, long score) {
        RScoredSortedSet<NodeDescription> clusterSet = getClusterSet(nodeDescription);
        clusterSet.add(score, nodeDescription);
        expireAsync(clusterSet);
    }

    @Override
    public void removeNodeFromCluster(@Nonnull NodeDescription nodeDescription) {
        RScoredSortedSet<NodeDescription> clusterSet = getClusterSet(nodeDescription);
        clusterSet.remove(nodeDescription);
        expireAsync(clusterSet);
    }

    @Nonnull
    @Override
    public Collection<NodeDescription> getCluster(@Nonnull String region, @Nonnull String version) {
        return getClusterSet(region, version).readAll();
    }

    @Nonnull
    @Override
    public List<NodeDescription> getCluster(@Nonnull String region, @Nonnull String version, int offset, int size) {
        RScoredSortedSet<NodeDescription> nodeDescriptions = getClusterSet(region, version);
        return !nodeDescriptions.isEmpty()
                ? Lists.newArrayList(nodeDescriptions.valueRange(offset, offset + size - 1))
                : Collections.emptyList();
    }

    @Nonnull
    @Override
    public Collection<NodeDescription> getClusterByScores(@Nonnull String region, @Nonnull String version, long fromScoreInclude, long toScoreExclude) {
        RScoredSortedSet<NodeDescription> clusterSet = getClusterSet(region, version);
        return clusterSet.valueRange(fromScoreInclude, true, toScoreExclude, false);
    }

    @Override
    public boolean isEmpty(@Nonnull String region, @Nonnull String version) {
        return getClusterSet(region, version).isEmpty();
    }

    @Nonnull
    @Override
    public Optional<Long> getScore(@Nonnull NodeDescription nodeDescription) {
        RScoredSortedSet<NodeDescription> nodeDescriptions = getClusterSet(nodeDescription);
        return Optional.ofNullable(nodeDescriptions.getScore(nodeDescription)).map(Double::longValue);
    }

    @Override
    public void addScore(@Nonnull NodeDescription nodeDescription, long scoreDelta) {
        RScoredSortedSet<NodeDescription> nodeDescriptions = getClusterSet(nodeDescription);
        nodeDescriptions.addScore(nodeDescription, scoreDelta);
        expireAsync(nodeDescriptions);
    }

    private RScoredSortedSet<NodeDescription> getClusterSet(@Nonnull NodeDescription nodeDescription) {
        return getClusterSet(nodeDescription.getRegion(), nodeDescription.getVersion());
    }

    private RScoredSortedSet<NodeDescription> getClusterSet(@Nonnull String region, @Nonnull String version) {
        return redissonClient.getScoredSortedSet(getClusterName(region, version));
    }

    @Override
    public void deductScore(@Nonnull NodeDescription nodeDescription, long scoreDelta) {
        addScore(nodeDescription, -1 * scoreDelta);
    }

    @Override
    public void removeAll(@Nonnull String region, @Nonnull String version) {
        getClusterSet(region, version).unlink();
    }

    private String getClusterName(@Nonnull String region, @Nonnull String version) {
        return String.join(":", NODE_CLUSTER_PREFIX, this.name, region, this.nodeType.name(), version);
    }

    private void expireAsync(RScoredSortedSet<NodeDescription> nodeDescriptions) {
        nodeDescriptions.expireAsync(getTtlInSec(), TimeUnit.SECONDS);
    }

    private long getTtlInSec() {
        return configurationService.get().getLong(NODE_CLUSTER_TTL.getLeft(), NODE_CLUSTER_TTL.getRight());
    }
}
