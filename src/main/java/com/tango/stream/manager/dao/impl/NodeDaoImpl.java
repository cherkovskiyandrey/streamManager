package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.dao.NodeDao;
import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.service.impl.ConfigurationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Repository;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

@Slf4j
@Repository
@RequiredArgsConstructor
public class NodeDaoImpl implements NodeDao {
    private static final Pair<String, Long> NODE_TTL = Pair.of("node.ttl.sec", 15L);
    private final static String NODE_KEY_PREFIX_BY_UID = "SRT_NODE_BY_UID";
    private final static String NODE_KEY_PREFIX_BY_IP = "SRT_NODE_BY_IP";
    private final RedissonClient redissonClient;
    private final ConfigurationService configurationService;

    @Nullable
    @Override
    public NodeData getNodeById(@Nonnull String uid) {
        return getBucketByUid(uid).get();
    }

    @Override
    public boolean isExist(@Nonnull String uid) {
        return getBucketByUid(uid).isExists();
    }

    @Nullable
    @Override
    public NodeDescription getNodeByExtIp(@Nonnull String extIp) {
        return getBucketByIp(extIp).get();
    }

    @Override
    public void save(@Nonnull NodeData nodeData) {
        getBucketByUid(nodeData.getUid()).set(nodeData, getTtlInSec(), TimeUnit.SECONDS);
        getBucketByIp(nodeData.getExtIp()).set(nodeData.toNodeDescription(), getTtlInSec(), TimeUnit.SECONDS);
    }

    @Override
    public void removeAsync(@Nonnull NodeData nodeData) {
        getBucketByUid(nodeData.getUid()).deleteAsync();
        getBucketByIp(nodeData.getExtIp()).deleteAsync();
    }

    @Override
    public void remove(@Nonnull NodeData nodeData) {
        getBucketByUid(nodeData.getUid()).delete();
        getBucketByIp(nodeData.getExtIp()).delete();
    }

    private RBucket<NodeData> getBucketByUid(String uid) {
        return redissonClient.getBucket(getKeyNameByUid(uid));
    }

    private RBucket<NodeDescription> getBucketByIp(@Nonnull String extIp) {
        return redissonClient.getBucket(getKeyNameByIp(extIp));
    }

    private String getKeyNameByUid(@Nonnull String uid) {
        return String.join(":", NODE_KEY_PREFIX_BY_UID, uid);
    }

    private String getKeyNameByIp(@Nonnull String ip) {
        return String.join(":", NODE_KEY_PREFIX_BY_IP, ip);
    }

    private long getTtlInSec() {
        return configurationService.get().getLong(NODE_TTL.getLeft(), NODE_TTL.getRight());
    }
}
