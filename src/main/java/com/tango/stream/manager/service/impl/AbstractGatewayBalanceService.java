package com.tango.stream.manager.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.tango.stream.manager.conf.DaoConfig;
import com.tango.stream.manager.dao.*;
import com.tango.stream.manager.exceptions.BalanceException;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.service.GatewayBalanceService;
import com.tango.stream.manager.service.LocksService;
import com.tango.stream.manager.service.VersionService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tango.stream.manager.model.Metrics.Tags.*;
import static com.tango.stream.manager.model.Metrics.Timers.*;
import static com.tango.stream.manager.utils.NamedLocksHelper.getNodeClusterLockName;
import static java.lang.String.format;


@Slf4j
@Service
@RequiredArgsConstructor
public abstract class AbstractGatewayBalanceService implements GatewayBalanceService {
    private static final int CLUSTER_BUTCH_SIZE = 10;
    protected final static NodeType GATEWAY_NODE_TYPE = NodeType.GATEWAY;

    protected static final Pair<String, String> DEFAULT_REGION = Pair.of("balancer.gateway.region.toBalance.default", "us-west");
    protected static final Pair<String, Long> BALANCE_LEASE_TIME = Pair.of("balancer.gateway.balance.lease.time.ms", 5000L);
    protected static final Pair<String, Long> BALANCE_WAIT_TIME = Pair.of("balancer.gateway.balance.wait.time.ms", 5000L);
    protected static final Pair<String, Double> UNREACHABLE_THRESHOLD = Pair.of("balancer.gateway.unreachable.reported.by.stream.threshold.percent", 10.D);

    @Autowired
    protected LocksService locksService;
    @Autowired
    protected RedissonClient redissonClient;
    @Autowired
    protected NodeDao nodeDao;
    @Autowired
    @Qualifier(DaoConfig.GW_PENDING_STREAM)
    protected ExpiringStreamDao<String> pendingStreamDao;
    @Autowired
    @Qualifier(DaoConfig.GATEWAY_STREAM)
    protected StreamDao<StreamDescription> streamDao;
    @Autowired
    @Qualifier(DaoConfig.GW_LOST_STREAM)
    protected StreamDao<String> lostStreamDao;
    @Autowired
    @Qualifier(DaoConfig.GATEWAY_VIEWER)
    protected ViewerDao<NodeDescription> viewerDao;
    @Autowired
    private VersionService versionService;
    @Autowired
    protected MeterRegistry meterRegistry;
    @Autowired
    protected ConfigurationService configurationService;

    @Nonnull
    public NodeDescription getAvailableGateway(@Nonnull AvailableGatewayRequest request) throws Exception {
        if (StringUtils.isBlank(request.getRegion())) {
            log.warn("Received getAvailableGateway without region: {}, use default one: {}", request, getDefaultRegion());
        }
        request.setRegion(StringUtils.firstNonBlank(request.getRegion(), getDefaultRegion()));
        String region = request.getRegion();
        String activeVersion = versionService.getActualVersion(GATEWAY_NODE_TYPE, region);
        Tags tags = Tags.of(VERSION, activeVersion, NODE_TYPE, GATEWAY_NODE_TYPE.name(), REGION, region);

        return locksService.doUnderLock(CallSpec.<NodeDescription>builder()
                .lockNames(ImmutableList.of(getNodeClusterLockName(region, GATEWAY_NODE_TYPE, activeVersion)))
                .action(() -> getAvailableGatewayBase(request))
                .fallbackAction(() -> fallbackAvailableGatewayBase(request))
                .waitTimeMs(getBalanceWaitTime())
                .leaseTimeMs(getBalanceLeaseTime())
                .outsideTimer(meterRegistry.timer(BALANCE_OF_AVAILABLE_GATEWAY_OUTSIDE_TIME, tags))
                .insideLocalTimer(meterRegistry.timer(BALANCE_OF_AVAILABLE_GATEWAY_INSIDE_LOCAL_TIME, tags))
                .insideRemoteTimer(meterRegistry.timer(BALANCE_OF_AVAILABLE_GATEWAY_INSIDE_REMOTE_TIME, tags))
                .build()
        );
    }

    @Nonnull
    private NodeDescription getAvailableGatewayBase(@Nonnull AvailableGatewayRequest request) {
        String region = request.getRegion();
        String activeVersion = versionService.getActualVersion(GATEWAY_NODE_TYPE, region);
        NodeClusterDao nodeClusterDao = getClusterDao();
        boolean isEmptyCluster = nodeClusterDao.isEmpty(region, activeVersion);

        if (isEmptyCluster) {
            log.warn("Can't find available nodes type: {}, region: {}, version {}, for: {}", GATEWAY_NODE_TYPE, region, activeVersion, request);
            Tags tags = Tags.of(VERSION, activeVersion, NODE_TYPE, GATEWAY_NODE_TYPE.name(), REGION, region, ERROR_TYPE, "notNodes");
            meterRegistry.counter(Metrics.Counters.NEXT_AVAILABLE_NODE_ERROR, tags).increment();
            return NodeDescription.NONE_NODE;
        }
        Set<String> voidedNodes = handleUnreachableNodes(request);
        NodeDescription lessLoadedNode = lessLoadedNode(nodeClusterDao, region, activeVersion, voidedNodes);
        if (lessLoadedNode == null || !registerStreams(lessLoadedNode, request, nodeClusterDao)) {
            log.warn("All nodes overloaded. Balancer {}, type: {}, region: {}, version {}, for: {}",
                    getName(), GATEWAY_NODE_TYPE, region, activeVersion, request);
            Tags tags = Tags.of(VERSION, activeVersion, NODE_TYPE, GATEWAY_NODE_TYPE.name(), REGION, region, ERROR_TYPE, "overloaded");
            meterRegistry.counter(Metrics.Counters.NEXT_AVAILABLE_NODE_ERROR, tags).increment();
            return NodeDescription.NONE_NODE; //TODO: signal to expand cluster
        }
        Tags tags = Tags.of(VERSION, activeVersion, NODE_TYPE, GATEWAY_NODE_TYPE.name(), REGION, region);
        meterRegistry.counter(Metrics.Counters.NEXT_AVAILABLE_NODE_OK, tags).increment();
        log.info("For request: {}, node is assigned uid: {}, extIp: {},  GATEWAY_NODE_TYPE: {}, region: {}, version: {}",
                request, lessLoadedNode.getNodeUid(), lessLoadedNode.getExtIp(), GATEWAY_NODE_TYPE, region, activeVersion);

        return lessLoadedNode;
    }

    @Nonnull
    private Set<String> handleUnreachableNodes(@Nonnull AvailableGatewayRequest request) {
        Set<String> goneNodes = Sets.newHashSet();
        for (String ip : request.getExcludedGateways()) {
            NodeDescription nodeDescription = nodeDao.getNodeByExtIp(ip);
            if (nodeDescription == null) {
                continue;
            }
            lostStreamDao.addStream(nodeDescription, request.getEncryptedStreamKey());
            goneNodes.add(nodeDescription.getNodeUid());
            log.warn("Stream: {} blacklist nodeData {}", request.getEncryptedStreamKey(), nodeDescription);
        }
        return goneNodes;
    }

    @Nonnull
    private NodeDescription fallbackAvailableGatewayBase(@Nonnull AvailableGatewayRequest request) {
        String region = request.getRegion();
        String activeVersion = versionService.getActualVersion(GATEWAY_NODE_TYPE, region);
        log.error("Couldn't process keep alive type: {}, region: {}, version {}, for: {}", GATEWAY_NODE_TYPE, region, activeVersion, request);
        Tags tags = Tags.of(VERSION, activeVersion, NODE_TYPE, GATEWAY_NODE_TYPE.name(), REGION, region, ERROR_TYPE, "maxAttemptToBalanceFail");
        meterRegistry.counter(Metrics.Counters.NEXT_AVAILABLE_NODE_ERROR, tags).increment();
        return NodeDescription.NONE_NODE; //fallback to RTMP //TODO: expand cluster
    }

    @Nullable
    private NodeDescription lessLoadedNode(@Nonnull NodeClusterDao nodeClusterDao,
                                           @Nonnull String region,
                                           @Nonnull String version,
                                           @Nonnull Set<String> voidedNodes) {
        int curButchPos = 0;
        while (true) {
            List<NodeDescription> sortedGateways = nodeClusterDao.getCluster(region, version, curButchPos, CLUSTER_BUTCH_SIZE).stream()
                    .filter(nd -> !voidedNodes.contains(nd.getNodeUid()))
                    .collect(Collectors.toList());
            for (NodeDescription nodeDescription : sortedGateways) {
                NodeData nodeData = nodeDao.getNodeById(nodeDescription.getNodeUid());
                if (nodeData != null) {
                    boolean potentialUnreachable = isPotentialUnreachable(nodeDescription);
                    if (potentialUnreachable) {
                        log.warn("Potential unreachable node is: {}", nodeData);
                    } else {
                        return nodeDescription;
                    }
                }
                log.warn("Node: {} seems to be gone.", nodeDescription.getNodeUid());
            }
            if (sortedGateways.size() < CLUSTER_BUTCH_SIZE) {
                break;
            }
            curButchPos += CLUSTER_BUTCH_SIZE;
        }
        return null;
    }

    private boolean isPotentialUnreachable(@Nonnull NodeDescription nodeDescription) {
        double lost = lostStreamDao.size(nodeDescription);
        double all = lost + streamDao.size(nodeDescription);
        return all != 0 && (lost / all) >= getUnreachableThreshold();
    }

    private double getUnreachableThreshold() {
        return configurationService.get().getDouble(UNREACHABLE_THRESHOLD.getLeft(), UNREACHABLE_THRESHOLD.getRight()) / 100D;
    }

    @Nonnull
    private String getDefaultRegion() {
        return configurationService.get().getString(DEFAULT_REGION.getLeft(), DEFAULT_REGION.getRight());
    }

    private long getBalanceLeaseTime() {
        return configurationService.get().getLong(BALANCE_LEASE_TIME.getLeft(), BALANCE_LEASE_TIME.getRight());
    }

    private long getBalanceWaitTime() {
        return configurationService.get().getLong(BALANCE_WAIT_TIME.getLeft(), BALANCE_WAIT_TIME.getRight());
    }

    private boolean registerStreams(@Nonnull NodeDescription nodeDescription, @Nonnull AvailableGatewayRequest request, @Nonnull NodeClusterDao nodeClusterDao) {
        long currentScore = nodeClusterDao.getScore(nodeDescription).orElse(0L);
        ScoreResult scoreResult = tryToAddStreams(currentScore, 1);
        if (scoreResult.getReservedClientPlaces() == 0) {
            return false;
        }
        long newTotalScore = currentScore + scoreResult.getNewScoreDelta();
        nodeClusterDao.addNodeToCluster(nodeDescription, newTotalScore);
        streamDao.addStream(nodeDescription, StreamDescription.builder()
                .encryptedStreamKey(request.getEncryptedStreamKey())
                .build());
        pendingStreamDao.addStream(nodeDescription, request.getEncryptedStreamKey());

        return true;
    }

    @Override
    public BookedNode bookClients(@Nonnull String encryptedStreamKey,
                                  @Nonnull NodeDescription gateway,
                                  int gatewayClients) throws BalanceException {
        NodeClusterDao nodeClusterDao = getClusterDao();
        Optional<Long> currentScore = nodeClusterDao.getScore(gateway);
        if (!currentScore.isPresent()) {
            log.error("Couldn't find node score in node=[{}] cluster. Stream=[{}].", gateway, encryptedStreamKey);
            throw new BalanceException(format("Couldn't find node in node=[%s] cluster, stream=[%s].", gateway, encryptedStreamKey));
        }
        long curScore = currentScore.get();
        EdgeOrRelayBalanceServiceImpl.ScoreResult scoreResult = tryToAddClients(curScore, gatewayClients);
        long newScore = curScore + scoreResult.getNewScoreDelta();
        nodeClusterDao.addNodeToCluster(gateway, newScore);
        return BookedNode.builder()
                .nodeDescription(gateway)
                .encryptedStreamId(encryptedStreamKey)
                .bookedClients(scoreResult.getReservedClientPlaces())
                .build();
    }

    @Override
    public void releaseClients(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription gateway, int capacity) {
        getClusterDao().deductScore(gateway, clientsToScore(capacity));
    }

    @Override
    public void releaseStreams(@Nonnull Collection<String> encryptedStreamKeys, @Nonnull NodeDescription gateway) {
        getClusterDao().deductScore(gateway, streamsToScore(encryptedStreamKeys.size()));
        streamDao.removeStreams(gateway, encryptedStreamKeys);
        pendingStreamDao.removeStreams(gateway, encryptedStreamKeys);
        log.info("on gateway=[{}] release streams=[{}]", gateway, encryptedStreamKeys);
    }
}
