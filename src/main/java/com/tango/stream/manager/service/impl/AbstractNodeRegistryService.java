package com.tango.stream.manager.service.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.tango.stream.manager.dao.LiveNodeVersionDao;
import com.tango.stream.manager.dao.LiveRegionDao;
import com.tango.stream.manager.dao.NodeDao;
import com.tango.stream.manager.dao.StreamDao;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.service.BalanceManagerService;
import com.tango.stream.manager.service.LocksService;
import com.tango.stream.manager.service.NodeRegistryService;
import com.tango.stream.manager.service.VersionService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.tango.stream.manager.model.Metrics.Counters.KEEP_ALIVE_COUNTER;
import static com.tango.stream.manager.model.Metrics.Counters.KEEP_ALIVE_ERROR;
import static com.tango.stream.manager.model.Metrics.Tags.*;
import static com.tango.stream.manager.model.Metrics.Timers.*;
import static com.tango.stream.manager.utils.NamedLocksHelper.getNodeClusterLockName;

@Slf4j
public abstract class AbstractNodeRegistryService implements NodeRegistryService {
    private static final Pair<String, Long> CLEANUP_PERIOD_SECONDS = Pair.of("node.registry.cleanup.period.seconds", 10L);
    static final Pair<String, Boolean> IS_GC_ENABLED = Pair.of("node.registry.gc.enable", true);
    static final Pair<String, Long> KEEP_ALIVE_WAIT_TIME = Pair.of("node.registry.keepAlive.wait.time.ms", 1000L);
    static final Pair<String, Long> KEEP_ALIVE_LEASE_TIME = Pair.of("node.registry.keepAlive.lease.time.ms", 2000L);
    static final Pair<String, Long> GC_WAIT_TIME = Pair.of("node.registry.gc.wait.time.ms", 3000L);
    static final Pair<String, Long> GC_LEASE_TIME = Pair.of("node.registry.gc.lease.time.ms", 5000L);
    static final Pair<String, Long> TTL_TO_APPLY_COMMAND_MS = Pair.of("node.registry.command.apply.ttl.ms", 5000L);
    static final Pair<String, Long> TTL_TO_SWITCH_COMMAND_MS = Pair.of("node.registry.command.switch.apply.ttl.ms", 5000L);

    private final ScheduledExecutorService cleanUpExecutorService = Executors.newSingleThreadScheduledExecutor(new BasicThreadFactory.Builder()
            .namingPattern("node-cleanup-thread-%d")
            .daemon(true)
            .build());

    @Autowired
    private LocksService locksService;
    @Autowired
    private LiveRegionDao liveRegionDao;
    @Autowired
    private LiveNodeVersionDao liveNodeVersionDao;
    @Autowired
    protected NodeDao nodeDao;
    @Autowired
    protected BalanceManagerService balanceManagerService;
    @Autowired
    protected VersionService versionService;
    @Autowired
    protected ConfigurationService configurationService;
    @Autowired
    protected MeterRegistry meterRegistry;

    protected abstract void processNodeData(@Nonnull NodeData newNodeData, @Nonnull Map<String, StreamData> activeStreams) throws Exception;

    @Nonnull
    protected abstract StreamDao<StreamDescription> getStreamDao();

    @PostConstruct
    protected void init() {
        runGCForNodes();
    }

    @Override
    public void onKeepAlive(@Nonnull KeepAliveRequest request) throws Exception {
        NodeType nodeType = request.getNodeType();
        if (nodeType == NodeType.UNKNOWN) {
            log.warn("Received keepAlive request from UNKNOWN node type: {}", request);
            return;
        }
        String region = request.getRegion();
        if (StringUtils.isBlank(region)) {
            log.warn("Received keepAlive request for UNDEFINED region: {}", request);
            meterRegistry.counter(KEEP_ALIVE_ERROR, Tags.of(NODE_TYPE, nodeType.name(), VERSION, request.getVersion())).increment();
            return;
        }

        request.setVersion(StringUtils.firstNonBlank(request.getVersion(), versionService.getActualVersion(nodeType, region)));
        request.setActiveStreams(Optional.ofNullable(request.getActiveStreams()).orElse(Collections.emptyMap()));
        String version = request.getVersion();

        liveRegionDao.addRegionAsync(nodeType, region);
        liveNodeVersionDao.addVersionAsync(nodeType, version);

        Tags tags = Tags.of(NODE_TYPE, nodeType.name(), REGION, region, VERSION, request.getVersion());
        locksService.doUnderLock(ActionSpec.builder()
                .lockNames(ImmutableList.of(getNodeClusterLockName(region, nodeType, request.getVersion())))
                .action(() -> onKeepAliveBase(request))
                .fallbackAction(() -> onKeepAliveForce(request))
                .waitTimeMs(getKeepAliveWaitTime())
                .leaseTimeMs(getKeepAliveLeaseTime())
                .outsideTimer(meterRegistry.timer(Metrics.Timers.KEEP_ALIVE_OUTSIDE_TIMER, tags))
                .insideLocalTimer(meterRegistry.timer(Metrics.Timers.KEEP_ALIVE_INSIDE_LOCAL_TIMER, tags))
                .insideRemoteTimer(meterRegistry.timer(Metrics.Timers.KEEP_ALIVE_INSIDE_REMOTE_TIMER, tags))
                .build());
    }

    private void onKeepAliveBase(@Nonnull KeepAliveRequest request) throws Exception {
        NodeType nodeType = request.getNodeType();
        NodeData newNodeData = calculateNewNodeData(request);
        NodeDescription nodeDescription = newNodeData.toNodeDescription();
        Map<String, StreamData> activeStreams = request.getActiveStreams();

        processNodeData(newNodeData, activeStreams);
        syncStreamInformation(nodeDescription, activeStreams);
        newNodeData.setLastUpdate(Instant.now().toEpochMilli());
        nodeDao.save(newNodeData);

        Tags tags = Tags.of(NODE_TYPE, nodeType.name(), REGION, request.getRegion(), VERSION, request.getVersion(), NODE_UPDATE_TYPE, "under_lock");
        meterRegistry.counter(KEEP_ALIVE_COUNTER, tags).increment();
        log.info("updated node data successfully under lock: {}", request);
    }

    private void syncStreamInformation(@Nonnull NodeDescription nodeDescription, @Nonnull Map<String, StreamData> activeStreams) {
        Collection<StreamDescription> knownStreams = getStreamDao().findStreams(nodeDescription, activeStreams.keySet());
        List<StreamDescription> updatedKnownStreams = knownStreams.stream()
                .map(streamDescription -> {
                    StreamData streamData = activeStreams.get(streamDescription.getEncryptedStreamKey());
                    if (streamData != null) {
                        streamDescription.setCommandUid(streamData.getCommandUid());
                        streamDescription.setCommandTimestamp(streamData.getCommandTimestamp());
                        streamDescription.setEgressBitrate(streamData.getEgressBitrate());
                        streamDescription.setIngressBitrate(streamData.getIngressBitrate());
                        streamDescription.setLastFragmentSizeInBytes(streamData.getLastFragmentSizeInBytes());
                        streamDescription.setStartTimeToLostConnection(streamData.getStartTimeToLostConnection());
                        streamDescription.setViewersCount(streamData.getViewersCount());
                        return streamDescription;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        getStreamDao().addStreams(nodeDescription, updatedKnownStreams);
    }

    private void onKeepAliveForce(@Nonnull KeepAliveRequest request) {
        NodeType nodeType = request.getNodeType();
        NodeData newNodeData = calculateNewNodeData(request);
        nodeDao.save(newNodeData);
        Tags tags = Tags.of(NODE_TYPE, nodeType.name(), REGION, request.getRegion(), VERSION, request.getVersion(), Metrics.Tags.NODE_UPDATE_TYPE, "force");
        meterRegistry.counter(KEEP_ALIVE_COUNTER, tags).increment();
        log.warn("updated node data successfully in force mode: {}", request);
    }

    @Nonnull
    private NodeData calculateNewNodeData(@Nonnull KeepAliveRequest request) {
        return NodeData.builder()
                .uid(request.getUid())
                .nodeType(request.getNodeType())
                .extIp(request.getExtIp())
                .region(request.getRegion())
                .version(request.getVersion())
                .avgCpuUsage(request.getAvgCpuUsage())
                .medCpuUsage(request.getMedCpuUsage())
                .build();
    }

    @Override
    public void unregister(@Nonnull String uid) {
        NodeData node = nodeDao.getNodeById(uid);
        if (node == null) {
            log.warn("Receive unregister request from unknown node: {}", uid);
            return;
        }
        log.info("Node {} unregistered.", node);
        nodeDao.removeAsync(node); //GS will clean NodeBalanceList
    }

    @SneakyThrows
    void runGCForNodes() {
        if (configurationService.get().getBoolean(IS_GC_ENABLED.getLeft(), IS_GC_ENABLED.getRight())) {
            NodeType nodeType = nodeType();
            for (String region : liveRegionDao.getAllRegions(nodeType)) {
                for (String version : liveNodeVersionDao.getAllVersions(nodeType)) {
                    runGCForSpecificCluster(region, nodeType, version);
                }
            }
        }
        long periodSeconds = configurationService.get().getLong(CLEANUP_PERIOD_SECONDS.getKey(), CLEANUP_PERIOD_SECONDS.getValue());
        cleanUpExecutorService.schedule(this::runGCForNodes, periodSeconds, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    private void runGCForSpecificCluster(@Nonnull String region, @Nonnull NodeType nodeType, @Nonnull String version) throws Exception {
        Tags tags = Tags.of(VERSION, version, NODE_TYPE, nodeType.name(), REGION, region);
        String lockName = getNodeClusterLockName(region, nodeType, version);
        locksService.doUnderLock(ActionSpec.builder()
                .lockNames(ImmutableList.of(lockName))
                .action(() -> runGCForSpecificClusterUnderLock(region, nodeType, version))
                .fallbackAction(() -> runGCFallback(region, nodeType, version, lockName))
                .waitTimeMs(getGCWaitTime())
                .leaseTimeMs(getGCLeaseTime())
                .outsideTimer(meterRegistry.timer(GC_OUTSIDE_TIMER, tags))
                .insideLocalTimer(meterRegistry.timer(GC_INSIDE_LOCAL_TIMER, tags))
                .insideRemoteTimer(meterRegistry.timer(GC_INSIDE_REMOTE_TIMER, tags))
                .build());
    }

    protected abstract void runGCForSpecificClusterUnderLock(@Nonnull String region, @Nonnull NodeType nodeType, @Nonnull String version);

    private void runGCFallback(@Nonnull String region, @Nonnull NodeType nodeType, @Nonnull String version, @Nonnull String lockName) {
        log.warn("GC: Couldn't make GS for nodes because of couldn't acquire a lock=[{}] in timeout nodeType=[{}], region=[{}], version=[{}]",
                lockName, nodeType, region, version);
        Tags tags = Tags.of(VERSION, version, NODE_TYPE, nodeType.name(), REGION, region);
        meterRegistry.counter(Metrics.Counters.NODES_GS_COUNTER_FAIL, tags).increment();
    }

    private long getKeepAliveLeaseTime() {
        return configurationService.get().getLong(KEEP_ALIVE_LEASE_TIME.getLeft(), KEEP_ALIVE_LEASE_TIME.getRight());
    }

    private long getKeepAliveWaitTime() {
        return configurationService.get().getLong(KEEP_ALIVE_WAIT_TIME.getLeft(), KEEP_ALIVE_WAIT_TIME.getRight());
    }

    private long getGCLeaseTime() {
        return configurationService.get().getLong(GC_LEASE_TIME.getLeft(), GC_LEASE_TIME.getRight());
    }

    private long getGCWaitTime() {
        return configurationService.get().getLong(GC_WAIT_TIME.getLeft(), GC_WAIT_TIME.getRight());
    }

    @PreDestroy
    private void shutdown() {
        cleanUpExecutorService.shutdownNow();
    }
}
