package com.tango.stream.manager.service.impl;

import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tango.stream.manager.conf.DaoConfig;
import com.tango.stream.manager.dao.*;
import com.tango.stream.manager.exceptions.BalanceException;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.model.commands.Command;
import com.tango.stream.manager.service.*;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tango.stream.manager.conf.DaoConfig.*;
import static com.tango.stream.manager.utils.NamedLocksHelper.getNodeClusterLockName;
import static java.lang.String.format;
import static java.util.stream.Collectors.*;

/**
 * IMPORTANT: global lock ordering is:
 * 1. stream
 * 2. edge cluster in region
 * 3. relay cluster in region
 * 4. gateway cluster in region
 */
@Service
@Slf4j
public class TopologyManagerServiceImpl implements TopologyManagerService {
    public final static Pair<String, Boolean> IS_BUILD_TOPOLOGY_ENABLED = Pair.of("topology.build.enabled", true);
    public final static Pair<String, Boolean> IS_DIRECT_ACCESS_ENABLED = Pair.of("topology.directAccess", true);

    private static final Pair<String, Long> GATEWAY_CLUSTER_LOCK_LEASE_TIME_MS = Pair.of("topology.gateway.cluster.lock.lease.time.ms", 5000L);
    private static final Pair<String, Long> GATEWAY_CLUSTER_LOCK_WAIT_TIME_MS = Pair.of("topology.gateway.cluster.lock.wait.time.ms", 5000L);
    private static final Pair<String, Long> RELAY_CLUSTER_LOCK_LEASE_TIME_MS = Pair.of("topology.relay.cluster.lock.lease.time.ms", 5000L);
    private static final Pair<String, Long> RELAY_CLUSTER_LOCK_WAIT_TIME_MS = Pair.of("topology.relay.cluster.lock.wait.time.ms", 5000L);
    private static final Pair<String, Long> EDGE_CLUSTER_LOCK_LEASE_TIME_MS = Pair.of("topology.edge.cluster.lock.lease.time.ms", 5000L);
    private static final Pair<String, Long> EDGE_CLUSTER_LOCK_WAIT_TIME_MS = Pair.of("topology.edge.cluster.lock.wait.time.ms", 5000L);

    private static final Pair<String, Integer> TOPOLOGY_MANAGER_CORE_POOL_SIZE = Pair.of("topology.manager.core.pool.size", 50);
    private static final Pair<String, Integer> TOPOLOGY_MANAGER_MAX_POOL_SIZE = Pair.of("topology.manager.core.pool.size", 300);

    private final ConfigurationService configurationService;
    private final ExecutorService asyncTaskExecutorService;

    @Autowired
    private BalanceManagerService balanceManagerService;
    @Autowired
    private BroadcasterStatService broadcasterStatService;
    @Autowired
    private CommandProcessor commandProcessor;
    @Autowired
    private LocksService locksService;
    @Autowired
    private VersionService versionService;
    @Autowired
    @Qualifier(EDGE_STREAM)
    private StreamDao<StreamDescription> edgeStreamDao;
    @Autowired
    @Qualifier(RELAY_STREAM)
    private StreamDao<StreamDescription> relayStreamDao;
    @Autowired
    @Qualifier(DaoConfig.GATEWAY_STREAM)
    private StreamDao<StreamDescription> gatewayStreamDao;
    @Autowired
    @Qualifier(DaoConfig.GW_PENDING_STREAM)
    protected ExpiringStreamDao<String> gatewayPendingStreamDao;
    @Autowired
    @Qualifier(GATEWAY_VIEWER)
    private ViewerDao<NodeDescription> gatewayViewerDao;
    @Autowired
    @Qualifier(RELAY_VIEWER)
    private ViewerDao<NodeDescription> relayViewerDao;
    @Autowired
    private LiveNodeVersionDao liveNodeVersionDao;
    @Autowired
    private LiveRegionDao liveRegionDao;
    @Autowired
    private PendingViewerDao edgePendingViewerDao;
    @Autowired
    @Qualifier(EDGE_ACTIVE_VIEWER)
    private ViewerDao<String> edgeActiveViewerDao;

    public TopologyManagerServiceImpl(ConfigurationService configurationService) {
        this.configurationService = configurationService;
        int corePoolSize = configurationService.get().getInt(TOPOLOGY_MANAGER_CORE_POOL_SIZE.getLeft(), TOPOLOGY_MANAGER_CORE_POOL_SIZE.getRight());
        int maxPoolSize = configurationService.get().getInt(TOPOLOGY_MANAGER_MAX_POOL_SIZE.getLeft(), TOPOLOGY_MANAGER_MAX_POOL_SIZE.getRight());
        asyncTaskExecutorService = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                10L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder()
                        .setNameFormat("topology-%d")
                        .setUncaughtExceptionHandler((Thread t, Throwable e) -> log.error("Uncaught exception on inner topology manager thread=[{}]", t, e))
                        .build());
    }

    @Nonnull
    @Override
    public AvailableEdgeResponse getAvailableEdge(@Nonnull AvailableEdgeRequest request) throws Exception {
        // Can't handle relay crash: try to find the client in EdgePendingViewerDao and in EdgeActiveViewerDao and
        //if success - don't mark edge as unavailable but exclude from search for this viewer, because viewer
        //can switch to other stream and went back to this one before we update state from keepAlive
        if (!isTopologyBuildEnabled()) {
            return AvailableEdgeResponse.none();
        }
        EdgeBalanceService actualEdgeBalancer = balanceManagerService.getActualEdgeBalancer();
        return actualEdgeBalancer.getAvailableEdge(request);
    }

    @Override
    @Nonnull
    public AvailableGatewayResponse getAvailableGateway(@Nonnull AvailableGatewayRequest request) throws Exception {
        GatewayBalanceService actualGatewayBalancer = balanceManagerService.getActualGatewayBalancer();
        NodeDescription newGateway = actualGatewayBalancer.getAvailableGateway(request);
        if (newGateway.isNone()) {
            return AvailableGatewayResponse.builder()
                    .region(newGateway.getRegion())
                    .externalIP(newGateway.getExtIp())
                    .build();
        }

        buildConnectionGraphs(newGateway, ImmutableList.of(request));
        return AvailableGatewayResponse.builder()
                .region(newGateway.getRegion())
                .externalIP(newGateway.getExtIp())
                .build();
    }

    @Override
    public void buildConnectionGraphs(@Nonnull NodeDescription gateway, @Nonnull List<AvailableGatewayRequest> requests) throws Exception {
        for (AvailableGatewayRequest request : requests) {
            String encryptedStreamKey = request.getEncryptedStreamKey();
            Collection<NodeDescription> gatewaysWithActiveStream = gatewayStreamDao.getNodesByStream(encryptedStreamKey);
            Set<NodeDescription> oldGateways = gatewaysWithActiveStream.stream()
                    .filter(nd -> !gateway.equals(nd))
                    .collect(toSet());

            if (!oldGateways.isEmpty()) {
                redirectViewers(encryptedStreamKey, gateway, oldGateways);
            } else if (isTopologyBuildEnabled()) {
                ViewersCapacity viewersCapacity = broadcasterStatService.getInitialViewersCapacity(request.getStreamerAccountId());
                buildRoutesForViewers(encryptedStreamKey, gateway, viewersCapacity, ImmutableSet.of());
            }
        }
    }

    @Override
    public void buildConnectionGraphsAsync(@Nonnull NodeDescription gateway, @Nonnull List<AvailableGatewayRequest> streams) {
        asyncTaskExecutorService.submit(() -> {
            try {
                buildConnectionGraphs(gateway, streams);
            } catch (Exception e) {
                log.error("Couldn't build connection graph async for gateway=[{}] and requests=[{}]", gateway, streams, e);
            }
        });
    }

    private void buildRoutesForViewers(@Nonnull String encryptedStreamKey,
                                       @Nonnull NodeDescription hostedGateway,
                                       @Nonnull ViewersCapacity viewersCapacity,
                                       @Nonnull Set<NodeDescription> excludeNodes) throws Exception {
        for (String region : viewersCapacity.getRegionToViewers().keySet()) {
            int viewers = viewersCapacity.getRegionToViewers().get(region);
            String lockName = getNodeClusterLockName(region, NodeType.EDGE, versionService.getActualVersion(NodeType.EDGE, region));

            locksService.doUnderLock(CallSpec.builder()
                    .lockNames(ImmutableList.of(lockName))
                    .action(() -> buildRouteUnderEdgeLock(encryptedStreamKey, region, hostedGateway, viewers, excludeNodes))
                    .fallbackAction(() -> buildRouteFallback(encryptedStreamKey, lockName))
                    .leaseTimeMs(getEdgeClusterLockLeaseTime())
                    .waitTimeMs(getEdgeClusterLockWaitTime())
                    .build());
        }
    }

    @Nonnull
    private BalanceResult buildRouteUnderEdgeLock(@Nonnull String encryptedStreamKey,
                                                  @Nonnull String region,
                                                  @Nonnull NodeDescription hostedGateway,
                                                  int viewers,
                                                  @Nonnull Set<NodeDescription> excludeNodes) throws Exception {
        EdgeOrRelayBalanceService actualEdgeBalancer = balanceManagerService.getActualEdgeBalancer();

        BookedNodes chosenEdges = actualEdgeBalancer.chooseNodesInRegion(encryptedStreamKey, region, viewers, excludeNodes);
        if (chosenEdges.getBookedNodes().isEmpty() && chosenEdges.getLinkedNodes().isEmpty()) {
            log.error("Couldn't book edges: stream=[{}], region=[{}], for viewers=[{}]", encryptedStreamKey, region, viewers);
            return BalanceResult.none();
        }

        String lockName = getNodeClusterLockName(region, NodeType.RELAY, versionService.getActualVersion(NodeType.RELAY, region));
        return locksService.doUnderLock(CallSpec.<BalanceResult>builder()
                .lockNames(ImmutableList.of(lockName))
                .action(() -> buildRouteUnderRelayLock(encryptedStreamKey, region, hostedGateway, chosenEdges, excludeNodes))
                .fallbackAction(() -> buildRouteFallback(encryptedStreamKey, lockName))
                .leaseTimeMs(getRelayClusterLockLeaseTime())
                .waitTimeMs(getRelayClusterLockWaitTime())
                .build());
    }

    @Nonnull
    private BalanceResult buildRouteUnderRelayLock(@Nonnull String encryptedStreamKey,
                                                   @Nonnull String region,
                                                   @Nonnull NodeDescription hostedGateway,
                                                   @Nonnull BookedNodes chosenEdges,
                                                   @Nonnull Set<NodeDescription> excludeNodes) throws Exception {
        EdgeOrRelayBalanceService actualRelayBalancer = balanceManagerService.getActualRelayBalancer();
        int onlyNewEdges = chosenEdges.getBookedNodes().size();
        BookedNodes chosenRelays = actualRelayBalancer.chooseNodesInRegion(encryptedStreamKey, region, onlyNewEdges, excludeNodes);

        String lockName = getNodeClusterLockName(hostedGateway.getRegion(), NodeType.GATEWAY, hostedGateway.getVersion());
        return locksService.doUnderLock(CallSpec.<BalanceResult>builder()
                .lockNames(ImmutableList.of(lockName))
                .action(() -> buildRouteUnderGatewayLock(encryptedStreamKey, region, hostedGateway, chosenEdges, chosenRelays))
                .fallbackAction(() -> buildRouteFallback(encryptedStreamKey, lockName))
                .leaseTimeMs(getEdgeClusterLockLeaseTime())
                .waitTimeMs(getEdgeClusterLockWaitTime())
                .build());
    }

    @Nonnull
    private BalanceResult buildRouteUnderGatewayLock(@Nonnull String encryptedStreamKey,
                                                     @Nonnull String viewerRegion,
                                                     @Nonnull NodeDescription hostedGateway,
                                                     @Nonnull BookedNodes chosenEdges,
                                                     @Nonnull BookedNodes chosenRelays) throws BalanceException {
        EdgeOrRelayBalanceService actualEdgeBalancer = balanceManagerService.getActualEdgeBalancer();
        EdgeOrRelayBalanceService actualRelayBalancer = balanceManagerService.getActualRelayBalancer();
        GatewayBalanceService actualGatewayBalancer = balanceManagerService.getActualGatewayBalancer();

        int edges = chosenEdges.getBookedNodes().size();
        boolean emptyRelays = chosenRelays.getBookedNodes().isEmpty();
        BookedNodes chosenRelaysOrEdges;
        if (emptyRelays) {
            if (isDirectAccessEnabled()) {
                log.warn("Couldn't book relays: stream=[{}], region=[{}], for viewers=[{}]. But direct access enabled.", encryptedStreamKey, viewerRegion, edges);
                chosenRelaysOrEdges = chosenEdges;
            } else {
                log.error("Couldn't book relays: stream=[{}], region=[{}], for viewers=[{}]. Direct access disabled. Skip region.", encryptedStreamKey, viewerRegion, edges);
                return BalanceResult.none();
            }
        } else {
            chosenRelaysOrEdges = chosenRelays;
        }

        int gatewayClients = chosenRelaysOrEdges.getBookedNodes().size();
        BookedNode bookedGateway = actualGatewayBalancer.bookClients(encryptedStreamKey, hostedGateway, gatewayClients);
        BookedNodes bookedGateways = BookedNodes.builder()
                .region(viewerRegion)
                .bookedNodes(ImmutableList.of(bookedGateway))
                .build();

        if (emptyRelays) {
            BookedNodes bookedEdges = actualEdgeBalancer.bookNodes(chosenEdges, bookedGateways);
            commandProcessor.executePreserveOrderAsync(bookedEdges.getCommands());

            return BalanceResult.builder()
                    .chosenEdges(chosenEdges)
                    .bookedEdges(bookedEdges)
                    .chosenRelays(chosenRelays)
                    .build();
        }
        BookedNodes bookedRelays = actualRelayBalancer.bookNodes(chosenRelays, bookedGateways);
        BookedNodes bookedEdges = actualEdgeBalancer.bookNodes(chosenEdges, bookedRelays);

        commandProcessor.executePreserveOrderAsync(bookedRelays.getCommands());
        commandProcessor.executePreserveOrderAsync(bookedEdges.getCommands());

        return BalanceResult.builder()
                .chosenEdges(chosenEdges)
                .bookedEdges(bookedEdges)
                .chosenRelays(chosenRelays)
                .bookedRelays(bookedRelays)
                .build();
    }

    @Nonnull
    private BalanceResult buildRouteFallback(@Nonnull String encryptedStreamKey,
                                             @Nonnull String lockName) {
        log.error("Couldn't get lock=[{}] to make a route for stream=[{}]", lockName, encryptedStreamKey);
        //todo: metric
        return BalanceResult.none();
    }

    private void redirectViewers(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription newGateway, @Nonnull Set<NodeDescription> oldGateways) throws Exception {
        for (NodeDescription oldGateway : oldGateways) {
            Map<String, List<NodeDescription>> allViewers = gatewayViewerDao.getNodeAllViewers(oldGateway, encryptedStreamKey).stream()
                    .collect(groupingBy(NodeDescription::getRegion, Collectors.mapping(Function.identity(), toList())));

            for (String region : allViewers.keySet()) {
                redirectViewersInRegion(encryptedStreamKey, newGateway, allViewers.get(region), region);
            }

            balanceManagerService.getActualGatewayBalancer().releaseClients(encryptedStreamKey, oldGateway, allViewers.size());
            balanceManagerService.getActualGatewayBalancer().releaseStreams(Collections.singletonList(encryptedStreamKey), oldGateway);
        }
    }

    private void redirectViewersInRegion(@Nonnull String encryptedStreamKey,
                                         @Nonnull NodeDescription newGateway,
                                         @Nonnull List<NodeDescription> gatewayViewers,
                                         @Nonnull String region) throws Exception {
        List<NodeDescription> edgeClients = gatewayViewers.stream().filter(nvd -> nvd.getNodeType() == NodeType.EDGE).collect(toList());
        List<NodeDescription> relayClients = gatewayViewers.stream().filter(nvd -> nvd.getNodeType() == NodeType.RELAY).collect(toList());

        if (!relayClients.isEmpty()) {
            Map<String, List<NodeDescription>> relaysByVersions = relayClients.stream().collect(groupingBy(NodeDescription::getVersion));
            for (String version : relaysByVersions.keySet()) {
                String relayLockName = getNodeClusterLockName(region, NodeType.RELAY, version);
                locksService.doUnderLock(ActionSpec.builder()
                        .lockNames(ImmutableList.of(relayLockName))
                        .action(() -> redirectNodes(encryptedStreamKey, newGateway, relayClients, balanceManagerService.getActualRelayBalancer(), relayStreamDao, region))
                        .leaseTimeMs(getEdgeClusterLockLeaseTime())
                        .waitTimeMs(getEdgeClusterLockWaitTime())
                        .build());
            }
        }

        if (!edgeClients.isEmpty()) {
            Map<String, List<NodeDescription>> edgesByVersions = edgeClients.stream().collect(groupingBy(NodeDescription::getVersion));
            for (String version : edgesByVersions.keySet()) {
                String edgeLockName = getNodeClusterLockName(region, NodeType.EDGE, version);
                locksService.doUnderLock(ActionSpec.builder()
                        .lockNames(ImmutableList.of(edgeLockName))
                        .action(() -> redirectNodes(encryptedStreamKey, newGateway, edgeClients, balanceManagerService.getActualEdgeBalancer(), edgeStreamDao, region))
                        .leaseTimeMs(getEdgeClusterLockLeaseTime())
                        .waitTimeMs(getEdgeClusterLockWaitTime())
                        .build());
            }
        }
    }

    private void redirectNodes(@Nonnull String encryptedStreamKey,
                               @Nonnull NodeDescription newGateway,
                               @Nonnull List<NodeDescription> clients,
                               @Nonnull EdgeOrRelayBalanceService clientBalancer,
                               @Nonnull StreamDao<StreamDescription> streamDao,
                               @Nonnull String region) throws Exception {
        String gatewayLockName = getNodeClusterLockName(region, NodeType.GATEWAY, newGateway.getVersion());
        locksService.doUnderLock(ActionSpec.builder()
                .lockNames(ImmutableList.of(gatewayLockName))
                .action(() -> redirectNodesUnderGatewayLock(encryptedStreamKey, newGateway, clients, clientBalancer, streamDao, region))
                .leaseTimeMs(getGatewayClusterLockLeaseTime())
                .waitTimeMs(getGatewayClusterLockWaitTime())
                .build());
    }

    private void redirectNodesUnderGatewayLock(@Nonnull String encryptedStreamKey,
                                               @Nonnull NodeDescription newGateway,
                                               @Nonnull List<NodeDescription> clients,
                                               @Nonnull EdgeOrRelayBalanceService clientBalancer,
                                               @Nonnull StreamDao<StreamDescription> streamDao,
                                               @Nonnull String region) throws BalanceException {
        GatewayBalanceService actualGatewayBalancer = balanceManagerService.getActualGatewayBalancer();

        BookedNode bookedGateway = actualGatewayBalancer.bookClients(encryptedStreamKey, newGateway, clients.size());
        BookedNodes bookedGateways = BookedNodes.builder()
                .region(region)
                .bookedNodes(ImmutableList.of(bookedGateway))
                .build();

        BookedNodes chosenClientNodes = toBookedNodes(clients, encryptedStreamKey, newGateway, region, streamDao);
        BookedNodes bookedClientNodes = clientBalancer.bookNodes(chosenClientNodes, bookedGateways);
        List<BookedNode> unlinkedClientNodes = Lists.newArrayList(Sets.difference(Sets.newHashSet(chosenClientNodes.getBookedNodes()), Sets.newHashSet(bookedClientNodes.getBookedNodes())));
        if (!unlinkedClientNodes.isEmpty()) {
            log.warn("redirectEdgesUnderLock: stream=[{}], newGateway=[{}], couldn't recovery nodes=[{}], allNodes=[{}], insufficient resources",
                    encryptedStreamKey, newGateway, unlinkedClientNodes, chosenClientNodes.getBookedNodes());
            unlinkedClientNodes.forEach(bookedNode -> destroyConnectionGraph(bookedNode.getNodeDescription(), ImmutableList.of(encryptedStreamKey)));
            //todo: notify resourceManager
        }
    }

    @Override
    public void rebuildConnectionGraph(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription voidedNode) throws Exception {
        if (voidedNode.getNodeType() != NodeType.EDGE && voidedNode.getNodeType() == NodeType.RELAY) {
            throw new IllegalArgumentException(format("Unsupported method rebuildConnectionGraph for node=[%s] and stream=[%s]", voidedNode, encryptedStreamKey));
        }

        String region = voidedNode.getRegion();
        String edgesClusterLockName = getNodeClusterLockName(region, NodeType.EDGE, versionService.getActualVersion(NodeType.EDGE, region));
        locksService.doUnderLock(ActionSpec.builder()
                .lockNames(ImmutableList.of(edgesClusterLockName))
                .action(() -> {
                    if (voidedNode.getNodeType() == NodeType.EDGE) {
                        rebuildConnectionGraphExcludeEdge(encryptedStreamKey, voidedNode);
                        return;
                    }
                    String relayClusterLockName = getNodeClusterLockName(region, NodeType.RELAY, versionService.getActualVersion(NodeType.RELAY, region));
                    locksService.doUnderLock(ActionSpec.builder()
                            .lockNames(ImmutableList.of(relayClusterLockName))
                            .action(() -> rebuildConnectionGraphExcludeRelay(encryptedStreamKey, voidedNode))
                            .leaseTimeMs(getRelayClusterLockLeaseTime())
                            .waitTimeMs(getRelayClusterLockWaitTime())
                            .build());
                })
                .leaseTimeMs(getEdgeClusterLockLeaseTime())
                .waitTimeMs(getEdgeClusterLockWaitTime())
                .build());
    }

    @Override
    public void rebuildConnectionGraphAsync(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription voidedNode) {
        asyncTaskExecutorService.submit(() -> {
            try {
                rebuildConnectionGraph(encryptedStreamKey, voidedNode);
            } catch (Exception e) {
                log.error("Couldn't rebuild connection graph for stream=[{}] and voided node=[{}]", encryptedStreamKey, voidedNode);
            }
        });
    }

    private void rebuildConnectionGraphExcludeEdge(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription voidedNode) throws Exception {
        String region = voidedNode.getRegion();
        Set<NodeDescription> voidedNodes = ImmutableSet.of(voidedNode);
        Optional<StreamDescription> streamData = edgeStreamDao.findStream(voidedNode, encryptedStreamKey);
        if (!streamData.isPresent()) {
            log.warn("rebuildConnectionGraph: couldn't find stream=[{}] on node=[{}]", encryptedStreamKey, voidedNode);
            return;
        }
        StreamDescription stream = streamData.get();
        int viewerCapacity = stream.getMaxAvailableClients();

        NodeDescription hostedGateway = excludeNodeFromGraphUpwards(encryptedStreamKey, voidedNode);
        if (hostedGateway == null) {
            log.warn("rebuildConnectionGraph: stream=[{}] couldn't find path to gateway from=[{}]. Couldn't recovery capacity=[{}]",
                    encryptedStreamKey, voidedNode, viewerCapacity);
            return;
        }
        BalanceResult balanceResult = buildRouteUnderEdgeLock(
                encryptedStreamKey,
                region,
                hostedGateway,
                viewerCapacity,
                voidedNodes
        );
        BookedNodes bookedEdges = balanceResult.getBookedEdges();
        if (bookedEdges == null || (bookedEdges.getBookedNodes().isEmpty() && bookedEdges.getLinkedNodes().isEmpty())) {
            log.warn("rebuildConnectionGraph: stream=[{}], node=[{}], couldn't recovery capacity=[{}], insufficient resources",
                    encryptedStreamKey, voidedNode, viewerCapacity);
            return;
        }
        int bookedCapacity = Stream.concat(bookedEdges.getBookedNodes().stream(), bookedEdges.getLinkedNodes().stream()).mapToInt(BookedNode::getBookedClients).sum();
        if (bookedCapacity < viewerCapacity) {
            log.warn("rebuildConnectionGraph: stream=[{}], node=[{}], couldn't recovery capacity=[{}], allCapacity=[{}], insufficient resources",
                    encryptedStreamKey, voidedNode, (viewerCapacity - bookedCapacity), viewerCapacity);
            //todo: notify resourceManager
        }
        //Don't try to redirect active viewers from voided edge to other active edges, they will recover themself.
        //1 - crash node -> clients go to recovery in general before streammanager detect the crash
        //2 - long losing connection -> streammanager delete stream from node, later when keepAlive arrived registerService destroy unknown streams - it will lead to stop stream
        //  on node and destroy srt connection with clients. As result clients go to recovery.
    }

    private void rebuildConnectionGraphExcludeRelay(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription voidedNode) throws Exception {
        String region = voidedNode.getRegion();
        Set<NodeDescription> voidedNodes = ImmutableSet.of(voidedNode);
        Optional<StreamDescription> streamData = relayStreamDao.findStream(voidedNode, encryptedStreamKey);
        if (!streamData.isPresent()) {
            log.warn("rebuildConnectionGraph: couldn't find stream=[{}] on node=[{}]", encryptedStreamKey, voidedNode);
            return;
        }
        Set<NodeDescription> relayViewersToRecovery = relayViewerDao.getNodeAllViewers(voidedNode, encryptedStreamKey);
        BookedNodes chosenEdges = toBookedNodes(Lists.newArrayList(relayViewersToRecovery), encryptedStreamKey, voidedNode, region, edgeStreamDao);
        NodeDescription hostedGateway = excludeNodeFromGraphUpwards(encryptedStreamKey, voidedNode);
        if (hostedGateway == null) {
            log.warn("rebuildConnectionGraph: stream=[{}] couldn't find path to gateway from=[{}]. Destroy graph for edges=[{}]",
                    encryptedStreamKey, voidedNode, relayViewersToRecovery);
            relayViewersToRecovery.forEach(edgeViewer -> destroyConnectionGraph(edgeViewer, ImmutableList.of(encryptedStreamKey)));
            return;
        }
        BalanceResult balanceResult = buildRouteUnderRelayLock(
                encryptedStreamKey,
                region,
                hostedGateway,
                chosenEdges,
                voidedNodes
        );
        BookedNodes bookedEdges = balanceResult.getBookedEdges();
        if (bookedEdges == null) {
            log.warn("rebuildConnectionGraph: stream=[{}], node=[{}], couldn't recovery all edges=[{}], insufficient resources",
                    encryptedStreamKey, voidedNode, relayViewersToRecovery);
            relayViewersToRecovery.forEach(edgeViewer -> destroyConnectionGraph(edgeViewer, ImmutableList.of(encryptedStreamKey)));
            return;
        }
        List<BookedNode> unlinkedEdges = Lists.newArrayList(Sets.difference(Sets.newHashSet(chosenEdges.getBookedNodes()), Sets.newHashSet(bookedEdges.getBookedNodes())));
        if (!unlinkedEdges.isEmpty()) {
            log.warn("rebuildConnectionGraph: stream=[{}], node=[{}], couldn't recovery edges=[{}], allEdges=[{}], insufficient resources",
                    encryptedStreamKey, voidedNode, unlinkedEdges, relayViewersToRecovery);
            unlinkedEdges.forEach(bookedNode -> destroyConnectionGraph(bookedNode.getNodeDescription(), ImmutableList.of(encryptedStreamKey)));
            //todo: notify resourceManager
        }
        //Don't try to redirect active viewers from unlinkedEdges to other active edges, they will recover themself.
    }

    @Nonnull
    private BookedNodes toBookedNodes(@Nonnull List<NodeDescription> viewers,
                                      @Nonnull String encryptedStreamKey,
                                      @Nonnull NodeDescription hostNode,
                                      @Nonnull String region,
                                      @Nonnull StreamDao<StreamDescription> streamDao) {
        Map<NodeDescription, Integer> maxViewersOnEdge = Maps.newHashMap();
        for (NodeDescription viewer : viewers) {
            Optional<StreamDescription> stream = streamDao.findStream(viewer, encryptedStreamKey);
            if (!stream.isPresent()) {
                log.warn("Inconsistency is detected: stream=[{}], hostNode=[{}], viewer=[{}] doesn't has relevant streamData", encryptedStreamKey, hostNode, viewer);
            } else {
                maxViewersOnEdge.put(viewer, stream.get().getMaxAvailableClients());
            }
        }
        return BookedNodes.builder()
                .region(region)
                .bookedNodes(
                        viewers.stream()
                                .map(data -> BookedNode.builder()
                                        .encryptedStreamId(encryptedStreamKey)
                                        .nodeDescription(data)
                                        .bookedClients(maxViewersOnEdge.getOrDefault(data, 0))
                                        .build())
                                .collect(toList())
                )
                .build();
    }

    @Override
    public void rebuildConnectionGraph(@Nonnull List<NodeDescription> goneNodes) {
        goneNodes.forEach(nodeDescription -> {
            if (nodeDescription.getNodeType() != NodeType.EDGE && nodeDescription.getNodeType() != NodeType.RELAY) {
                log.error("Unsupported method rebuildConnectionGraph for node=[{}]", nodeDescription);
                return;
            }

            String region = nodeDescription.getRegion();
            List<String> edgesAllClusterLockNames = liveNodeVersionDao.getAllVersions(NodeType.EDGE).stream()
                    .map(version -> getNodeClusterLockName(region, NodeType.EDGE, version))
                    .sorted()
                    .collect(Collectors.toList());
            ImmutableList.Builder<String> locksPreserveOrder = ImmutableList.<String>builder().addAll(edgesAllClusterLockNames);
            if (nodeDescription.getNodeType() == NodeType.RELAY) {
                String relayClusterLockName = getNodeClusterLockName(region, NodeType.RELAY, nodeDescription.getVersion());
                locksPreserveOrder.add(relayClusterLockName);
            }

            try {
                locksService.doUnderLock(ActionSpec.builder()
                        .lockNames(locksPreserveOrder.build())
                        .action(() -> {
                            StreamDao<StreamDescription> activeStreamDao = getStreamDao(nodeDescription.getNodeType());
                            if (nodeDescription.getNodeType() == NodeType.EDGE) {
                                Collection<StreamDescription> allStreams = activeStreamDao.getAllStreams(nodeDescription);
                                allStreams.forEach(stream -> {
                                    try {
                                        rebuildConnectionGraphExcludeEdge(stream.getEncryptedStreamKey(), nodeDescription);
                                    } catch (Exception e) {
                                        log.error("Couldn't rebuild graph for node=[{}] and stream=[{}]", nodeDescription, stream.getEncryptedStreamKey());
                                    }
                                });
                                return;
                            }

                            Collection<StreamDescription> allStreams = activeStreamDao.getAllStreams(nodeDescription);
                            allStreams.forEach(stream -> {
                                try {
                                    rebuildConnectionGraphExcludeRelay(stream.getEncryptedStreamKey(), nodeDescription);
                                } catch (Exception e) {
                                    log.error("Couldn't rebuild graph for node=[{}] and stream=[{}]", nodeDescription, stream.getEncryptedStreamKey());
                                }
                            });
                        })
                        .leaseTimeMs(getEdgeClusterLockLeaseTime())
                        .waitTimeMs(getEdgeClusterLockWaitTime())
                        .build());
            } catch (Exception e) {
                log.error("Couldn't rebuild graph for node=[{}]", nodeDescription);
            }
        });
    }

    @Override
    public void rebuildConnectionGraphAsync(@Nonnull List<NodeDescription> goneNodes) {
        asyncTaskExecutorService.submit(() -> rebuildConnectionGraph(goneNodes));
    }

    /**
     * Exclude node from graph, sequentially free resource from passed node to gateway.
     * Return gateway if available.
     * TODO: write a batch version
     *
     * @param encryptedStreamKey
     * @param node
     * @return
     */
    @Nullable
    private NodeDescription excludeNodeFromGraphUpwards(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription node) throws Exception {
        Multimap<NodeDescription, Command> commands = MultimapBuilder.hashKeys().linkedListValues().build();
        NodeDescription result;
        if (node.getNodeType() == NodeType.EDGE) {
            result = excludeEdgeFromGraphUpwards(encryptedStreamKey, node, commands);
        } else if (node.getNodeType() == NodeType.RELAY) {
            result = excludeRelayFromGraphUpwards(encryptedStreamKey, node, commands);
        } else {
            throw new IllegalArgumentException(format("Unsupported method excludeNodeFromGraphWithoutClients for node=[%s] and stream=[%s]", node, encryptedStreamKey));
        }
        commandProcessor.executePreserveOrderAsync(commands);
        return result;
    }

    @Nullable
    private NodeDescription excludeEdgeFromGraphUpwards(@Nonnull String encryptedStreamKey,
                                                        @Nonnull NodeDescription edge,
                                                        @Nonnull Multimap<NodeDescription, Command> commands) throws Exception {
        String region = edge.getRegion();
        String version = edge.getVersion();
        String edgesClusterLockName = getNodeClusterLockName(region, NodeType.EDGE, version);
        return locksService.doUnderLock(CallSpec.<NodeDescription>builder()
                .lockNames(ImmutableList.of(edgesClusterLockName))
                .action(() -> {
                    EdgeBalanceService actualEdgeBalancer = balanceManagerService.getActualEdgeBalancer();
                    ReleaseClients releaseEdge = actualEdgeBalancer.releaseStream(encryptedStreamKey, edge);
                    commands.putAll(releaseEdge.getNodeDescription(), releaseEdge.getCommands());
                    if (releaseEdge.getParentNode() == null) {
                        log.warn("parent node isn't exists for node=[{}], stream=[{}]", edge, encryptedStreamKey);
                        return null;
                    }
                    return releaseEdgeClient(releaseEdge.getEncryptedStreamKey(), releaseEdge.getParentNode(), releaseEdge.getNodeDescription(), commands);
                })
                .leaseTimeMs(getEdgeClusterLockLeaseTime())
                .waitTimeMs(getEdgeClusterLockWaitTime())
                .build());
    }

    @Nullable
    private NodeDescription excludeRelayFromGraphUpwards(@Nonnull String encryptedStreamKey,
                                                         @Nonnull NodeDescription relay,
                                                         @Nonnull Multimap<NodeDescription, Command> commands) throws Exception {
        String region = relay.getRegion();
        String relayClusterLockName = getNodeClusterLockName(region, NodeType.RELAY, relay.getVersion());

        return locksService.doUnderLock(CallSpec.<NodeDescription>builder()
                .lockNames(ImmutableList.of(relayClusterLockName))
                .action(() -> {
                    RelayBalanceService actualRelayBalancer = balanceManagerService.getActualRelayBalancer();
                    ReleaseClients releaseRelay = actualRelayBalancer.releaseStream(encryptedStreamKey, relay);
                    commands.putAll(releaseRelay.getNodeDescription(), releaseRelay.getCommands());

                    NodeDescription gateway = releaseRelay.getParentNode();
                    if (gateway == null) {
                        log.warn("parent node isn't exists for node=[{}], stream=[{}]", relay, encryptedStreamKey);
                        return null;
                    }
                    if (releaseRelay.isStreamReleased()) {
                        releaseClientOnGateway(encryptedStreamKey, gateway);
                    }
                    return gateway;
                })
                .leaseTimeMs(getRelayClusterLockLeaseTime())
                .waitTimeMs(getRelayClusterLockWaitTime())
                .build());
    }

    @Nullable
    private NodeDescription releaseEdgeClient(@Nonnull String encryptedStreamKey,
                                              @Nonnull NodeDescription parent,
                                              @Nonnull NodeDescription edge,
                                              @Nonnull Multimap<NodeDescription, Command> commands) throws Exception {
        if (parent.getNodeType() == NodeType.RELAY) {
            return releaseEdgeClientOnRelay(encryptedStreamKey, parent, edge, commands);
        } else if (parent.getNodeType() == NodeType.GATEWAY) {
            return releaseClientOnGateway(encryptedStreamKey, parent);
        } else {
            throw new IllegalArgumentException(format("Unsupported method releaseEdgeClient for parent=[%s] and stream=[%s]", parent, encryptedStreamKey));
        }
    }

    @Nullable
    private NodeDescription releaseEdgeClientOnRelay(@Nonnull String encryptedStreamKey,
                                                     @Nonnull NodeDescription relay,
                                                     @Nonnull NodeDescription edge,
                                                     @Nonnull Multimap<NodeDescription, Command> commands) throws Exception {
        String region = edge.getRegion();
        RelayBalanceService actualRelayBalancer = balanceManagerService.getActualRelayBalancer();
        String relayClusterLockName = getNodeClusterLockName(region, NodeType.RELAY, relay.getVersion());

        return locksService.doUnderLock(CallSpec.<NodeDescription>builder()
                .lockNames(ImmutableList.of(relayClusterLockName))
                .action(() -> {
                            ReleaseClients releaseRelay = actualRelayBalancer.releaseClientCapacity(encryptedStreamKey, relay, 1);
                            commands.putAll(releaseRelay.getNodeDescription(), releaseRelay.getCommands());

                            NodeDescription gateway = releaseRelay.getParentNode();
                            if (gateway == null) {
                                log.warn("parent node isn't exists for node=[{}], stream=[{}]", relay, encryptedStreamKey);
                                return null;
                            }
                            if (releaseRelay.isStreamReleased()) {
                                releaseClientOnGateway(encryptedStreamKey, gateway);
                            }
                            return gateway;
                        }
                )
                .leaseTimeMs(getRelayClusterLockLeaseTime())
                .waitTimeMs(getRelayClusterLockWaitTime())
                .build());
    }

    @Nonnull
    private NodeDescription releaseClientOnGateway(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription gateway) throws Exception {
        String lockName = getNodeClusterLockName(gateway.getRegion(), NodeType.GATEWAY, gateway.getVersion());
        locksService.doUnderLock(ActionSpec.builder()
                .lockNames(ImmutableList.of(lockName))
                .action(() -> balanceManagerService.getActualGatewayBalancer().releaseClients(encryptedStreamKey, gateway, 1))
                .leaseTimeMs(getGatewayClusterLockLeaseTime())
                .waitTimeMs(getGatewayClusterLockWaitTime())
                .build());
        return gateway;
    }

    @Nonnull
    private StreamDao<StreamDescription> getStreamDao(@Nonnull NodeType nodeType) {
        switch (nodeType) {
            case RELAY:
                return relayStreamDao;
            case EDGE:
                return edgeStreamDao;
            default:
                throw new IllegalArgumentException("There isn't stream dao for type: " + nodeType);
        }
    }

    @Override
    public void destroyConnectionGraph(@Nonnull NodeDescription node, @Nonnull Collection<String> streams) {
        if (node.getNodeType() == NodeType.EDGE) {
            destroyConnectionGraphFromEdge(node, streams);
        } else if (node.getNodeType() == NodeType.RELAY) {
            destroyConnectionGraphFromRelay(node, streams);
        } else if (node.getNodeType() == NodeType.GATEWAY) {
            destroyConnectionGraphFromGateway(node, streams);
        }
    }

    private void destroyConnectionGraphFromGateway(@Nonnull NodeDescription gateway, @Nonnull Collection<String> streams) {
        Set<String> streamSet = Sets.newHashSet(streams);

        ImmutableList<String> locksPreserveOrder = ImmutableList.<String>builder()
                .addAll(getAllLocks(NodeType.EDGE))
                .addAll(getAllLocks(NodeType.RELAY))
                .add(getNodeClusterLockName(gateway.getRegion(), NodeType.GATEWAY, gateway.getVersion()))
                .build();
        try {
            locksService.doUnderLock(ActionSpec.builder()
                    .lockNames(locksPreserveOrder)
                    .action(() -> {
                        SetMultimap<String, NodeDescription> allGatewayViewers = gatewayViewerDao.getNodeAllViewers(gateway, streamSet);
                        for (String encryptedStreamKey : allGatewayViewers.keys()) {
                            Set<NodeDescription> gatewayClients = allGatewayViewers.get(encryptedStreamKey);
                            gatewayClients.forEach(gatewayClient -> {
                                if (gatewayClient.getNodeType() == NodeType.EDGE) {
                                    destroyConnectionGraphFromEdge(gatewayClient, ImmutableList.of(encryptedStreamKey));
                                } else if (gatewayClient.getNodeType() == NodeType.RELAY) {
                                    destroyConnectionGraphFromRelay(gatewayClient, ImmutableList.of(encryptedStreamKey));
                                }
                            });
                        }
                        balanceManagerService.getActualGatewayBalancer().releaseStreams(streams, gateway);
                    })
                    .leaseTimeMs(getEdgeClusterLockLeaseTime())
                    .waitTimeMs(getEdgeClusterLockWaitTime())
                    .build());
        } catch (Exception e) {
            log.warn("Couldn't destroy connection graph for streams=[{}] on node=[{}]", streams, gateway);
        }
    }

    private void destroyConnectionGraphFromRelay(@Nonnull NodeDescription relay, @Nonnull Collection<String> streams) {
        EdgeBalanceService actualEdgeBalancer = balanceManagerService.getActualEdgeBalancer();
        Set<String> streamSet = Sets.newHashSet(streams);
        String region = relay.getRegion();

        List<String> edgesAllClusterLockNames = liveNodeVersionDao.getAllVersions(NodeType.EDGE).stream()
                .map(version -> getNodeClusterLockName(region, NodeType.EDGE, version))
                .sorted()
                .collect(Collectors.toList());
        String relayClusterLockName = getNodeClusterLockName(region, NodeType.RELAY, relay.getVersion());
        ImmutableList<String> locksPreserveOrder = ImmutableList.<String>builder()
                .addAll(edgesAllClusterLockNames)
                .add(relayClusterLockName)
                .build();

        try {
            locksService.doUnderLock(ActionSpec.builder()
                    .lockNames(locksPreserveOrder)
                    .action(() -> {
                        Multimap<NodeDescription, Command> commands = MultimapBuilder.hashKeys().linkedListValues().build();
                        SetMultimap<String, NodeDescription> allRelayViewers = relayViewerDao.getNodeAllViewers(relay, streamSet);
                        for (String encryptedStreamKey : allRelayViewers.keys()) {
                            allRelayViewers.get(encryptedStreamKey).forEach(relayClient -> {
                                releaseEdgeViewers(relayClient, encryptedStreamKey);
                                ReleaseClients releaseClients = actualEdgeBalancer.releaseStream(encryptedStreamKey, relayClient);
                                edgeActiveViewerDao.removeAllViewers(relayClient, encryptedStreamKey);
                                commands.putAll(relayClient, releaseClients.getCommands());
                            });
                        }
                        commandProcessor.executePreserveOrderAsync(commands);

                        streams.forEach(stream -> {
                            try {
                                excludeNodeFromGraphUpwards(stream, relay);
                            } catch (Exception e) {
                                log.warn("Couldn't destroy connection graph for stream=[{}] on node=[{}]", stream, relay);
                            }
                        });
                    })
                    .leaseTimeMs(getEdgeClusterLockLeaseTime())
                    .waitTimeMs(getEdgeClusterLockWaitTime())
                    .build());
        } catch (Exception e) {
            log.warn("Couldn't destroy connection graph for streams=[{}] on node=[{}]", streams, relay);
        }
    }

    private void destroyConnectionGraphFromEdge(@Nonnull NodeDescription edge, @Nonnull Collection<String> streams) {
        String region = edge.getRegion();
        String version = edge.getVersion();
        String edgesClusterLockName = getNodeClusterLockName(region, NodeType.EDGE, version);
        try {
            locksService.doUnderLock(ActionSpec.builder()
                    .lockNames(ImmutableList.of(edgesClusterLockName))
                    .action(() -> streams.forEach(stream -> {
                        releaseEdgeViewers(edge, stream);
                        try {
                            excludeNodeFromGraphUpwards(stream, edge);
                        } catch (Exception e) {
                            log.warn("Couldn't destroy connection graph for stream=[{}] on node=[{}]", stream, edge);
                        }
                    }))
                    .leaseTimeMs(getEdgeClusterLockLeaseTime())
                    .waitTimeMs(getEdgeClusterLockWaitTime())
                    .build());
        } catch (Exception e) {
            log.warn("Couldn't destroy connection graph for streams=[{}] on node=[{}]", streams, edge);
        }
    }

    @Override
    public void destroyConnectionGraphAsync(@Nonnull NodeDescription node, @Nonnull Collection<String> streams) {
        asyncTaskExecutorService.submit(() -> {
            try {
                destroyConnectionGraph(node, streams);
            } catch (Exception e) {
                log.error("Couldn't destroy connection graph for streams=[{}] on node=[{}]", streams, node);
            }
        });
    }

    private void releaseEdgeViewers(@Nonnull NodeDescription edge, @Nonnull String encryptedStreamKey) {
        edgeActiveViewerDao.removeAllViewers(edge, encryptedStreamKey);
        edgePendingViewerDao.removeAllViewers(edge, encryptedStreamKey);
    }

    @Override
    public void addViewerCapacity(@Nonnull String encryptedStreamKey, int capacityToAdd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addViewerCapacityAsync(@Nonnull String encryptedStreamKey, int capacityToAdd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeViewerCapacity(@Nonnull String encryptedStreamKey, @Nonnull Map<NodeDescription, Integer> capacityToRemove) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeViewerCapacityAsync(@Nonnull String encryptedStreamKey, @Nonnull Map<NodeDescription, Integer> capacityToRemove) {
        throw new UnsupportedOperationException();
    }


    @Nonnull
    private List<String> getAllLocks(@Nonnull NodeType nodeType) {
        return liveRegionDao.getAllRegions(nodeType).stream()
                .flatMap(region -> liveNodeVersionDao.getAllVersions(nodeType).stream().map(version -> Pair.of(region, version)))
                .map(pair -> getNodeClusterLockName(pair.getLeft(), nodeType, pair.getRight()))
                .sorted()
                .collect(toList());
    }

    private long getGatewayClusterLockWaitTime() {
        return configurationService.get().getLong(GATEWAY_CLUSTER_LOCK_WAIT_TIME_MS.getLeft(), GATEWAY_CLUSTER_LOCK_WAIT_TIME_MS.getRight());
    }

    private long getGatewayClusterLockLeaseTime() {
        return configurationService.get().getLong(GATEWAY_CLUSTER_LOCK_LEASE_TIME_MS.getLeft(), GATEWAY_CLUSTER_LOCK_LEASE_TIME_MS.getRight());
    }

    private long getRelayClusterLockWaitTime() {
        return configurationService.get().getLong(RELAY_CLUSTER_LOCK_WAIT_TIME_MS.getLeft(), RELAY_CLUSTER_LOCK_WAIT_TIME_MS.getRight());
    }

    private long getRelayClusterLockLeaseTime() {
        return configurationService.get().getLong(RELAY_CLUSTER_LOCK_LEASE_TIME_MS.getLeft(), RELAY_CLUSTER_LOCK_LEASE_TIME_MS.getRight());
    }

    private long getEdgeClusterLockWaitTime() {
        return configurationService.get().getLong(EDGE_CLUSTER_LOCK_WAIT_TIME_MS.getLeft(), EDGE_CLUSTER_LOCK_WAIT_TIME_MS.getRight());
    }

    private long getEdgeClusterLockLeaseTime() {
        return configurationService.get().getLong(EDGE_CLUSTER_LOCK_LEASE_TIME_MS.getLeft(), EDGE_CLUSTER_LOCK_LEASE_TIME_MS.getRight());
    }

    private boolean isDirectAccessEnabled() {
        return configurationService.get().getBoolean(IS_DIRECT_ACCESS_ENABLED.getLeft(), IS_DIRECT_ACCESS_ENABLED.getRight());
    }

    private boolean isTopologyBuildEnabled() {
        return configurationService.get().getBoolean(IS_BUILD_TOPOLOGY_ENABLED.getLeft(), IS_BUILD_TOPOLOGY_ENABLED.getRight());
    }

    @PreDestroy
    void destroy() throws InterruptedException {
        asyncTaskExecutorService.shutdownNow();
        asyncTaskExecutorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Value
    @Builder
    private static class BalanceResult {
        @Nullable
        BookedNodes chosenEdges;
        @Nullable
        BookedNodes chosenRelays;
        @Nullable
        BookedNodes bookedEdges;
        @Nullable
        BookedNodes bookedRelays;

        public static BalanceResult none() {
            return BalanceResult.builder().build();
        }
    }
}
