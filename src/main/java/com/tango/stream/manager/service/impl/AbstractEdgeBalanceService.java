package com.tango.stream.manager.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.tango.stream.manager.conf.DaoConfig;
import com.tango.stream.manager.dao.*;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.service.BroadcasterStatService;
import com.tango.stream.manager.service.EdgeBalanceService;
import com.tango.stream.manager.utils.EncryptUtil;
import com.tango.stream.manager.utils.NamedLocksHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tango.stream.manager.conf.DaoConfig.EDGE_STREAM;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractEdgeBalanceService extends EdgeOrRelayBalanceServiceImpl implements EdgeBalanceService {
    private static final Pair<String, Integer> EDGE_SLOT_SIZE = Pair.of("balancer.edge.slot.size", 10);
    private static final Pair<String, String> DEFAULT_REGION = Pair.of("balancer.edge.region.toBalance.default", "us-east");
    private static final Pair<String, Long> EDGE_ASSIGNMENT_WAIT_TIME_MS = Pair.of("balancer.edge.assignment.wait.time.ms", 1000L);
    private static final Pair<String, Long> EDGE_ASSIGNMENT_LEASE_TIME_MS = Pair.of("balancer.edge.assignment.lease.time.ms", 1000L);

    @Autowired
    @Qualifier(DaoConfig.GATEWAY_STREAM)
    protected StreamDao<StreamDescription> gatewayStreamDao;
    @Autowired
    @Qualifier(DaoConfig.GW_PENDING_STREAM)
    protected ExpiringStreamDao<String> gatewayPendingStreamDao;
    @Autowired
    @Qualifier(DaoConfig.EDGE_ACTIVE_VIEWER)
    protected ViewerDao<String> activeViewerDao;
    @Autowired
    protected PendingViewerDao pendingViewerDao;
    @Autowired
    @Qualifier(EDGE_STREAM)
    protected StreamDao<StreamDescription> edgeStreamDao;
    @Autowired
    private NodeDao nodeDao;
    @Autowired
    protected BroadcasterStatService broadcasterStatService;

    protected int toBookedClients(int clients) {
        return (int) Math.ceil((double) clients / getSlotSize());
    }

    private int getSlotSize() {
        return configurationService.get().getInt(EDGE_SLOT_SIZE.getLeft(), EDGE_SLOT_SIZE.getRight());
    }

    @Nonnull
    private String getDefaultRegion() {
        return configurationService.get().getString(DEFAULT_REGION.getLeft(), DEFAULT_REGION.getRight());
    }

    @Override
    protected StreamDao<StreamDescription> getStreamDao() {
        return edgeStreamDao;
    }

    @Nonnull
    @Override
    protected NodeType getNodeType() {
        return NodeType.EDGE;
    }

    @Nonnull
    @Override
    public AvailableEdgeResponse getAvailableEdge(@Nonnull AvailableEdgeRequest request) throws Exception {
        String encryptedStreamKey = request.getEncryptedStreamKey();
        NodeType nodeType = NodeType.EDGE;
        request.setRegion(StringUtils.firstNonBlank(request.getRegion(), getDefaultRegion()));
        String viewerRegion = Objects.requireNonNull(request.getRegion());

        return locksService.doUnderLock(CallSpec.<AvailableEdgeResponse>builder()
                .lockNames(ImmutableList.of(NamedLocksHelper.getStreamLockName(nodeType, viewerRegion, encryptedStreamKey)))
                .action(() -> getAvailableEdgeBase(request))
                .fallbackAction(() -> fallbackAvailableEdgeBase(request))
                .waitTimeMs(getEdgeAssignmentWaitTime())
                .leaseTimeMs(getEdgeAssignmentLeaseTime())
                .build());
    }

    @Nonnull
    private AvailableEdgeResponse fallbackAvailableEdgeBase(@Nonnull AvailableEdgeRequest request) {
        String encryptedStreamKey = request.getEncryptedStreamKey();
        log.warn("Couldn't lock resources to assign edge for stream=[{}] for request=[{}]", encryptedStreamKey, request);
        return AvailableEdgeResponse.none();
    }

    @Nonnull
    public AvailableEdgeResponse getAvailableEdgeBase(@Nonnull AvailableEdgeRequest request) {
        String encryptedStreamKey = request.getEncryptedStreamKey();
        request.setRegion(StringUtils.firstNonBlank(request.getRegion(), getDefaultRegion()));
        String viewerRegion = Objects.requireNonNull(request.getRegion());
        Long viewerAccountId = request.getViewerAccountId();
        String viewerUsername = request.getViewerUsername();
        String region = request.getRegion();

        if (viewerAccountId == null || viewerUsername == null) {
            //direct access to gateway
            Collection<NodeDescription> gateways = gatewayStreamDao.getNodesByStream(encryptedStreamKey);
            if (gateways.isEmpty()) {
                gateways = gatewayPendingStreamDao.getNodesByStream(encryptedStreamKey);
                if (gateways.isEmpty()) {
                    log.warn("Couldn't find active gateway for stream=[{}] for request=[{}]", encryptedStreamKey, request);
                    return AvailableEdgeResponse.none();
                }
            }
            NodeData gateway = nodeDao.getNodeById(gateways.stream().findFirst().get().getNodeUid());
            if (gateway == null) {
                log.warn("Couldn't find active gateway for stream=[{}] for request=[{}]", encryptedStreamKey, request);
                return AvailableEdgeResponse.none();
            }
            return AvailableEdgeResponse.builder()
                    .region(gateway.getRegion())
                    .externalIP(gateway.getExtIp())
                    .build();
        }

        String encryptedViewerId = EncryptUtil.encryptViewer(viewerAccountId, viewerUsername);
        Set<NodeDescription> voidedNodes = findViewerEdges(encryptedStreamKey, viewerAccountId, viewerUsername, encryptedViewerId, region);
        List<NodeDescription> orderedEdges = getOrderedEdges(encryptedStreamKey, viewerRegion, voidedNodes);
        if (orderedEdges.isEmpty()) {
            log.warn("Couldn't find any edges for stream=[{}] in viewerRegion=[{}] for request=[{}]", encryptedStreamKey, viewerRegion, request);
            return AvailableEdgeResponse.none();
        }

        NodeData edge = registerViewer(encryptedStreamKey, orderedEdges, viewerRegion, viewerAccountId, viewerUsername, encryptedViewerId);
        if (edge == null) {
            log.error("All slots are busy and couldn't assign edge for stream=[{}] in viewerRegion=[{}] for request=[{}]", encryptedStreamKey, viewerRegion, request);
            return AvailableEdgeResponse.none();
        }
        return AvailableEdgeResponse.builder()
                .externalIP(edge.getExtIp())
                .region(viewerRegion)
                .build();
    }

    @Nonnull
    private Set<NodeDescription> findViewerEdges(@Nonnull String encryptedStreamKey,
                                                 long viewerAccountId,
                                                 @Nonnull String viewerUsername,
                                                 @Nonnull String encryptedViewerId,
                                                 @Nonnull String region) {
        Collection<NodeDescription> nodesByStreamInRegion = getStreamDao().getNodesByStreamInRegion(encryptedStreamKey, region);
        return Stream.of(activeViewerDao, pendingViewerDao)
                .map(viewerDao -> {
                    NodeDescription currentEdge = nodesByStreamInRegion.stream()
                            .filter(edge -> viewerDao.isViewerExists(edge, encryptedStreamKey, encryptedViewerId))
                            .findAny()
                            .orElse(null);
                    if (currentEdge != null) {
                        log.warn("Viewer=[{}:{}] has been already assign to edge=[{}] for stream=[{}]. Try to assign to other one.",
                                viewerAccountId, viewerUsername, currentEdge, encryptedStreamKey);
                        return currentEdge;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Nonnull
    private List<NodeDescription> getOrderedEdges(@Nonnull String encryptedStreamKey, @Nonnull String region, @Nonnull Set<NodeDescription> voidedNodes) {
        Collection<NodeDescription> activeEdges = edgeStreamDao.getNodesByStreamInRegion(encryptedStreamKey, region);
        List<Pair<Integer, NodeDescription>> weightedNodes = activeEdges.stream()
                .map(nd -> Pair.of(edgeStreamDao.findStream(nd, encryptedStreamKey)
                        .map(StreamDescription::getMaxAvailableClients)
                        .orElse(0), nd))
                .sorted(Comparator.comparingInt((ToIntFunction<Pair<Integer, NodeDescription>>) Pair::getLeft).reversed())
                .collect(Collectors.toList());

        int maxWeight = weightedNodes.stream().findFirst().map(Pair::getLeft).orElse(0);
        return weightedNodes.stream()
                .sorted(Comparator.comparingInt(pair -> voidedNodes.contains(pair.getRight()) ? pair.getLeft() - maxWeight : pair.getLeft())).map(Pair::getRight)
                .collect(Collectors.toList());
    }

    @Nullable
    private NodeData registerViewer(@Nonnull String encryptedStreamKey,
                                    @Nonnull List<NodeDescription> orderedEdges,
                                    @Nonnull String viewerRegion,
                                    long viewerAccountId,
                                    @Nonnull String viewerUsername,
                                    @Nonnull String encryptedViewerId) {
        for (NodeDescription nodeDescription : orderedEdges) {
            Optional<StreamDescription> streamDataOpt = edgeStreamDao.findStream(nodeDescription, encryptedStreamKey);
            if (!streamDataOpt.isPresent()) {
                log.error("Couldn't find stream streamData on edge=[{}] for stream=[{}] for viewer=[{}:{}]", nodeDescription, encryptedStreamKey, viewerAccountId, viewerUsername);
                return null;
            }
            StreamDescription streamData = streamDataOpt.get();
            Set<String> pendingViewers = pendingViewerDao.getNodeAllViewers(nodeDescription, encryptedStreamKey);
            Set<String> actualViewers = activeViewerDao.getNodeAllViewers(nodeDescription, encryptedStreamKey);
            Set<String> realPendingViewers = Sets.newHashSet(Sets.difference(pendingViewers, actualViewers));
            int currentViewers = actualViewers.size() + realPendingViewers.size();
            if (currentViewers < streamData.getMaxAvailableClients()) {
                pendingViewerDao.addViewer(nodeDescription, encryptedStreamKey, encryptedViewerId);
                log.info("Chose edge=[{}] with stream=[{}] for viewer=[{}:{}]. MaxAvailableViewers=[{}], activeViewers=[{}]",
                        nodeDescription, encryptedStreamKey, viewerAccountId, viewerUsername, streamData.getMaxAvailableClients(), currentViewers);
                broadcasterStatService.onAddViewer(encryptedStreamKey, viewerRegion, encryptedViewerId);
                return nodeDao.getNodeById(nodeDescription.getNodeUid());
            }
        }
        return null;
    }

    private long getEdgeAssignmentLeaseTime() {
        return configurationService.get().getLong(EDGE_ASSIGNMENT_LEASE_TIME_MS.getLeft(), EDGE_ASSIGNMENT_LEASE_TIME_MS.getRight());
    }

    private long getEdgeAssignmentWaitTime() {
        return configurationService.get().getLong(EDGE_ASSIGNMENT_WAIT_TIME_MS.getLeft(), EDGE_ASSIGNMENT_WAIT_TIME_MS.getRight());
    }
}






















