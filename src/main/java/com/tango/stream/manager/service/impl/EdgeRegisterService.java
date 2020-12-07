package com.tango.stream.manager.service.impl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tango.stream.manager.conf.DaoConfig;
import com.tango.stream.manager.dao.PendingViewerDao;
import com.tango.stream.manager.dao.StreamDao;
import com.tango.stream.manager.dao.ViewerDao;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.service.BalanceService;
import com.tango.stream.manager.service.BroadcasterStatService;
import com.tango.stream.manager.service.EdgeBalanceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

import static com.tango.stream.manager.conf.DaoConfig.EDGE_STREAM;

/**
 * Differences:
 * 1. There is a time lag between adding to streamData of stream and real applying, in this time we can balance viewers to node which don't know
 * about stream - I think it isn't a problem, because there are 2 options: fast fallback to HLS if none edge is ready or balance booked edge and
 * return. In second option clients have ability to try to connect several times before fallback to HLS and can be successfully connected in more cases.
 * <p>
 * 2. (*) In case where failed connection we can ask clients to reconnect to other edge if that available and its connect with hostNode is ok (avoid edge)
 * <p>
 * 3. (*) In case where switching couldn't be processed - ask clients to reconnect to other edge if that available and its connect with hostNode is ok (avoid edge)
 */
@Slf4j
@Service
public class EdgeRegisterService extends EdgeOrRelayRegisterService {
    private final static NodeType EDGE_NODE_TYPE = NodeType.EDGE;

    @Autowired
    @Qualifier(DaoConfig.EDGE_ACTIVE_VIEWER)
    protected ViewerDao<String> edgeActiveViewerDao;
    @Autowired
    private PendingViewerDao edgePendingViewerDao;
    @Autowired
    @Qualifier(EDGE_STREAM)
    private StreamDao<StreamDescription> edgeStreamDao;
    @Autowired
    private List<EdgeBalanceService> edgeBalanceServices;
    @Autowired
    private BroadcasterStatService broadcasterStatService;

    @Nonnull
    @Override
    public NodeType nodeType() {
        return EDGE_NODE_TYPE;
    }

    @Nonnull
    @Override
    protected StreamDao<StreamDescription> getStreamDao() {
        return edgeStreamDao;
    }

    @Override
    @Nonnull
    protected List<BalanceService> getBalanceService() {
        return Lists.newArrayList(edgeBalanceServices);
    }

    @Nonnull
    @Override
    protected BalanceService getActualBalancer() {
        return balanceManagerService.getActualEdgeBalancer();
    }

    /**
     * Track:
     * - pending clients: expire and remove or move to active one (+)
     * - active clients: if was active but now inactive - remove from active collection
     */
    @Override
    protected void handleClients(@Nonnull NodeData newNodeData, @Nonnull Map<String, StreamData> activeStreams, @Nonnull Set<String> knownStreams) {
        NodeDescription nodeDescription = newNodeData.toNodeDescription();
        String region = newNodeData.getRegion();
        Set<String> allStreams = ImmutableSet.<String>builder().addAll(activeStreams.keySet()).addAll(knownStreams).build();

        for (String encryptedStreamKey : allStreams) {
            //pending to active
            Set<String> pendingViewers = edgePendingViewerDao.getNodeAllViewers(nodeDescription, encryptedStreamKey);
            Set<String> actualViewers = Optional.ofNullable(activeStreams.get(encryptedStreamKey))
                    .map(StreamData::getActualViewers)
                    .orElse(ImmutableSet.of());
            Set<String> newComers = Sets.newHashSet(Sets.intersection(actualViewers, pendingViewers));
            if (!newComers.isEmpty()) {
                log.debug("on edge=[{}] for stream=[{}] pending to active viewers=[{}]", nodeDescription, encryptedStreamKey, newComers);
                edgePendingViewerDao.removeViewers(nodeDescription, encryptedStreamKey, newComers);
            }

            //expired
            Collection<String> expiredViewers = edgePendingViewerDao.getAndRemoveExpired(nodeDescription, encryptedStreamKey);
            if (!expiredViewers.isEmpty()) {
                log.info("on edge=[{}] for stream=[{}] expired viewers=[{}]", nodeDescription, encryptedStreamKey, expiredViewers);
                expiredViewers.forEach(viewerEncryptedId -> broadcasterStatService.onRemoveViewer(
                        encryptedStreamKey,
                        region,
                        viewerEncryptedId
                ));
            }

            //was active but now inactive
            Set<String> activeViewers = edgeActiveViewerDao.getNodeAllViewers(nodeDescription, encryptedStreamKey);
            Set<String> lostViewers = Sets.newHashSet(Sets.difference(activeViewers, actualViewers));
            if (!lostViewers.isEmpty()) {
                log.warn("on edge=[{}] for stream=[{}] lost viewers=[{}]", nodeDescription, encryptedStreamKey, lostViewers);
                edgeActiveViewerDao.removeViewers(nodeDescription, encryptedStreamKey, lostViewers);
                lostViewers.forEach(viewerEncryptedId -> broadcasterStatService.onRemoveViewer(
                        encryptedStreamKey,
                        region,
                        viewerEncryptedId
                ));
            }

            //unexpected but present
            ImmutableSet<String> knownViewers = ImmutableSet.<String>builder()
                    .addAll(pendingViewers)
                    .addAll(activeViewers)
                    .build();
            Set<String> unexpectedViewers = Sets.newHashSet(Sets.difference(actualViewers, knownViewers));
            if (!unexpectedViewers.isEmpty()) {
                log.warn("on edge=[{}] for stream=[{}] unexpected viewers viewers=[{}]", nodeDescription, encryptedStreamKey, unexpectedViewers);
                edgeActiveViewerDao.addViewers(nodeDescription, encryptedStreamKey, unexpectedViewers);
            }
        }

    }

    @Override
    protected void removeLostViewers(@Nonnull Collection<NodeDescription> nodeCluster) {
        for (NodeDescription edge : nodeCluster) {
            Set<String> allActualStreams = getStreamDao().getAllStreams(edge).stream().map(StreamDescription::getEncryptedStreamKey).collect(Collectors.toSet());
            removeLostViewersDao(edgeActiveViewerDao, edge, allActualStreams, "active");
            removeLostViewersDao(edgeActiveViewerDao, edge, allActualStreams, "pending");
        }
    }

    private void removeLostViewersDao(@Nonnull ViewerDao<String> viewerDao,
                                      @Nonnull NodeDescription edge,
                                      @Nonnull Set<String> allActualStreams,
                                      @Nonnull String daoName) {
        Set<String> streamsFromActiveViewerDao = viewerDao.getAllKnownStreams(edge);
        Set<String> lostViewersInDao = Sets.newHashSet(Sets.difference(streamsFromActiveViewerDao, allActualStreams));
        lostViewersInDao.forEach(stream -> {
            log.info("lost {} viewers in dao on edge=[{}] for stream=[{}]. Cleaning...", daoName, edge, stream);
            viewerDao.removeAllViewers(edge, stream);
        });
    }
}
