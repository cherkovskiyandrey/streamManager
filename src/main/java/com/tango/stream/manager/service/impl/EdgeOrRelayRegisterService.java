package com.tango.stream.manager.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.tango.stream.manager.dao.LiveStreamDao;
import com.tango.stream.manager.dao.NodeClusterDao;
import com.tango.stream.manager.dao.StreamDao;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.service.BalanceService;
import com.tango.stream.manager.service.CommandProcessor;
import com.tango.stream.manager.service.TopologyManagerService;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.tango.stream.manager.model.Metrics.Tags.*;
import static java.util.stream.Collectors.toMap;


/**
 * Problems in control relay/edge
 * 1. Connection between manager and relay lost (relay is alive) and command can't be sent (commandProcessor)
 * 2. Relay fell down and manager can't sent command and can't distinguish the state from the first one (commandProcessor)
 * 3. Expected state isn't observed in keepAlive after timeout (+)
 * 5. Received unknown routes (+).
 * 7. We observe failedConnection
 * 8. Received unexpected viewers
 * 9. GC recognised node is fell down
 *
 * <p>
 * Reactions:
 * 1. and 2. The main idea is: command processor eventually tried to apply commands until manager think node is alive (nodeData ttl expired 15 seconds).
 * After that ether GC get away all streams from this node. Or connection recovers before ttl and command processor apply rest commands.
 * If connections recovers after TTL -> unexpected streams have to be destroyed.
 * Node state has to be checked on consistency:
 * - allowed connection != expected -> fatal error, avoid this node for stream
 * - failed connection == allowed connection -> 2 cases:
 * 1 - host node fall down (do nothing, wait for switching to other host)
 * 2 - after ttl host node is alive - avoid this node
 * - established connection != allowed connection after ttl -> avoid this node for stream
 * <p>
 * 3. It means command processor hasn't delivery command to this node despite recovering connection, for example - contract is broken during deploy.
 * Just rebuild route, exclude this node.
 * <p>
 * 5. If unknown routes on relay/edge - it can be in case where deleteRoute command hasn't been processed yet do nothing (just save as is),
 * do nothing, defragmentation process later consider the topology
 * <p>
 * 7. (a) For relay if gateway fell down we rely on auto-recovering streams it will make redirectViewers - do nothing just wait for
 * (b) For relay if srt connection lost between them - try to rebuild route exclude this relay
 * To distinguish two cases - start timer to wait for keepAlive from gateway, if timer expired and gateway is alive - (a), otherwise - (b)
 */
@Slf4j
public abstract class EdgeOrRelayRegisterService extends AbstractNodeRegistryService {
    @Autowired
    protected TopologyManagerService topologyManagerService;
    @Autowired
    protected LiveStreamDao liveStreamDao;
    @Autowired
    protected CommandProcessor commandProcessor;

    @Nonnull
    protected abstract List<BalanceService> getBalanceService();

    @Nonnull
    protected abstract BalanceService getActualBalancer();

    protected abstract void handleClients(@Nonnull NodeData newNodeData, @Nonnull Map<String, StreamData> activeStreams, @Nonnull Set<String> knownStreams);

    protected abstract void removeLostViewers(@Nonnull Collection<NodeDescription> nodeCluster);

    /**
     * The main idea is: command processor eventually tried to apply commands until manager think node is alive (nodeData ttl expired 15 seconds).
     * After that ether GC get away all streams from this node. Or connection recovers before ttl it means, command processor apply rest commands.
     * If connections recovers after TTL -> unexpected streams have to be destroyed.
     * Node state has to be checked on consistency:
     * - allowed connection != expected -> fatal error, avoid this node for stream
     * - failed connection == allowed connection -> 2 cases:
     * 1 - host node fall down (do nothing, wait for switching to other host)
     * 2 - after ttl host node is alive - avoid this node
     * - established connection != allowed connection after ttl -> avoid this node for stream
     * - maxClients != expected - fatal error, avoid this node for stream
     *
     */
    @Override
    protected void processNodeData(@Nonnull NodeData newNodeData, @Nonnull Map<String, StreamData> activeStreams) {
        NodeDescription nodeDescription = newNodeData.toNodeDescription();
        Collection<StreamDescription> knownStreamData = getStreamDao().getAllStreams(nodeDescription);
        Map<String, StreamDescription> knownStreams = knownStreamData.stream().collect(toMap(StreamDescription::getEncryptedStreamKey, Function.identity()));

        NodeData curNodeData = nodeDao.getNodeById(nodeDescription.getNodeUid());
        long lastNodeUpdate = Optional.ofNullable(curNodeData).map(NodeData::getLastUpdate).orElse(Long.MAX_VALUE);
        boolean isNodeSuspectedAsWasLost = lastNodeUpdate >= getSuspendTtlToLostNode();

        handleAbsenceStreams(newNodeData, knownStreamData, activeStreams, isNodeSuspectedAsWasLost);
        Set<String> unexpectedStreams = handleUnexpectedStreams(newNodeData, activeStreams, knownStreams, isNodeSuspectedAsWasLost);
        Set<String> expectedStreams = Sets.newHashSet(Sets.difference(activeStreams.keySet(), unexpectedStreams));
        handleKnownStreamsConsistency(newNodeData, expectedStreams, activeStreams, knownStreams);
        handleClients(newNodeData, activeStreams, knownStreams.keySet());

        getBalanceService().forEach(bs -> bs.recalculateAndApplyScore(newNodeData, activeStreams));
    }

    private void handleAbsenceStreams(@Nonnull NodeData newNodeData,
                                      @Nonnull Collection<StreamDescription> knownStreamData,
                                      @Nonnull Map<String, StreamData> activeStreams,
                                      boolean isNodeSuspendedAsWasLost) {
        NodeDescription nodeDescription = newNodeData.toNodeDescription();
        for (StreamDescription streamData : knownStreamData) {
            String encryptedStreamKey = streamData.getEncryptedStreamKey();
            StreamData activeStream = activeStreams.get(encryptedStreamKey);
            if (activeStream != null || !isTtlToApplyCommandExpired(streamData.getCommandTimestamp())) {
                continue;
            }
            if (isNodeSuspendedAsWasLost) {
                log.warn("keepAlive from node=[{}] doesn't contain expected stream=[{}], but it seems node/connection has been lost, check it later.",
                        nodeDescription, encryptedStreamKey);
                streamData.setCommandTimestamp(Instant.now().toEpochMilli());
                getStreamDao().addStream(nodeDescription, streamData);
            } else {
                log.warn("keepAlive from node=[{}] doesn't contain expected stream=[{}], avoiding...", nodeDescription, encryptedStreamKey);
                topologyManagerService.rebuildConnectionGraphAsync(encryptedStreamKey, nodeDescription);
            }
        }
    }

    @Nonnull
    private Set<String> handleUnexpectedStreams(@Nonnull NodeData newNodeData,
                                                @Nonnull Map<String, StreamData> activeStreams,
                                                @Nonnull Map<String, StreamDescription> knownStreams,
                                                boolean isNodeSuspectedAsWasLost) {
        NodeDescription nodeDescription = newNodeData.toNodeDescription();
        Set<String> unexpectedStreams = Sets.newHashSet(Sets.difference(activeStreams.keySet(), knownStreams.keySet()));
        for (String encryptedStreamKey : unexpectedStreams) {
            StreamData activeStream = activeStreams.get(encryptedStreamKey);
            if (isTtlToApplyCommandExpired(activeStream.getCommandTimestamp()) && !isNodeSuspectedAsWasLost) {
                log.warn("keepAlive from node=[{}] contains unexpected stream=[{}], destroying...", nodeDescription, encryptedStreamKey);
                topologyManagerService.destroyConnectionGraphAsync(nodeDescription, ImmutableList.of(encryptedStreamKey));
            }
        }
        return unexpectedStreams;
    }

    private void handleKnownStreamsConsistency(@Nonnull NodeData newNodeData,
                                               @Nonnull Set<String> expectedStreams,
                                               @Nonnull Map<String, StreamData> activeStreams,
                                               @Nonnull Map<String, StreamDescription> knownStreams) {
        expectedStreams.forEach(encryptedStreamKey -> {
            try {
                handleKnownStreamConsistency(newNodeData, encryptedStreamKey, activeStreams, knownStreams);
            } catch (Exception e) {
                log.error("Couldn't handle stream=[{}] consistency on node=[{}]", encryptedStreamKey, newNodeData.toNodeDescription(), e);
            }
        });
    }

    private void handleKnownStreamConsistency(@Nonnull NodeData newNodeData,
                                              @Nonnull String encryptedStreamKey,
                                              @Nonnull Map<String, StreamData> activeStreams,
                                              @Nonnull Map<String, StreamDescription> knownStreams) {
        NodeDescription nodeDescription = newNodeData.toNodeDescription();
        NodeData curNodeData = nodeDao.getNodeById(nodeDescription.getNodeUid());
        long lastNodeUpdate = Optional.ofNullable(curNodeData).map(NodeData::getLastUpdate).orElse(Long.MAX_VALUE);

        StreamDescription expectedState = knownStreams.get(encryptedStreamKey);
        StreamData actualState = activeStreams.get(encryptedStreamKey);
        if (!isCommandsUidEqual(expectedState, actualState)) {
            //was lost connection and command could be not processed yet, check it later
            if (lastNodeUpdate >= getSuspendTtlToLostNode()) {
                log.warn("expectedState=[{}] != actualState=[{}] for stream=[{}], but it seems node/connection has been lost, check command later",
                        expectedState.getCommandUid(), actualState.getCommandTimestamp(), encryptedStreamKey);
                expectedState.setCommandTimestamp(Instant.now().toEpochMilli());
                getStreamDao().addStream(nodeDescription, expectedState);
                return;
            }
            if (isTtlToApplyCommandExpired(expectedState.getCommandTimestamp())) {
                //command had enough time to be delivered and applied but wasn't - critical - avoid this node
                log.error("Critical: expectedState=[{}] != actualState=[{}] for stream=[{}], avoiding...",
                        expectedState.getCommandUid(), actualState.getCommandTimestamp(), encryptedStreamKey);
                topologyManagerService.rebuildConnectionGraphAsync(encryptedStreamKey, nodeDescription);
            }
            return;
        }
        NodeReference expectedAllowedConnection = NodeReference.builder()
                .nodeUid(expectedState.getHostNode().getNodeUid())
                .extIp(expectedState.getHostNode().getExtIp())
                .region(expectedState.getHostNode().getRegion())
                .build();
        NodeReference allowedConnection = actualState.getAllowedConnection();
        if (!allowedConnection.equals(expectedAllowedConnection)) {
            log.error("Critical: keepAlive from node=[{}] has divergence in host node for stream=[{}]. Manager assign=[{}], actual=[{}], avoiding...",
                    nodeDescription, encryptedStreamKey, expectedAllowedConnection, allowedConnection);
            topologyManagerService.rebuildConnectionGraphAsync(encryptedStreamKey, nodeDescription);
            return;
        }

        if (actualState.getFailedConnections().stream().anyMatch(allowedConnection::equals)) {
            log.warn("keepAlive from node=[{}] showed failed attempt to connect to hostNode=[{}] for stream=[{}]", nodeDescription, allowedConnection, encryptedStreamKey);
            NodeData hostNode = nodeDao.getNodeById(allowedConnection.getNodeUid());
            if (hostNode == null || hostNode.getLastUpdate() <= actualState.getStartTimeToLostConnection()) {
                long lastUpdateHostNode = Optional.ofNullable(hostNode).map(NodeData::getLastUpdate).orElse(0L);
                log.warn("connection=[{}] on node=[{}] is failed since=[{}], hostNode=[{}] has lastUpdate=[{}] and seems to be lost",
                        allowedConnection, nodeDescription, actualState.getStartTimeToLostConnection(), allowedConnection, lastUpdateHostNode);
                expectedState.setSuccessKeepsAliveFromLostingHostNode(0);
                getStreamDao().addStream(nodeDescription, expectedState);
                return;
            }
            if (expectedState.getSuccessKeepsAliveFromLostingHostNode() > 1) {
                expectedState.setSuccessKeepsAliveFromLostingHostNode(expectedState.getSuccessKeepsAliveFromLostingHostNode() + 1);
                getStreamDao().addStream(nodeDescription, expectedState);
                log.error("broken srt connection between node=[{}] and hostNode=[{}] for stream=[{}] detected, successKeepsAliveFromLostingHostNode=[{}], avoiding...",
                        nodeDescription, allowedConnection, encryptedStreamKey, expectedState.getSuccessKeepsAliveFromLostingHostNode());
                topologyManagerService.rebuildConnectionGraphAsync(encryptedStreamKey, nodeDescription);
            } else {
                expectedState.setSuccessKeepsAliveFromLostingHostNode(expectedState.getSuccessKeepsAliveFromLostingHostNode() + 1);
                getStreamDao().addStream(nodeDescription, expectedState);
                log.warn("suspected as broken srt connection between node=[{}] and hostNode=[{}] for stream=[{}] detected, successKeepsAliveFromLostingHostNode=[{}]",
                        nodeDescription, allowedConnection, encryptedStreamKey, expectedState.getSuccessKeepsAliveFromLostingHostNode());
            }
            return;
        }

        if (!actualState.getActualConnections().isEmpty() &&
                actualState.getActualConnections().stream().noneMatch(allowedConnection::equals) &&
                isTtlToSwitchHostExpired(actualState.getCommandTimestamp())) {
            log.error("keepAlive from node=[{}] has divergence in allowedConnection=[{}] and establishedConnections=[{}] for stream=[{}], avoiding...",
                    nodeDescription, allowedConnection, actualState.getActualConnections(), encryptedStreamKey);
            topologyManagerService.rebuildConnectionGraphAsync(encryptedStreamKey, nodeDescription);
            return;
        }

        if (actualState.getMaxAvailableClients() != expectedState.getMaxAvailableClients()) {
            log.error("keepAlive from node=[{}] has divergence in maxAllowedClients=[{}] and expectedMaxAllowedClients=[{}] for stream=[{}], avoiding...",
                    nodeDescription, actualState.getMaxAvailableClients(), expectedState.getMaxAvailableClients(), encryptedStreamKey);
            topologyManagerService.rebuildConnectionGraphAsync(encryptedStreamKey, nodeDescription);
            return;
        }
        expectedState.setSuccessKeepsAliveFromLostingHostNode(0);
        getStreamDao().addStream(nodeDescription, expectedState);
    }

    private boolean isCommandsUidEqual(@Nonnull StreamDescription expectedState, @Nonnull StreamData actualState) {
        return expectedState.getCommandUid() != null && actualState.getCommandUid() != null &&
                expectedState.getCommandUid().equals(actualState.getCommandUid());
    }

    @Override
    protected void runGCForSpecificClusterUnderLock(@Nonnull String region, @Nonnull NodeType nodeType, @Nonnull String version) {
        Tags tags = Tags.of(VERSION, version, NODE_TYPE, nodeType.name(), REGION, region);
        try {
            BalanceService actualBalanceService = getActualBalancer();
            NodeClusterDao clusterDao = actualBalanceService.getClusterDao();

            Collection<NodeDescription> nodeCluster = clusterDao.getCluster(region, version);
            if (nodeCluster.isEmpty()) {
                return;
            }
            List<NodeDescription> lostNodesList = nodeCluster.stream()
                    .filter(nd -> !nodeDao.isExist(nd.getNodeUid()))
                    .collect(Collectors.toList());
            if (lostNodesList.isEmpty()) {
                return;
            }
            lostNodesList.forEach(nd -> {
                commandProcessor.cancelAllCommands(nd);
                getBalanceService().forEach(bs -> bs.getClusterDao().removeNodeFromCluster(nd));
            });
            log.error("GC: Next nodes have been lost (absent keepAlive) and removed from balancing: {}", lostNodesList);
            meterRegistry.counter(Metrics.Counters.LOST_NODES, tags).increment(lostNodesList.size());

            topologyManagerService.rebuildConnectionGraphAsync(lostNodesList);

            //TODO: remove dead streams
            //TODO: remove pending and active viewers
            removeLostViewers(nodeCluster);

        } finally {
            meterRegistry.counter(Metrics.Counters.NODES_GS_COUNTER_OK, tags).increment();
        }
    }

    private boolean isTtlToApplyCommandExpired(long startTimestamp) {
        long ttlMs = configurationService.get().getLong(TTL_TO_APPLY_COMMAND_MS.getLeft(), TTL_TO_APPLY_COMMAND_MS.getRight());
        return Instant.now().toEpochMilli() - startTimestamp >= ttlMs;
    }


    private boolean isTtlToSwitchHostExpired(long startTimestamp) {
        long ttlMs = configurationService.get().getLong(TTL_TO_SWITCH_COMMAND_MS.getLeft(), TTL_TO_SWITCH_COMMAND_MS.getRight());
        return Instant.now().toEpochMilli() - startTimestamp >= ttlMs;
    }


    private long getSuspendTtlToLostNode() {
        throw new UnsupportedOperationException();
    }
}
