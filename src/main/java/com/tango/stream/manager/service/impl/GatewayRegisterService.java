package com.tango.stream.manager.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.tango.stream.manager.conf.DaoConfig;
import com.tango.stream.manager.dao.*;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.service.GatewayBalanceService;
import com.tango.stream.manager.service.TopologyManagerService;
import com.tango.stream.manager.utils.EncryptUtil;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tango.stream.manager.conf.DaoConfig.NODE_CLUSTER_DAO_FOR_LOST_GATEWAYS;
import static com.tango.stream.manager.conf.KafkaConfig.TERMINATED_STREAMS_TOPIC;
import static com.tango.stream.manager.model.Metrics.Tags.*;
import static java.util.stream.Collectors.toMap;

@Slf4j
@Service
public class GatewayRegisterService extends AbstractNodeRegistryService {
    private final static NodeType GATEWAY_NODE_TYPE = NodeType.GATEWAY;
    public final static Pair<String, Integer> TTL_TO_STREAM_RECOVERY = Pair.of("node.registry.gateway.stream.recovery.ttl", 10);

    @Autowired
    private List<GatewayBalanceService> balanceServices;
    @Autowired
    @Qualifier(DaoConfig.GATEWAY_STREAM)
    protected StreamDao<StreamDescription> gatewayStreamDao;
    @Autowired
    @Qualifier(DaoConfig.GW_PENDING_STREAM)
    private ExpiringStreamDao<String> pendingStreamDao;
    @Autowired
    @Qualifier(DaoConfig.GW_LOST_STREAM)
    protected StreamDao<String> lostStreamDao;
    @Autowired
    @Qualifier(DaoConfig.GATEWAY_VIEWER)
    private ViewerDao<NodeDescription> viewerDao;
    @Autowired
    private TopologyManagerService topologyManagerService;
    @Autowired
    private LiveStreamDao liveStreamDao;
    @Autowired
    @Qualifier(NODE_CLUSTER_DAO_FOR_LOST_GATEWAYS)
    private NodeClusterDao lostCluster;

    @Nonnull
    @Override
    public NodeType nodeType() {
        return GATEWAY_NODE_TYPE;
    }

    @KafkaListener(
            topics = "#{'${" + TERMINATED_STREAMS_TOPIC + "}'}",
            containerFactory = "terminatedStreamKafkaContainerFactory",
            autoStartup = "${terminated.streams.listener.autostart.enabled:true}"
    )
    public void processRecord(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String streamId,
                              @Payload Stream terminatedStream) {
        if (terminatedStream == null) {
            log.warn("Received malformed message body is empty for key=[{}]", streamId);
            return;
        }
        if (terminatedStream.getInitTime() == null) {
            log.warn("Received malformed message body is empty for key=[{}] and value=[{}]", streamId, terminatedStream);
            return;
        }
        String encryptStreamKey = EncryptUtil.encryptStreamKey(StreamKey.builder()
                .initTime(terminatedStream.getInitTime())
                .streamId(terminatedStream.getId())
                .build());

        Collection<NodeDescription> gateways = gatewayStreamDao.getNodesByStream(encryptStreamKey);
        gateways.forEach(gateway -> topologyManagerService.destroyConnectionGraph(gateway, ImmutableList.of(encryptStreamKey)));
    }

    @Override
    protected void processNodeData(@Nonnull NodeData newNodeData, @Nonnull Map<String, StreamData> activeStreams) {
        NodeDescription nodeDescription = newNodeData.toNodeDescription();

        lostCluster.removeNodeFromCluster(nodeDescription);
        handlePendingStreams(newNodeData, activeStreams);
        handleUnexpectedLiveStreams(newNodeData, activeStreams);
        //don't process lost streams, it could be suspend, if not - stream termination from kafka will destroy topology

        lostStreamDao.removeAllStreams(nodeDescription);
        balanceServices.forEach(bs -> bs.recalculateAndApplyScore(newNodeData, activeStreams));
    }

    @Nonnull
    @Override
    protected StreamDao<StreamDescription> getStreamDao() {
        return gatewayStreamDao;
    }

    private void handlePendingStreams(@Nonnull NodeData newNodeData, @Nonnull Map<String, StreamData> activeStreams) {
        NodeDescription nodeDescription = newNodeData.toNodeDescription();
        Set<String> activeStreamIds = activeStreams.keySet();
        List<String> balancedStreams = pendingStreamDao.removeStreams(nodeDescription, activeStreamIds);
        if (!balancedStreams.isEmpty()) {
            log.info("Balanced streams: {}", balancedStreams);
        }

        Collection<String> allExpiredStreams = pendingStreamDao.getAndRemoveExpired(nodeDescription);
        if (!allExpiredStreams.isEmpty()) {
            log.info("Expired streams: {}", allExpiredStreams);
            topologyManagerService.destroyConnectionGraphAsync(newNodeData.toNodeDescription(), allExpiredStreams);
        }
    }

    private void handleUnexpectedLiveStreams(@Nonnull NodeData newNodeData, @Nonnull Map<String, StreamData> activeStreams) {
        NodeDescription nodeDescription = newNodeData.toNodeDescription();
        Set<String> activeStreamIds = activeStreams.keySet();
        Set<String> knownStreams = gatewayStreamDao.getAllStreams(nodeDescription).stream()
                .map(StreamDescription::getEncryptedStreamKey)
                .collect(Collectors.toSet());
        Set<String> unexpectedStreamsId = Sets.newHashSet(Sets.difference(activeStreamIds, knownStreams));
        if (unexpectedStreamsId.isEmpty()) {
            return;
        }

        //TODO: may be recovering topology is better
        log.error("Critical: node=[{}] has unexpected streams: [{}].", nodeDescription, unexpectedStreamsId);
        topologyManagerService.destroyConnectionGraph(nodeDescription, unexpectedStreamsId);
    }

    @Override
    protected void runGCForSpecificClusterUnderLock(@Nonnull String region, @Nonnull NodeType nodeType, @Nonnull String version) {
        Tags tags = Tags.of(VERSION, version, NODE_TYPE, nodeType.name(), REGION, region);
        try {
            GatewayBalanceService actualGatewayBalancer = balanceManagerService.getActualGatewayBalancer();
            NodeClusterDao clusterDao = actualGatewayBalancer.getClusterDao();

            Collection<NodeDescription> nodeCluster = clusterDao.getCluster(region, version);
            if (!nodeCluster.isEmpty()) {
                gcHandleLostNodes(nodeCluster, tags);
            }
            gcHandlePendingStreamsToRecovery(region, version);

            //TODO: remove dead streams

        } finally {
            meterRegistry.counter(Metrics.Counters.NODES_GS_COUNTER_OK, tags).increment();
        }
    }

    private void gcHandleLostNodes(@Nonnull Collection<NodeDescription> nodeCluster, @Nonnull Tags tags) {
        List<NodeDescription> lostNodesList = nodeCluster.stream()
                .filter(nd -> !nodeDao.isExist(nd.getNodeUid()))
                .collect(Collectors.toList());
        if (lostNodesList.isEmpty()) {
            return;
        }
        lostNodesList.forEach(lostStreamDao::removeAllStreams);
        lostNodesList.forEach(nd -> lostCluster.addNodeToCluster(nd, Instant.now().toEpochMilli()));
        lostNodesList.forEach(nd -> balanceServices.forEach(bs -> bs.getClusterDao().removeNodeFromCluster(nd)));

        log.error("GC: Next nodes have been lost (absent keepAlive) and removed from balancing: {}", lostNodesList);
        meterRegistry.counter(Metrics.Counters.LOST_NODES, tags).increment(lostNodesList.size());
    }

    private void gcHandlePendingStreamsToRecovery(@Nonnull String region, @Nonnull String version) {
        Collection<NodeDescription> expiredLostNodes = lostCluster.getClusterByScores(region, version, 0, getTtlToStreamRecovering());
        Map<NodeDescription, Collection<String>> expiredStreamToClients =
                expiredLostNodes.stream()
                        .map(nd -> Pair.of(nd, gatewayStreamDao.getAllStreams(nd).stream().map(StreamDescription::getEncryptedStreamKey).collect(Collectors.toList())))
                        .filter(pair -> !pair.getRight().isEmpty())
                        .collect(toMap(
                                Pair::getLeft,
                                Pair::getRight
                        ));

        expiredLostNodes.forEach(lostStreamDao::removeAllStreams);
        expiredLostNodes.forEach(pendingStreamDao::removeAllStreams);
        expiredLostNodes.forEach(lostCluster::removeNodeFromCluster);

        expiredStreamToClients.forEach((gateway, streams) -> {
            log.info("Expired on gateway=[{}] pending for recovering streams=[{}]", gateway, streams);
            topologyManagerService.destroyConnectionGraphAsync(gateway, streams);
        });
    }

    private long getTtlToStreamRecovering() {
        return Instant.now().minusSeconds(getTtlToStreamRecoveringSec()).toEpochMilli();
    }

    private long getTtlToStreamRecoveringSec() {
        return configurationService.get().getInt(TTL_TO_STREAM_RECOVERY.getLeft(), TTL_TO_STREAM_RECOVERY.getRight());
    }
}