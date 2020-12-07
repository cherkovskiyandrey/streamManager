package com.tango.stream.manager.conf;

import com.tango.stream.manager.dao.*;
import com.tango.stream.manager.dao.impl.ExpiringStreamKeyDaoImpl;
import com.tango.stream.manager.dao.impl.PendingViewerDaoImpl;
import com.tango.stream.manager.dao.impl.StreamKeyDaoImpl;
import com.tango.stream.manager.dao.impl.ViewerDaoImpl;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.StreamData;
import com.tango.stream.manager.model.StreamDescription;
import com.tango.stream.manager.service.impl.*;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.function.Function;

import static com.tango.stream.manager.model.NodeType.*;

@Configuration
@RequiredArgsConstructor
public class DaoConfig {
    public static final Pair<String, Integer> PENDING_STREAM_EXPIRATION_TIME_SEC = Pair.of("node.registry.pending.stream.expiration.time.sec", 10);
    public static final String EDGE_ACTIVE_VIEWER = "edgeActiveViewer";
    public static final String GATEWAY_VIEWER = "gatewayViewer";
    public static final String RELAY_VIEWER = "relayViewer";
    public static final String EDGE_STREAM = "edgeStream";
    public static final String RELAY_STREAM = "relayStream";
    public static final String GATEWAY_STREAM = "gatewayStream";
    public static final String GW_LOST_STREAM = "gwLostStream";
    public static final String GW_PENDING_STREAM = "gwPendingStream";
    public static final String NODE_CLUSTER_DAO_FOR_EDGE_CPU_USAGE_BALANCER = "nodeClusterDaoForEdgeCpuUsageBalancer";
    public static final String NODE_CLUSTER_DAO_FOR_RELAY_CPU_USAGE_BALANCER = "nodeClusterDaoForRelayCpuUsageBalancer";
    public static final String NODE_CLUSTER_DAO_FOR_GATEWAY_CPU_USAGE_BALANCER = "nodeClusterDaoForGatewayCpuUsageBalancer";
    public static final String NODE_CLUSTER_DAO_FOR_GATEWAY_LAST_FRAGMENT_BALANCER = "nodeClusterDaoForGatewayLastFragmentBalancer";
    public static final String NODE_CLUSTER_DAO_FOR_LOST_GATEWAYS = "nodeClusterDaoForLostGateways";

    private final RedissonClient redissonClient;
    private final ConfigurationService configurationService;

    @Bean(EDGE_ACTIVE_VIEWER)
    public ViewerDao<String> edgeActiveViewerDao() {
        return new ViewerDaoImpl<>(EDGE_ACTIVE_VIEWER, redissonClient, configurationService);
    }

    @Bean(GATEWAY_VIEWER)
    public ViewerDao<NodeDescription> gatewayViewerDao() {
        return new ViewerDaoImpl<>(GATEWAY_VIEWER, redissonClient, configurationService);
    }

    @Bean(RELAY_VIEWER)
    public ViewerDao<NodeDescription> relayViewerDao() {
        return new ViewerDaoImpl<>(RELAY_VIEWER, redissonClient, configurationService);
    }

    @Bean(GW_LOST_STREAM)
    public StreamDao<String> gwLostStreamDao() {
        return new StreamKeyDaoImpl<>(GW_LOST_STREAM, redissonClient, configurationService, Function.identity());
    }

    @Bean(GW_PENDING_STREAM)
    public ExpiringStreamDao<String> gwPendingStreamDao(Clock clock) {
        return new ExpiringStreamKeyDaoImpl<>(GW_PENDING_STREAM, redissonClient, configurationService, Function.identity(),
                clock, conf -> configurationService.get().getInt(PENDING_STREAM_EXPIRATION_TIME_SEC.getKey(), PENDING_STREAM_EXPIRATION_TIME_SEC.getValue()));
    }

    @Bean(GATEWAY_STREAM)
    public StreamDao<StreamDescription> gatewayStreamDao() {
        return new StreamKeyDaoImpl<>(GATEWAY_STREAM, redissonClient, configurationService, StreamDescription::getEncryptedStreamKey);
    }

    @Bean(RELAY_STREAM)
    public StreamDao<StreamDescription> relayStreamDao() {
        return new StreamKeyDaoImpl<>(RELAY_STREAM, redissonClient, configurationService, StreamDescription::getEncryptedStreamKey);
    }

    @Bean(EDGE_STREAM)
    public StreamDao<StreamDescription> edgeStreamDao() {
        return new StreamKeyDaoImpl<>(EDGE_STREAM, redissonClient, configurationService, StreamDescription::getEncryptedStreamKey);
    }

    @Bean
    public PendingViewerDao edgePendingViewerDao() {
        return new PendingViewerDaoImpl("edgePendingViewer", redissonClient, configurationService);
    }

    @Bean(NODE_CLUSTER_DAO_FOR_EDGE_CPU_USAGE_BALANCER)
    public NodeClusterDao nodeClusterDaoForEdgeCpuUsageBalancer(NodeClusterDaoFactory nodeClusterDaoFactory) {
        return nodeClusterDaoFactory.create(EdgeCpuUsageBalancer.NAME, EDGE);
    }

    @Bean(NODE_CLUSTER_DAO_FOR_RELAY_CPU_USAGE_BALANCER)
    public NodeClusterDao nodeClusterDaoForRelayCpuUsageBalancer(NodeClusterDaoFactory nodeClusterDaoFactory) {
        return nodeClusterDaoFactory.create(RelayCpuUsageBalancer.NAME, RELAY);
    }

    @Bean(NODE_CLUSTER_DAO_FOR_GATEWAY_CPU_USAGE_BALANCER)
    public NodeClusterDao nodeClusterDaoForGatewayCpuUsageBalancer(NodeClusterDaoFactory nodeClusterDaoFactory) {
        return nodeClusterDaoFactory.create(GatewayCpuUsageBalancer.NAME, GATEWAY);
    }

    @Bean(NODE_CLUSTER_DAO_FOR_GATEWAY_LAST_FRAGMENT_BALANCER)
    public NodeClusterDao nodeClusterDaoForGatewayLastFragmentBalancer(NodeClusterDaoFactory nodeClusterDaoFactory) {
        return nodeClusterDaoFactory.create(GatewayLastFragmentSizeBaseBalancer.NAME, GATEWAY);
    }

    @Bean(NODE_CLUSTER_DAO_FOR_LOST_GATEWAYS)
    public NodeClusterDao nodeClusterDaoForLostGateways(NodeClusterDaoFactory nodeClusterDaoFactory) {
        return nodeClusterDaoFactory.create("lostGateways", GATEWAY);
    }
}
