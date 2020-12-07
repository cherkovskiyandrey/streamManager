package com.tango.stream.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.tango.stream.manager.dao.*;
import com.tango.stream.manager.model.CallSpec;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.model.StreamDescription;
import com.tango.stream.manager.restClients.NodeCommandController;
import com.tango.stream.manager.service.BalanceManagerService;
import com.tango.stream.manager.service.LocksService;
import com.tango.stream.manager.service.RegistryManagerService;
import com.tango.stream.manager.service.TopologyManagerService;
import com.tango.stream.manager.service.impl.ConfigurationService;
import com.tango.stream.manager.service.impl.EdgeRegisterService;
import com.tango.stream.manager.service.impl.GatewayRegisterService;
import com.tango.stream.manager.service.impl.RelayRegisterService;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.tango.stream.manager.conf.DaoConfig.*;
import static com.tango.stream.manager.service.impl.GatewayRegisterService.TTL_TO_STREAM_RECOVERY;
import static com.tango.stream.manager.service.impl.VersionServiceImpl.DEFAULT_VERSION;
import static com.tango.stream.manager.utils.NamedLocksHelper.getNodeClusterLockName;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Disabled
public class BaseTest {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    public static final String DEFAULT_REGION = "default";
    protected final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<String, Object> overriddenProperties = new HashMap<>();

    @Autowired
    protected TopologyManagerService topologyManagerService;
    @Autowired
    protected LocksService locksService;
    @Autowired
    @Qualifier(NODE_CLUSTER_DAO_FOR_LOST_GATEWAYS)
    protected NodeClusterDao lostCluster;
    @Autowired
    @Qualifier(GATEWAY_STREAM)
    protected StreamDao<StreamDescription> gatewayStreamDao;
    @Autowired
    @Qualifier(GW_PENDING_STREAM)
    protected ExpiringStreamDao<String> gatewayPendingDao;
    @Autowired
    @Qualifier(GW_LOST_STREAM)
    protected StreamDao<String> gatewayLostStreamDao;
    @Autowired
    @Qualifier(GATEWAY_VIEWER)
    protected ViewerDao<NodeDescription> gatewayViewerDao;
    @Autowired
    @Qualifier(RELAY_VIEWER)
    protected ViewerDao<NodeDescription> relayViewerDao;
    @Autowired
    @Qualifier(RELAY_STREAM)
    protected StreamDao<StreamDescription> relayStreamDao;
    @Autowired
    @Qualifier(EDGE_ACTIVE_VIEWER)
    protected ViewerDao<String> edgeActiveViewerDao;
    @Autowired
    @Qualifier(EDGE_STREAM)
    protected StreamDao<StreamDescription> edgeStreamDao;
    @Autowired
    protected PendingViewerDao edgePendingViewerDao;
    @Autowired
    protected GatewayRegisterService gatewayRegisterService;
    @Autowired
    protected RelayRegisterService relayRegisterService;
    @Autowired
    protected EdgeRegisterService edgeRegisterService;
    @Autowired
    @MockBean
    protected NodeCommandController nodeCommandController;
    @Autowired
    protected RedissonClient redissonClient;
    @Autowired
    protected RegistryManagerService registryManagerService;
    @Autowired
    protected NodeDao nodeDao;
    @Autowired
    protected BalanceManagerService balanceManagerService;
    @Autowired
    protected ConfigurationService configurationService;

    public void setProperty(String key, Object value) {
        overriddenProperties.putIfAbsent(key, configurationService.get().getProperty(key));
        configurationService.update(configuration -> configuration.setProperty(key, value));
    }

    public void resetOverriddenProperties() {
        for (String key : overriddenProperties.keySet()) {
            Object value = overriddenProperties.get(key);
            if (value == null) {
                configurationService.update(configuration -> configuration.clearProperty(key));
            } else {
                configurationService.update(configuration -> configuration.setProperty(key, value));
            }
        }
    }

    @BeforeEach
    @AfterEach
    public void clear() {
        try {
            clearClusters();
        } finally {
            resetOverriddenProperties();
        }
    }

    private void clearClusters() {
        //to speed up releasing of resources
        setProperty(PENDING_STREAM_EXPIRATION_TIME_SEC.getLeft(), "0");
        setProperty(TTL_TO_STREAM_RECOVERY.getLeft(), "0");

        NodeClusterDao gatewayClusterDao = balanceManagerService.getActualGatewayBalancer().getClusterDao();
        Collection<NodeDescription> gatewayCluster = gatewayClusterDao.getCluster(DEFAULT_REGION, DEFAULT_VERSION);
        gatewayCluster.forEach(nodeDescription -> {
            registryManagerService.getNodeRegistryService(nodeDescription.getNodeType()).unregister(nodeDescription.getNodeUid());
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> {
                                ReflectionTestUtils.invokeMethod(gatewayRegisterService, "runGCForSpecificCluster", DEFAULT_REGION, NodeType.GATEWAY, DEFAULT_VERSION);
                                return locksService.doUnderLock(CallSpec.<Boolean>builder()
                                        .lockNames(ImmutableList.of(getNodeClusterLockName(DEFAULT_REGION, NodeType.GATEWAY, DEFAULT_VERSION)))
                                        .action(() ->
                                                !nodeDao.isExist(nodeDescription.getNodeUid()) &&
                                                        gatewayStreamDao.size(nodeDescription) == 0 &&
                                                        gatewayPendingDao.size(nodeDescription) == 0 &&
                                                        gatewayLostStreamDao.size(nodeDescription) == 0 &&
                                                        gatewayViewerDao.getAllViewersSize(nodeDescription) == 0 &&
                                                        lostCluster.isEmpty(DEFAULT_REGION, DEFAULT_VERSION))
                                        .leaseTimeMs(5000)
                                        .waitTimeMs(5000)
                                        .build());
                            }
                    );
        });
        log.info("remove all cluster where type=[{}], region=[{}] and version=[{}]", NodeType.GATEWAY, DEFAULT_REGION, DEFAULT_VERSION);
        gatewayClusterDao.removeAll(DEFAULT_REGION, DEFAULT_VERSION);

        NodeClusterDao relayClusterDao = balanceManagerService.getActualRelayBalancer().getClusterDao();
        Collection<NodeDescription> relayCluster = relayClusterDao.getCluster(DEFAULT_REGION, DEFAULT_VERSION);
        relayCluster.forEach(nodeDescription -> {
            registryManagerService.getNodeRegistryService(nodeDescription.getNodeType()).unregister(nodeDescription.getNodeUid());
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> {
                                ReflectionTestUtils.invokeMethod(relayRegisterService, "runGCForSpecificCluster", DEFAULT_REGION, NodeType.RELAY, DEFAULT_VERSION);
                                return locksService.doUnderLock(CallSpec.<Boolean>builder()
                                        .lockNames(ImmutableList.of(getNodeClusterLockName(DEFAULT_REGION, NodeType.RELAY, DEFAULT_VERSION)))
                                        .action(() ->
                                                !nodeDao.isExist(nodeDescription.getNodeUid()) &&
                                                        relayStreamDao.size(nodeDescription) == 0 &&
                                                        relayViewerDao.getAllViewersSize(nodeDescription) == 0)
                                        .leaseTimeMs(5000)
                                        .waitTimeMs(5000)
                                        .build());
                            }
                    );
        });
        relayClusterDao.removeAll(DEFAULT_REGION, DEFAULT_VERSION);

        NodeClusterDao edgeClusterDao = balanceManagerService.getActualEdgeBalancer().getClusterDao();
        Collection<NodeDescription> edgeCluster = edgeClusterDao.getCluster(DEFAULT_REGION, DEFAULT_VERSION);
        edgeCluster.forEach(nodeDescription -> {
            registryManagerService.getNodeRegistryService(nodeDescription.getNodeType()).unregister(nodeDescription.getNodeUid());
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> {
                                ReflectionTestUtils.invokeMethod(edgeRegisterService, "runGCForSpecificCluster", DEFAULT_REGION, NodeType.EDGE, DEFAULT_VERSION);
                                return locksService.doUnderLock(CallSpec.<Boolean>builder()
                                        .lockNames(ImmutableList.of(getNodeClusterLockName(DEFAULT_REGION, NodeType.EDGE, DEFAULT_VERSION)))
                                        .action(() ->
                                                !nodeDao.isExist(nodeDescription.getNodeUid()) &&
                                                        edgeStreamDao.size(nodeDescription) == 0 &&
                                                        edgeActiveViewerDao.getAllViewersSize(nodeDescription) == 0 &&
                                                        edgePendingViewerDao.getAllViewersSize(nodeDescription) == 0)
                                        .leaseTimeMs(5000)
                                        .waitTimeMs(5000)
                                        .build());
                            }
                    );
        });
        edgeClusterDao.removeAll(DEFAULT_REGION, DEFAULT_VERSION);
    }
}
