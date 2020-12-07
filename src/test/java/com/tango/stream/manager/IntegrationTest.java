package com.tango.stream.manager;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tango.stream.manager.conf.DaoConfig;
import com.tango.stream.manager.dao.ExpiringStreamDao;
import com.tango.stream.manager.dao.NodeClusterDao;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.service.TopologyManagerService;
import com.tango.stream.manager.service.impl.ConfigurationService;
import com.tango.stream.manager.service.impl.GatewayCpuUsageBalancer;
import com.tango.stream.manager.service.impl.GatewayRegisterService;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.tango.stream.manager.service.impl.VersionServiceImpl.DEFAULT_VERSION;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class IntegrationTest extends NoKafkaBaseTest {
    @Autowired
    private ConfigurationService configurationService;
    @Autowired
    private TopologyManagerService topologyManagerService;
    @Autowired
    private GatewayRegisterService nodeRegistryService;
    @Autowired
    @Qualifier(DaoConfig.GW_PENDING_STREAM)
    private ExpiringStreamDao<String> gatewayPendingStreamDao;
    private Map<String, TestNode> testNodes;
    private List<TestClient> testClients;
    private List<TestClient> testFallbackClients;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @AfterEach
    public void clean() {
        if (testClients != null) {
            testClients.forEach(TestClient::stop);
        }
        if (testFallbackClients != null) {
            testFallbackClients.forEach(TestClient::stop);
        }
        if (testNodes != null) {
            testNodes.forEach((url, node) -> node.stop());
        }
    }

    @Test
    public void shouldBalanceAllStreamsWhenImmediateLoad() throws InterruptedException, ExecutionException {
        //by cpu usage: 80%/0.4(per stream)*5(nodes)=1000 clients
        //by lastFragmentSize: 3_000_000 * 1000 = 3_000_000_000 > 200_000_000
        testNodes = createTestNodes(5, NodeType.GATEWAY);
        testClients = createTestClients(1000, testNodes);
        testFallbackClients = createTestClients(100, testNodes);

        setupConnections(testClients);
        makeSureGoodEstablishedConnections(testClients);
        makeSureGoodBalancing();

        setupConnections(testFallbackClients);
        makeSureFallbackSrtConnections(testFallbackClients);
    }

    //todo: release load from nodes
    //todo: gc tests

    private void makeSureFallbackSrtConnections(@Nonnull List<TestClient> client) {
        client.forEach(cl -> assertFalse(cl.hasConnection()));
    }

    private void makeSureGoodEstablishedConnections(@Nonnull List<TestClient> clients) {
        clients.forEach(cl -> assertTrue(cl.hasConnection()));
    }

    private void setupConnections(@Nonnull List<TestClient> clients) throws InterruptedException, ExecutionException {
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        CyclicBarrier cyclicBarrier = new CyclicBarrier(10);
        for (int i = 0; i < clients.size(); ++i) {
            int clientNum = i;
            completableFutures.add(CompletableFuture.runAsync(() -> clients.get(clientNum).setupConnection(cyclicBarrier), executorService));
            if ((i + 1) % 10 == 0) {
                CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0])).get();
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }
    }

    private void makeSureGoodBalancing() throws InterruptedException {
        TimeUnit.SECONDS.sleep((long) (gatewayPendingStreamDao.getExpirationPeriodInSec() * 1.5));
        NodeClusterDao gatewayClusterDao = balanceManagerService.getActualGatewayBalancer().getClusterDao();
        Collection<NodeDescription> gatewayCluster = gatewayClusterDao.getCluster(DEFAULT_REGION, DEFAULT_VERSION);
        assertEquals(5, gatewayCluster.size());
        gatewayCluster.forEach(nodeDescription -> {
            Optional<Long> curScore = gatewayClusterDao.getScore(nodeDescription);
            assertTrue(curScore.isPresent());
            log.info("Node item: {}, score {}", nodeDescription, curScore.get());
            assertEquals(0, gatewayPendingDao.size(nodeDescription));
            assertTrue(curScore.filter(score -> score > 7500).isPresent());
        });
    }

    private class TestNode {
        private final ScheduledExecutorService executorService;
        private final DelayQueue<TestStream> testStreams = new DelayQueue<>();
        private final Map<String, StreamData> activeStreams = Maps.newHashMap();
        private final String extIp;
        private final String uid;
        private final NodeType nodeType;
        private final String region;
        private final String version;

        TestNode(String uid, String extIp, NodeType nodeType, String region, String version) {
            this.extIp = extIp;
            this.uid = uid;
            this.nodeType = nodeType;
            this.region = region;
            this.version = version;
            this.executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("node-thread-" + uid)
                    .setUncaughtExceptionHandler((t, e) -> log.error("Error in thread: {}.", t.getName(), e))
                    .build());
            sendKeepAlive();
            this.executorService.scheduleWithFixedDelay(this::sendKeepAlive, 0, 500, TimeUnit.MILLISECONDS);
        }

        private void sendKeepAlive() {
            try {
                manageActiveStreams();
                sendKeepAliveToNode();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Current thread {} has been interrupted.", Thread.currentThread().getName());
                return;
            } catch (Exception e) {
                log.error("Send keepAlive ERROR: ", e);
            }
            log.info("Send keepAlive for uuid: {}, extIp: {}", uid, extIp);
        }

        private void sendKeepAliveToNode() throws Exception {
            nodeRegistryService.onKeepAlive(
                    KeepAliveRequest.builder()
                            .extIp(extIp)
                            .region(region)
                            .version(version)
                            .uid(uid)
                            .nodeType(nodeType)
                            .activeStreams(activeStreams)
                            .avgCpuUsage(activeStreams.size() * configurationService.get().getDouble(GatewayCpuUsageBalancer.AVERAGE_CPU_USAGE_BY_ONE_STREAM.getLeft()))
                            .medCpuUsage(activeStreams.size() * configurationService.get().getDouble(GatewayCpuUsageBalancer.AVERAGE_CPU_USAGE_BY_ONE_STREAM.getLeft()))
                            .build()
            );
        }

        private void manageActiveStreams() throws InterruptedException {
            while (true) {
                final TestStream testStream = testStreams.poll(100, TimeUnit.MILLISECONDS);
                if (testStream == null) {
                    break;
                }
                if (StreamAction.ADD == testStream.streamAction) {
                    activeStreams.put(testStream.getEncryptedStreamKey(), StreamData.builder()
                            .viewersCount(0) //todo
                            .ingressBitrate(0) //todo
                            .egressBitrate(0) //todo
                            .encryptedStreamKey(testStream.getEncryptedStreamKey())
                            .lastFragmentSizeInBytes(Maps.newHashMap(
                                    ImmutableMap.of(
                                            "hd", 1_000_000L,
                                            "sd", 1_000_000L,
                                            "ld", 1_000_000L
                                    )
                            ))
                            .build());
                } else {
                    activeStreams.remove(testStream.getEncryptedStreamKey());
                }
            }
        }

        public String getExtIp() {
            return extIp;
        }

        void addStream(@Nonnull String encryptedStreamKey, long delay) {
            testStreams.put(TestStream.builder()
                    .encryptedStreamKey(encryptedStreamKey)
                    .delayInMs(delay)
                    .streamAction(StreamAction.ADD)
                    .build()
            );
        }

        void removeStream(@Nonnull String encryptedStreamKey, long delay) {
            testStreams.put(TestStream.builder()
                    .encryptedStreamKey(encryptedStreamKey)
                    .delayInMs(delay)
                    .streamAction(StreamAction.REMOVE)
                    .build()
            );
        }

        void stop() {
            executorService.shutdown();
            try {
                executorService.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignore) {
            }
        }
    }

    private enum StreamAction {
        ADD, REMOVE;
    }

    @Value
    @Builder
    private static class TestStream implements Delayed {
        String encryptedStreamKey;
        long delayInMs;
        @Builder.Default
        long createTimeMs = System.currentTimeMillis();
        StreamAction streamAction;

        @Override
        public long getDelay(TimeUnit unit) {
            long diff = (createTimeMs + delayInMs) - System.currentTimeMillis();
            return unit.convert(diff, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return (int) (getStartTime() - ((TestStream) o).getStartTime());
        }

        long getStartTime() {
            return createTimeMs + delayInMs;
        }
    }

    private class TestClient {
        private final Map<String, TestNode> testNodes;
        private final String region;
        private volatile String currentEncryptedStreamKey;
        private volatile String nodeExternalIp;

        private TestClient(Map<String, TestNode> testNodes, String region) {
            this.testNodes = testNodes;
            this.region = region;
        }

        @SneakyThrows
        boolean setupConnection() {
            AvailableGatewayRequest availableGatewayRequest = new AvailableGatewayRequest();
            availableGatewayRequest.setEncryptedStreamKey(UUID.randomUUID().toString());
            availableGatewayRequest.setRegion(region);
            AvailableGatewayResponse availableGateway = topologyManagerService.getAvailableGateway(availableGatewayRequest);
            String externalIP = availableGateway.getExternalIP();
            if (StringUtils.isBlank(externalIP)) {
                return false;
            }
            TestNode testNode = testNodes.get(externalIP);
            if (testNode == null) {
                return false;
            }
            long delay = ThreadLocalRandom.current().nextLong(gatewayPendingStreamDao.getExpirationPeriodInSec() / 2);
            testNode.addStream(availableGatewayRequest.getEncryptedStreamKey(), delay * 1000L);
            nodeExternalIp = externalIP;
            currentEncryptedStreamKey = availableGatewayRequest.getEncryptedStreamKey();
            return true;
        }

        void setupConnection(@Nonnull CyclicBarrier cyclicBarrier) {
            try {
                cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                log.error("Error during waiting for simultaneous start.", e);
                throw new IllegalStateException(e);
            }
            setupConnection();
        }

        boolean hasConnection() {
            return StringUtils.isNotBlank(currentEncryptedStreamKey);
        }

        void stop() {
            testNodes.computeIfPresent(nodeExternalIp, (k, v) -> {
                v.removeStream(currentEncryptedStreamKey, 0L);
                return v;
            });
        }
    }


    @Nonnull
    private List<TestClient> createTestClients(int amount, @Nonnull Map<String, TestNode> nodes) {
        return IntStream.range(0, amount).mapToObj(i -> new TestClient(nodes, "default")).collect(Collectors.toList());
    }

    private Map<String, TestNode> createTestNodes(int amount, NodeType type) {
        return IntStream.range(0, amount).mapToObj(i -> new TestNode(
                UUID.randomUUID().toString(),
                generateIp(),
                type,
                DEFAULT_REGION,
                DEFAULT_VERSION
        )).collect(Collectors.toMap(
                TestNode::getExtIp,
                Function.identity()
        ));
    }

    private String generateIp() {
        int a = ThreadLocalRandom.current().nextInt(255);
        int b = ThreadLocalRandom.current().nextInt(255);
        int c = ThreadLocalRandom.current().nextInt(255);
        int d = ThreadLocalRandom.current().nextInt(255);
        return String.join(".", String.valueOf(a), String.valueOf(b), String.valueOf(c), String.valueOf(d));
    }
}
