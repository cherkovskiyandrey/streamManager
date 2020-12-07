package com.tango.stream.manager.controllers;

import com.google.common.collect.Maps;
import com.tango.stream.manager.NoKafkaBaseTest;
import com.tango.stream.manager.conf.DaoConfig;
import com.tango.stream.manager.dao.ExpiringStreamDao;
import com.tango.stream.manager.dao.NodeClusterDao;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.service.impl.ConfigurationService;
import com.tango.stream.manager.service.impl.GatewayCpuUsageBalancer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.tango.stream.manager.service.impl.VersionServiceImpl.DEFAULT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class ManageControllerTest extends NoKafkaBaseTest {
    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ConfigurationService configurationService;
    @Autowired
    @Qualifier(DaoConfig.GW_PENDING_STREAM)
    private ExpiringStreamDao<String> gatewayPendingStreamDao;

    @Test
    public void shouldGetAvailableGatewayTest() throws Exception {
        registryGateway("1", "1.1.1.1");
        mockMvc.perform(post("/manager/availableGateway")
                .content("{\"streamerAccountId\":\"1\",\"encryptedStreamKey\":\"123\",\"region\":\"" + DEFAULT_REGION + "\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().json(objectMapper.writeValueAsString(
                        AvailableGatewayResponse.builder()
                                .externalIP("1.1.1.1")
                                .region(DEFAULT_REGION)
                                .build())
                ));
    }

    @Test
    public void shouldReleaseReservedBalanceQuotaAfterTimeout() throws Exception {
        registryGateway("1", "1.1.1.1");
        mockMvc.perform(post("/manager/availableGateway")
                .content("{\"streamerAccountId\":\"1\",\"encryptedStreamKey\":\"123\",\"region\":\"" + DEFAULT_REGION + "\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().json(objectMapper.writeValueAsString(
                        AvailableGatewayResponse.builder()
                                .externalIP("1.1.1.1")
                                .region(DEFAULT_REGION)
                                .build())
                ));
        long expectedVal = (long) (configurationService.get().getDouble(GatewayCpuUsageBalancer.AVERAGE_CPU_USAGE_BY_ONE_STREAM.getLeft()) * 100L);
        assertThat(getCurrentScore()).isEqualTo(expectedVal);
        long streamExpTimeout = gatewayPendingStreamDao.getExpirationPeriodInSec();
        await().atMost(streamExpTimeout * 2, TimeUnit.SECONDS).until(() -> {
            registryGateway("1", "1.1.1.1");
            TimeUnit.SECONDS.sleep(1);
            return getCurrentScore() == 0;
        });
    }

    private long getCurrentScore() {
        NodeClusterDao clusterDao = balanceManagerService.getActualGatewayBalancer().getClusterDao();
        Collection<NodeDescription> cluster = clusterDao.getCluster(DEFAULT_REGION, DEFAULT_VERSION);
        assertThat(cluster).hasSize(1);
        Optional<Long> score = clusterDao.getScore(cluster.iterator().next());
        assertTrue(score.isPresent());
        return score.get();
    }

    @Test
    public void shouldTakeIntoAccountVoidedGateways() throws Exception {
        registryGateway("1", "1.1.1.1");
        registryGateway("2", "2.2.2.2");
        for (int i = 0; i < 10; i++) {
            mockMvc.perform(post("/manager/availableGateway")
                    .content("{\"encryptedStreamKey\":\"123\",\"region\":\"" + DEFAULT_REGION + "\",\"excludedGateways\":[\"1.1.1.1\"]}")
                    .contentType(MediaType.APPLICATION_JSON))
                    .andDo(print())
                    .andExpect(status().isOk())
                    .andExpect(content().json(objectMapper.writeValueAsString(
                            AvailableGatewayResponse.builder()
                                    .externalIP("2.2.2.2")
                                    .region(DEFAULT_REGION)
                                    .build())
                    ));
        }
    }

    @Test
    public void shouldMarkNodeAsPotentialUnreachable() throws Exception {
        registryGateway("1", "1.1.1.1");
        balanceAndVerify(1, 90, "1.1.1.1", null);
        registryGateway("2", "2.2.2.2");
        balanceAndVerify(201, 290, "2.2.2.2", null);
        registryGateway("1", "1.1.1.1", generateActiveStreams(1, 90));
        registryGateway("2", "2.2.2.2", generateActiveStreams(201, 290));

        balanceAndVerify(1, 8, "2.2.2.2", "1.1.1.1"); // force to second gateway form first one
        balanceAndVerify(291, 299, "1.1.1.1", null);  // first gateway is less loaded than second - balancer should choose first one
        balanceAndVerify(9, 10, "2.2.2.2", "1.1.1.1"); // force for second one stream from first gateway
        balanceAndVerify(300, 320, "2.2.2.2", null);  // first gateway is less loaded but isn't chosen because of marked as unreachable

        registryGateway("1", "1.1.1.1", generateActiveStreams(11, 90), generateActiveStreams(291, 299));
        registryGateway("2", "2.2.2.2", generateActiveStreams(201, 290), generateActiveStreams(1, 10), generateActiveStreams(300, 320));

        verify("1", "1.1.1.1", 89, 0, 0);
        verify("2", "2.2.2.2", 121, 0, 0);

        balanceAndVerify(401, 430, "1.1.1.1", null);
    }

    private void verify(String nodeId, String ip, int activeStreams, int lostStreams, int pendingStreams) {
        NodeDescription nodeDescription = NodeDescription.builder()
                .extIp(ip)
                .nodeType(NodeType.GATEWAY)
                .nodeUid(nodeId)
                .region(DEFAULT_REGION)
                .version(DEFAULT_VERSION)
                .build();
        assertEquals(activeStreams, gatewayStreamDao.size(nodeDescription));
        assertEquals(pendingStreams, gatewayPendingStreamDao.size(nodeDescription));
        assertEquals(lostStreams, gatewayLostStreamDao.size(nodeDescription));
    }

    private void balanceAndVerify(int firstStreamId, int lastStreamId, @Nonnull String expectedExtId, @Nullable String excludedExtId) throws Exception {
        String urlPattern = excludedExtId == null ?
                "{\"encryptedStreamKey\":\"%d\",\"region\":\"" + DEFAULT_REGION + "\"}" :
                "{\"encryptedStreamKey\":\"%d\",\"region\":\"" + DEFAULT_REGION + "\",\"excludedGateways\":[\"%s\"]}";
        for (int i = firstStreamId; i <= lastStreamId; ++i) {
            System.out.println("===>> " + i);
            mockMvc.perform(post("/manager/availableGateway")
                    .content(excludedExtId == null ? String.format(urlPattern, i) : String.format(urlPattern, i, excludedExtId))
                    .contentType(MediaType.APPLICATION_JSON))
                    .andDo(print())
                    .andExpect(status().isOk())
                    .andExpect(content().json(objectMapper.writeValueAsString(
                            AvailableGatewayResponse.builder()
                                    .externalIP(expectedExtId)
                                    .region(DEFAULT_REGION)
                                    .build())
                    ));
        }
    }

    private Map<String, StreamData> generateActiveStreams(int firstStreamId, int lastStreamId) {
        return IntStream.rangeClosed(firstStreamId, lastStreamId)
                .mapToObj(i -> StreamData.builder()
                        .encryptedStreamKey(String.valueOf(i))
                        .egressBitrate(1)
                        .egressBitrate(1)
                        .viewersCount(0)
                        .build())
                .collect(Collectors.toMap(
                        StreamData::getEncryptedStreamKey,
                        Function.identity()
                ));
    }

    private void registryGateway(@Nonnull String uid, @Nonnull String ip) throws Exception {
        registryGateway(uid, ip, Maps.newHashMap());
    }

    @SafeVarargs
    private final void registryGateway(@Nonnull String uid, @Nonnull String ip, @Nonnull Map<String, StreamData>... activeStream) throws Exception {
        Map<String, StreamData> allActiveStreams = Arrays.stream(activeStream)
                .reduce(Maps.newHashMap(), (Map<String, StreamData> result, Map<String, StreamData> element) -> {
                    result.putAll(element);
                    return result;
                });
        registryManagerService.getNodeRegistryService(NodeType.GATEWAY).onKeepAlive(KeepAliveRequest.builder()
                .extIp(ip)
                .region(DEFAULT_REGION)
                .version(DEFAULT_VERSION)
                .uid(uid)
                .nodeType(NodeType.GATEWAY)
                .avgCpuUsage(allActiveStreams.size() * configurationService.get().getDouble(GatewayCpuUsageBalancer.AVERAGE_CPU_USAGE_BY_ONE_STREAM.getLeft()))
                .medCpuUsage(allActiveStreams.size() * configurationService.get().getDouble(GatewayCpuUsageBalancer.AVERAGE_CPU_USAGE_BY_ONE_STREAM.getLeft()))
                .activeStreams(allActiveStreams)
                .build());
    }
}