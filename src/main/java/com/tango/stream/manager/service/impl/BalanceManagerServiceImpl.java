package com.tango.stream.manager.service.impl;

import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.service.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class BalanceManagerServiceImpl implements BalanceManagerService {
    private final Pair<String, String> ACTUAL_GATEWAY_BALANCER_NAME = Pair.of("balancer.gateway.actual.name", GatewayCpuUsageBalancer.NAME);
    private final Pair<String, String> ACTUAL_RELAY_BALANCER_NAME = Pair.of("balancer.relay.actual.name", CpuUsageBalanceHelper.NAME);
    private final Pair<String, String> ACTUAL_EDGE_BALANCER_NAME = Pair.of("balancer.edge.actual.name", CpuUsageBalanceHelper.NAME);
    private final ConfigurationService configurationService;
    private final Map<String, GatewayBalanceService> actualGatewayBalancers;
    private final Map<String, RelayBalanceService> actualRelayBalancers;
    private final Map<String, EdgeBalanceService> actualEdgeBalancers;

    public BalanceManagerServiceImpl(List<GatewayBalanceService> actualGatewayBalancers,
                                     List<RelayBalanceService> relayBalanceServices,
                                     List<EdgeBalanceService> edgeBalanceServices,
                                     ConfigurationService configurationService) {
        this.configurationService = configurationService;
        this.actualGatewayBalancers = actualGatewayBalancers.stream().collect(Collectors.toMap(GatewayBalanceService::getName, Function.identity()));
        this.actualRelayBalancers = relayBalanceServices.stream().collect(Collectors.toMap(RelayBalanceService::getName, Function.identity()));
        this.actualEdgeBalancers = edgeBalanceServices.stream().collect(Collectors.toMap(EdgeBalanceService::getName, Function.identity()));
    }

    @Nonnull
    @Override
    public GatewayBalanceService getActualGatewayBalancer() {
        String name = configurationService.get().getString(ACTUAL_GATEWAY_BALANCER_NAME.getLeft(), ACTUAL_GATEWAY_BALANCER_NAME.getRight());
        return actualGatewayBalancers.get(name);
    }

    @Nonnull
    @Override
    public RelayBalanceService getActualRelayBalancer() {
        String name = configurationService.get().getString(ACTUAL_RELAY_BALANCER_NAME.getLeft(), ACTUAL_RELAY_BALANCER_NAME.getRight());
        return actualRelayBalancers.get(name);
    }

    @Nonnull
    @Override
    public EdgeBalanceService getActualEdgeBalancer() {
        String name = configurationService.get().getString(ACTUAL_EDGE_BALANCER_NAME.getLeft(), ACTUAL_EDGE_BALANCER_NAME.getRight());
        return actualEdgeBalancers.get(name);
    }

    @Nonnull
    @Override
    public BalanceService getActualBalancer(@Nonnull NodeType nodeType) {
        switch (nodeType) {
            case GATEWAY:
                return getActualGatewayBalancer();
            case RELAY:
                return getActualRelayBalancer();
            case EDGE:
                return getActualEdgeBalancer();
            default:
                throw new IllegalArgumentException("Unknown type: " + nodeType);
        }
    }
}
