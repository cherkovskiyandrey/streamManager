package com.tango.stream.manager.service;

import com.tango.stream.manager.model.NodeType;

import javax.annotation.Nonnull;

public interface BalanceManagerService {
    @Nonnull
    GatewayBalanceService getActualGatewayBalancer();

    @Nonnull
    RelayBalanceService getActualRelayBalancer();

    @Nonnull
    EdgeBalanceService getActualEdgeBalancer();

    @Nonnull
    BalanceService getActualBalancer(@Nonnull NodeType nodeType);
}
