package com.tango.stream.manager.service;

import com.tango.stream.manager.model.AvailableEdgeRequest;
import com.tango.stream.manager.model.AvailableEdgeResponse;

import javax.annotation.Nonnull;

public interface EdgeBalanceService extends EdgeOrRelayBalanceService {
    @Nonnull
    AvailableEdgeResponse getAvailableEdge(@Nonnull AvailableEdgeRequest request) throws Exception;
}
