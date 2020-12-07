package com.tango.stream.manager.service.impl;

import com.tango.stream.manager.dao.StreamDao;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.service.BalanceService;
import com.tango.stream.manager.service.RelayBalanceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.util.*;

import static com.tango.stream.manager.conf.DaoConfig.RELAY_STREAM;

@Slf4j
@Service
public class RelayRegisterService extends EdgeOrRelayRegisterService {
    private final static NodeType RELAY_NODE_TYPE = NodeType.RELAY;

    @Autowired
    protected List<RelayBalanceService> balanceServices;
    @Autowired
    @Qualifier(RELAY_STREAM)
    protected StreamDao<StreamDescription> relayStreamDao;


    @Nonnull
    @Override
    protected StreamDao<StreamDescription> getStreamDao() {
        return relayStreamDao;
    }

    @Override
    @Nonnull
    protected List<BalanceService> getBalanceService() {
        return new ArrayList<>(balanceServices);
    }

    @Nonnull
    @Override
    protected BalanceService getActualBalancer() {
        return balanceManagerService.getActualRelayBalancer();
    }

    @Override
    protected void handleClients(@Nonnull NodeData newNodeData, @Nonnull Map<String, StreamData> activeStreams, @Nonnull Set<String> knownStreams) {
    }

    @Override
    protected void removeLostViewers(@Nonnull Collection<NodeDescription> nodeCluster) {
    }

    @Nonnull
    @Override
    public NodeType nodeType() {
        return RELAY_NODE_TYPE;
    }
}
