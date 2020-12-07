package com.tango.stream.manager.service.impl;

import com.tango.stream.manager.dao.StreamDao;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.model.StreamData;
import com.tango.stream.manager.model.StreamDescription;
import com.tango.stream.manager.service.RelayBalanceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.Nonnull;

import static com.tango.stream.manager.conf.DaoConfig.RELAY_STREAM;

@Slf4j
public abstract class AbstractRelayBalanceService extends EdgeOrRelayBalanceServiceImpl implements RelayBalanceService {

    @Autowired
    @Qualifier(RELAY_STREAM)
    protected StreamDao<StreamDescription> relayStreamDao;

    @Nonnull
    @Override
    protected NodeType getNodeType() {
        return NodeType.RELAY;
    }

    @Override
    protected int toBookedClients(int clients) {
        return clients;
    }

    @Override
    protected StreamDao<StreamDescription> getStreamDao() {
        return relayStreamDao;
    }
}
