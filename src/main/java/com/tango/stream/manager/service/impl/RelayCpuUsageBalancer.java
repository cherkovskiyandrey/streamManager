package com.tango.stream.manager.service.impl;

import com.tango.stream.manager.dao.NodeClusterDao;
import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.StreamData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;

import java.util.Map;

import static com.tango.stream.manager.conf.CommonConfig.RELAY_CPU_USAGE_BALANCE_HELPER;
import static com.tango.stream.manager.conf.DaoConfig.NODE_CLUSTER_DAO_FOR_RELAY_CPU_USAGE_BALANCER;

@Slf4j
@Service
public class RelayCpuUsageBalancer extends AbstractRelayBalanceService {
    public static final String NAME = CpuUsageBalanceHelper.NAME;

    @Autowired
    @Qualifier(NODE_CLUSTER_DAO_FOR_RELAY_CPU_USAGE_BALANCER)
    private NodeClusterDao nodeClusterDao;

    @Autowired
    @Qualifier(RELAY_CPU_USAGE_BALANCE_HELPER)
    private CpuUsageBalanceHelper cpuUsageBalanceHelper;

    @Nonnull
    @Override
    public String getName() {
        return cpuUsageBalanceHelper.getName();
    }

    @Override
    public void recalculateAndApplyScore(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams) {
        cpuUsageBalanceHelper.recalculateAndApplyScore(nodeData, activeStreams);
    }

    @Nonnull
    @Override
    public NodeClusterDao getClusterDao() {
        return nodeClusterDao;
    }

    @Nonnull
    @Override
    public ScoreResult tryToAddClients(long currentScore, int requestedClients) {
        return cpuUsageBalanceHelper.tryToAddClients(currentScore, requestedClients);
    }

    @Override
    public long streamsToScore(int streams) {
        return cpuUsageBalanceHelper.streamsToScore(streams);
    }

    @Nonnull
    @Override
    public ScoreResult tryToAddStreams(long currentScore, int requestedStreams) {
        return cpuUsageBalanceHelper.tryToAddStreams(currentScore, requestedStreams);
    }

    @Override
    public long clientsToScore(int clients) {
        return cpuUsageBalanceHelper.clientsToScore(clients);
    }
}
