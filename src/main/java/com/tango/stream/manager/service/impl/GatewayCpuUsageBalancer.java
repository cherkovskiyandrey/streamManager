package com.tango.stream.manager.service.impl;

import com.tango.stream.manager.dao.NodeClusterDao;
import com.tango.stream.manager.model.CpuUsageStrategy;
import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.StreamData;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;

import java.util.Map;

import static com.tango.stream.manager.conf.DaoConfig.NODE_CLUSTER_DAO_FOR_GATEWAY_CPU_USAGE_BALANCER;

@Slf4j
@Service
public class GatewayCpuUsageBalancer extends AbstractGatewayBalanceService {
    public static final String NAME = "byCpuUsage";

    public static final Pair<String, Double> AVERAGE_CPU_USAGE_BY_ONE_STREAM = Pair.of("balancer.gateway.byCpuUsage.average.cpu.usage.byOneStream", 0.4D);
    public static final Pair<String, Double> AVERAGE_CPU_USAGE_BY_ONE_VIEWER = Pair.of("balancer.gateway.byCpuUsage.average.cpu.usage.byOneViewer", 0.2D);
    public static final Pair<String, String> CPU_USAGE_STRATEGY = Pair.of("balancer.gateway.byCpuUsage.strategy", CpuUsageStrategy.AVERAGE.name());
    private static final String CPU_USAGE_THRESHOLD = "balancer.gateway.byCpuUsage.cpu.usage.threshold";

    @Autowired
    @Qualifier(NODE_CLUSTER_DAO_FOR_GATEWAY_CPU_USAGE_BALANCER)
    private NodeClusterDao nodeClusterDao;

    @Override
    public void recalculateAndApplyScore(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams) {
        long newScore = calculateScore(nodeData, activeStreams);
        getClusterDao().addNodeToCluster(nodeData.toNodeDescription(), newScore);
    }

    @Nonnull
    @Override
    public NodeClusterDao getClusterDao() {
        return nodeClusterDao;
    }

    private long calculateScore(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams) {
        return (long) ((
                getCpuUsage(nodeData) +
                        calculateInactiveStreams(nodeData, activeStreams) * getAverageCpuUsageByOneStream() +
                        calculateInactiveClients(nodeData, activeStreams) * getAverageCpuUsageByOneViewer()
        ) * 100);
    }

    private long calculateInactiveStreams(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams) {
        long allStreams = streamDao.size(nodeData.toNodeDescription());
        long activeStreamsSize = activeStreams.size();
        long inactiveStreams = allStreams - activeStreamsSize;
        return Math.max(inactiveStreams, 0);
    }

    private int calculateInactiveClients(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams) {
        int allViewers = viewerDao.getAllViewersSize(nodeData.toNodeDescription());
        int activeViewers = activeStreams.values().stream().mapToInt(StreamData::getViewersCount).sum();
        int inactiveViewers = allViewers - activeViewers;
        return Math.max(inactiveViewers, 0);
    }

    private Double getCpuUsage(@NonNull NodeData nodeData) {
        switch (getActiveStrategy()) {
            case MEDIAN:
                return nodeData.getMedCpuUsage();
            case AVERAGE:
            default:
                return nodeData.getAvgCpuUsage();
        }
    }

    private CpuUsageStrategy getActiveStrategy() {
        return CpuUsageStrategy.fromStr(configurationService.get().getString(CPU_USAGE_STRATEGY.getLeft(), CPU_USAGE_STRATEGY.getRight()));
    }

    private long getCpuUsageThreshold() {
        return configurationService.get().getLong(CPU_USAGE_THRESHOLD) * 100;
    }

    private Double getAverageCpuUsageByOneStream() {
        return configurationService.get().getDouble(AVERAGE_CPU_USAGE_BY_ONE_STREAM.getLeft(), AVERAGE_CPU_USAGE_BY_ONE_STREAM.getRight());
    }

    private Double getAverageCpuUsageByOneViewer() {
        return configurationService.get().getDouble(AVERAGE_CPU_USAGE_BY_ONE_VIEWER.getLeft(), AVERAGE_CPU_USAGE_BY_ONE_VIEWER.getRight());
    }

    @Nonnull
    @Override
    public ScoreResult tryToAddClients(long currentScore, int requestedClients) {
        long restCapacity = getCpuUsageThreshold() - currentScore;
        long requestScores = (long) (requestedClients * getAverageCpuUsageByOneViewer() * 100);
        long possibleScoreToReserve = Math.min(requestScores, restCapacity);
        int possibleClientsToReserve = (int) (possibleScoreToReserve / (getAverageCpuUsageByOneViewer() * 100));
        return new ScoreResult(possibleScoreToReserve, possibleClientsToReserve);
    }


    @Nonnull
    @Override
    public ScoreResult tryToAddStreams(long currentScore, int requestedStreams) {
        long restCapacity = getCpuUsageThreshold() - currentScore;
        long requestScores = (long) (requestedStreams * getAverageCpuUsageByOneStream() * 100);
        long possibleScoreToReserve = Math.min(requestScores, restCapacity);
        int possibleClientsToReserve = (int) (possibleScoreToReserve / (getAverageCpuUsageByOneStream() * 100));
        return new ScoreResult(possibleScoreToReserve, possibleClientsToReserve);
    }

    @Override
    public long clientsToScore(int clients) {
        return (long) (clients * getAverageCpuUsageByOneViewer() * 100);
    }

    @Override
    public long streamsToScore(int streams) {
        return (long) (streams * getAverageCpuUsageByOneStream() * 100);
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }
}
