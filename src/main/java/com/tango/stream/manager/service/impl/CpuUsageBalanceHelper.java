package com.tango.stream.manager.service.impl;

import com.tango.stream.manager.dao.NodeClusterDao;
import com.tango.stream.manager.dao.StreamDao;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.service.BalanceService;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Map;

public class CpuUsageBalanceHelper {
    public static final String NAME = "byCpuUsage";

    private final ConfigurationService configurationService;
    private final NodeClusterDao nodeClusterDao;
    private final StreamDao<StreamDescription> streamDao;
    private final Pair<String, Double> averageCpuUsageByOneStreamProp;
    private final Pair<String, Double> averageCpuUsageByOneViewer;
    private final String cpuUsageThreshold;
    private final Pair<String, String> cpuUsageStrategy;

    public CpuUsageBalanceHelper(@Nonnull ConfigurationService configurationService,
                                 @Nonnull NodeClusterDao nodeClusterDao,
                                 @Nonnull StreamDao<StreamDescription> streamDao,
                                 @Nonnull NodeType nodeType) {
        this.configurationService = configurationService;
        this.nodeClusterDao = nodeClusterDao;
        this.streamDao = streamDao;
        this.averageCpuUsageByOneStreamProp = Pair.of(String.format("balancer.%s.byCpuUsage.average.cpu.usage.byOneStream", nodeType.name().toLowerCase()), 0.2D);
        this.averageCpuUsageByOneViewer = Pair.of(String.format("balancer.%s.byCpuUsage.average.cpu.usage.byOneViewer", nodeType.name().toLowerCase()), 0.2D);
        this.cpuUsageThreshold = String.format("balancer.%s.byCpuUsage.cpu.usage.threshold", nodeType.name().toLowerCase());
        this.cpuUsageStrategy = Pair.of(String.format("balancer.%s.byCpuUsage.strategy", nodeType.name().toLowerCase()), CpuUsageStrategy.AVERAGE.name());
    }

    @Nonnull
    String getName() {
        return NAME;
    }

    void recalculateAndApplyScore(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams) {
        long newScore = calculateScore(nodeData, activeStreams);
        nodeClusterDao.addNodeToCluster(nodeData.toNodeDescription(), newScore);
    }

    private long calculateScore(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams) {
        return (long) ((
                getCpuUsage(nodeData) +
                        calculateInactiveStreams(nodeData, activeStreams) * getAverageCpuUsageByOneStream() +
                        calculateInactiveClients(nodeData, activeStreams) * getAverageCpuUsageByOneViewer()
        ) * 100);
    }

    private long calculateInactiveStreams(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams) {
        long allStreamsSize = streamDao.size(nodeData.toNodeDescription());
        long activeStreamsSize = activeStreams.size();
        long inactiveStreams = allStreamsSize - activeStreamsSize;
        return Math.max(inactiveStreams, 0);
    }

    private int calculateInactiveClients(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams) {
        int allViewers = streamDao.getAllStreams(nodeData.toNodeDescription()).stream()
                .mapToInt(StreamDescription::getMaxAvailableClients)
                .sum();
        int activeViewers = activeStreams.values().stream().mapToInt(StreamData::getViewersCount).sum();
        int inactiveViewers = allViewers - activeViewers;
        return Math.max(inactiveViewers, 0);
    }

    @Nonnull
    BalanceService.ScoreResult tryToAddClients(long currentScore, int requestedClients) {
        long restCapacity = getCpuUsageThreshold() - currentScore;
        long requestScores = (long) (requestedClients * getAverageCpuUsageByOneViewer() * 100);
        long possibleScoreToReserve = Math.min(requestScores, restCapacity);
        int possibleClientsToReserve = (int) (possibleScoreToReserve / (getAverageCpuUsageByOneViewer() * 100));
        return new BalanceService.ScoreResult(possibleScoreToReserve, possibleClientsToReserve);
    }

    @Nonnull
    BalanceService.ScoreResult tryToAddStreams(long currentScore, int requestedStreams) {
        long restCapacity = getCpuUsageThreshold() - currentScore;
        long requestScores = (long) (requestedStreams * getAverageCpuUsageByOneStream() * 100);
        long possibleScoreToReserve = Math.min(requestScores, restCapacity);
        int possibleClientsToReserve = (int) (possibleScoreToReserve / (getAverageCpuUsageByOneStream() * 100));
        return new BalanceService.ScoreResult(possibleScoreToReserve, possibleClientsToReserve);
    }

    long clientsToScore(int clients) {
        return (long) (clients * getAverageCpuUsageByOneViewer() * 100);
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
        return CpuUsageStrategy.fromStr(configurationService.get().getString(cpuUsageStrategy.getLeft(), cpuUsageStrategy.getRight()));
    }

    private Double getAverageCpuUsageByOneStream() {
        return configurationService.get().getDouble(averageCpuUsageByOneStreamProp.getLeft(), averageCpuUsageByOneStreamProp.getRight());
    }

    private Double getAverageCpuUsageByOneViewer() {
        return configurationService.get().getDouble(averageCpuUsageByOneViewer.getLeft(), averageCpuUsageByOneViewer.getRight());
    }

    private long getCpuUsageThreshold() {
        return configurationService.get().getLong(cpuUsageThreshold) * 100;
    }

    public long streamsToScore(int streams) {
        return (long) (streams * getAverageCpuUsageByOneStream() * 100);
    }
}
