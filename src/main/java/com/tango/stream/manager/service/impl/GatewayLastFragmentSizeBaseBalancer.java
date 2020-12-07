package com.tango.stream.manager.service.impl;

import com.tango.stream.manager.dao.NodeClusterDao;
import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.StreamData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;

import java.util.Map;

import static com.tango.stream.manager.conf.DaoConfig.NODE_CLUSTER_DAO_FOR_GATEWAY_LAST_FRAGMENT_BALANCER;


@Slf4j
@Service
public class GatewayLastFragmentSizeBaseBalancer extends AbstractGatewayBalanceService {
    public static final String NAME = "byFragmentsBalancer";
    private static final Pair<String, Long> AVERAGE_FRAGMENT_SIZE_BYTES = Pair.of("balancer.gateway.fragmentBase.average.fragment.size.bytes.byOneStream", 3_000_000L);
    private static final Pair<String, Long> AVERAGE_FRAGMENT_SIZE_BYTES_BY_ONE_VIEWER = Pair.of("balancer.gateway.fragmentBase.average.fragment.size.bytes.byOneViewer", 3_000_000L);
    private static final String FRAGMENT_SIZE_THRESHOLD = "balancer.gateway.fragmentBase.fragment.size.threshold";

    @Autowired
    @Qualifier(NODE_CLUSTER_DAO_FOR_GATEWAY_LAST_FRAGMENT_BALANCER)
    private NodeClusterDao nodeClusterDao;

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Nonnull
    @Override
    public NodeClusterDao getClusterDao() {
        return nodeClusterDao;
    }


    @Override
    public void recalculateAndApplyScore(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams) {
        long newScore = calculateScore(nodeData, activeStreams);
        getClusterDao().addNodeToCluster(nodeData.toNodeDescription(), newScore);
    }

    private long calculateScore(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams) {
        long activeStreamsFragmentSizeInBytes = activeStreams.values().stream()
                .flatMapToLong(streamData -> streamData.getLastFragmentSizeInBytes().values().stream().mapToLong(l -> l))
                .sum();
        long activeViewerFragmentSizeInBytes = activeStreams.values().stream()
                //each viewer connection replicate all quality
                .mapToLong(streamData -> streamData.getLastFragmentSizeInBytes().values().stream().mapToLong(l -> l).sum() * streamData.getViewersCount())
                .sum();
        long inactiveStreamsFragmentSizeInBytes = calculateInactiveStreams(nodeData, activeStreams) * getAverageFragmentSizeBytes();
        long inactiveViewersFragmentsInBytes = calculateInactiveClients(nodeData, activeStreams) * getAverageFragmentSizeByOneViewer();

        return activeStreamsFragmentSizeInBytes + inactiveStreamsFragmentSizeInBytes + activeViewerFragmentSizeInBytes + inactiveViewersFragmentsInBytes;
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
        int reservedViewers = allViewers - activeViewers;
        return Math.max(reservedViewers, 0);
    }

    @Nonnull
    @Override
    public ScoreResult tryToAddClients(long currentScore, int requestedClients) {
        long restCapacity = getFragmentSizeThreshold() - currentScore;
        long requestScores = requestedClients * getAverageFragmentSizeByOneViewer() * 100;
        long possibleScoreToReserve = Math.min(requestScores, restCapacity);
        int possibleClientsToReserve = (int) (possibleScoreToReserve / (getAverageFragmentSizeByOneViewer() * 100));
        return new ScoreResult(possibleScoreToReserve, possibleClientsToReserve);
    }

    @Nonnull
    @Override
    public ScoreResult tryToAddStreams(long currentScore, int requestedStreams) {
        long restCapacity = getFragmentSizeThreshold() - currentScore;
        long requestScores = requestedStreams * getAverageFragmentSizeBytes() * 100;
        long possibleScoreToReserve = Math.min(requestScores, restCapacity);
        int possibleClientsToReserve = (int) (possibleScoreToReserve / (getAverageFragmentSizeBytes() * 100));
        return new ScoreResult(possibleScoreToReserve, possibleClientsToReserve);
    }

    @Override
    public long streamsToScore(int streams) {
        return streams * getAverageFragmentSizeBytes() * 100;
    }

    @Override
    public long clientsToScore(int clients) {
        return clients * getAverageFragmentSizeByOneViewer() * 100;
    }

    private long getFragmentSizeThreshold() {
        return configurationService.get().getLong(FRAGMENT_SIZE_THRESHOLD);
    }

    private long getAverageFragmentSizeBytes() {
        return configurationService.get().getLong(AVERAGE_FRAGMENT_SIZE_BYTES.getLeft(), AVERAGE_FRAGMENT_SIZE_BYTES.getRight());
    }

    private long getAverageFragmentSizeByOneViewer() {
        return configurationService.get().getLong(AVERAGE_FRAGMENT_SIZE_BYTES_BY_ONE_VIEWER.getLeft(), AVERAGE_FRAGMENT_SIZE_BYTES_BY_ONE_VIEWER.getRight());
    }
}
