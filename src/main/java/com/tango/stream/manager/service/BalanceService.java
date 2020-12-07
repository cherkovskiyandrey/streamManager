package com.tango.stream.manager.service;

import com.tango.stream.manager.dao.NodeClusterDao;
import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.StreamData;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Map;

public interface BalanceService {

    @Nonnull
    String getName();

    void recalculateAndApplyScore(@Nonnull NodeData nodeData, @Nonnull Map<String, StreamData> activeStreams);

    @Nonnull
    NodeClusterDao getClusterDao();

    @Nonnull
    ScoreResult tryToAddStreams(long currentScore, int requestedStreams);

    @Nonnull
    ScoreResult tryToAddClients(long currentScore, int requestedClients);

    long streamsToScore(int streams);

    long clientsToScore(int clients);

    @Value
    class ScoreResult {
        long newScoreDelta;
        int reservedClientPlaces;
    }
}
