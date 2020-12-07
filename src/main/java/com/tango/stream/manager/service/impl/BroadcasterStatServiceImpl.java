package com.tango.stream.manager.service.impl;

import com.google.common.math.Quantiles;
import com.tango.stream.manager.capacity.InitialViewersCapacityProvider;
import com.tango.stream.manager.capacity.InitialViewersCapacityStrategy;
import com.tango.stream.manager.dao.BroadcasterStatHistoryDao;
import com.tango.stream.manager.dao.LiveBroadcasterStatDao;
import com.tango.stream.manager.dao.LiveRegionDao;
import com.tango.stream.manager.dao.LiveStreamDao;
import com.tango.stream.manager.model.ViewerCountStatsHistory;
import com.tango.stream.manager.model.ViewerCountStatsLive;
import com.tango.stream.manager.model.ViewersCapacity;
import com.tango.stream.manager.service.BroadcasterStatService;
import com.tango.stream.manager.utils.EncryptUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.util.*;

import static com.tango.stream.manager.capacity.InitialViewersCapacityStrategy.STATIC;
import static com.tango.stream.manager.model.NodeType.EDGE;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@Service
@Slf4j
public class BroadcasterStatServiceImpl implements BroadcasterStatService {
    public static final String VIEWER_STRATEGY_PROP = "stats.viewer.strategy";
    public static final String PERCENTILES_PROP = "stats.viewer.store.percentiles";

    private final LiveStreamDao liveStreamDao;
    private final LiveBroadcasterStatDao liveBroadcasterStatDao;
    private final LiveRegionDao liveRegionDao;
    private final BroadcasterStatHistoryDao broadcasterStatHistoryDao;
    private final ConfigurationService configurationService;
    private final Map<InitialViewersCapacityStrategy, InitialViewersCapacityProvider> capacityProviders;
    private final InitialViewersCapacityProvider staticCapacityProvider;

    @Autowired
    public BroadcasterStatServiceImpl(LiveStreamDao liveStreamDao,
                                      LiveBroadcasterStatDao liveBroadcasterStatDao,
                                      LiveRegionDao liveRegionDao,
                                      BroadcasterStatHistoryDao broadcasterStatHistoryDao,
                                      ConfigurationService configurationService,
                                      List<InitialViewersCapacityProvider> capacityProvidersList) {
        this.liveStreamDao = liveStreamDao;
        this.liveBroadcasterStatDao = liveBroadcasterStatDao;
        this.liveRegionDao = liveRegionDao;
        this.broadcasterStatHistoryDao = broadcasterStatHistoryDao;
        this.configurationService = configurationService;
        this.capacityProviders = capacityProvidersList.stream()
                .collect(toMap(
                        InitialViewersCapacityProvider::getStrategy,
                        identity(),
                        (l, r) -> {
                            throw new IllegalArgumentException("Two implementations for strategy: " + l);
                        },
                        () -> new EnumMap<>(InitialViewersCapacityStrategy.class)
                ));
        this.staticCapacityProvider = capacityProviders.get(STATIC);
        if (this.staticCapacityProvider == null) {
            throw new RuntimeException("Static capacity provider required");
        }
    }

    @Override
    public void onAddViewer(@Nonnull String encryptedStreamKey, @Nonnull String viewerRegion, @Nonnull String viewerEncryptedId) {
        liveStreamDao.getStream(encryptedStreamKey)
                .ifPresent(stream -> liveBroadcasterStatDao.addViewer(stream, viewerRegion));
    }

    @Override
    public void onRemoveViewer(@Nonnull String encryptedStreamKey, @Nonnull String viewerRegion, @Nonnull String viewerEncryptedId) {
        liveStreamDao.getStream(encryptedStreamKey)
                .ifPresent(stream -> liveBroadcasterStatDao.removeViewer(stream, viewerRegion));
    }

    @Override
    public void onStreamTermination(String encryptedStreamKey) {
        Optional<Long> streamIdOp = EncryptUtil.getStreamId(encryptedStreamKey);
        if (!streamIdOp.isPresent()) {
            log.warn("Statistics won't be saved, could not decrypt stream id: {}", encryptedStreamKey);
            return;
        }
        long streamId = streamIdOp.get();
        List<String> regions = liveRegionDao.getAllRegions(EDGE);
        for (String region : regions) {
            Optional<ViewerCountStatsLive> stats = liveBroadcasterStatDao.fetchStats(streamId, region);
            stats.ifPresent(data -> recordStats(region, data));
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    private void recordStats(String region, ViewerCountStatsLive stats) {
        if (stats.isEmpty()) {
            return;
        }
        Collection<Long> observations = restoreSampledObservations(stats.getStatistics());
        Map<Integer, Double> percentiles = Quantiles.percentiles()
                .indexes(getPercentiles())
                .compute(observations);

        long accountId = stats.getAccountId();
        List<ViewerCountStatsHistory> statsHistory = percentiles.entrySet().stream()
                .map(entry -> percentileToHistory(accountId, region, entry.getKey(), entry.getValue().intValue()))
                .collect(toList());
        broadcasterStatHistoryDao.saveStats(statsHistory);
    }

    private ViewerCountStatsHistory percentileToHistory(long accountId, String region, Integer percentile, int count) {
        return ViewerCountStatsHistory.builder()
                .accountId(accountId)
                .region(region)
                .percentile(percentile)
                .value(count)
                .build();
    }

    private Collection<Long> restoreSampledObservations(Map<Long, Long> stats) {
        if (stats.size() == 1) {
            return stats.values();
        }

        List<Long> timeslots = new ArrayList<>(stats.keySet());
        Collections.sort(timeslots);

        List<Long> observations = new ArrayList<>();
        for (int i = 0; i < timeslots.size(); i++) {
            Long timeslot = timeslots.get(i);
            Long observation = stats.get(timeslot);
            observations.add(observation);

            if (i + 1 >= timeslots.size()) {
                continue;
            }

            //if there is gap between two consequent time slots larger than 1
            //then add last observed value to the result for each missing interval
            Long nextTimeslot = timeslots.get(i + 1);
            long missingObservationsCount = nextTimeslot - timeslot - 1;
            while (missingObservationsCount > 0) {
                observations.add(observation);
                missingObservationsCount--;
            }
        }

        return observations;
    }

    @Nonnull
    @Override
    public ViewersCapacity getInitialViewersCapacity(long accountId) {
        ViewersCapacity strategyCapacity = selectCapacityProvider().getInitialViewersCapacity(accountId);
        ViewersCapacity staticCapacity = staticCapacityProvider.getInitialViewersCapacity(accountId);

        strategyCapacity.addMissingRegions(staticCapacity.getRegionToViewers());
        return strategyCapacity;
    }

    private InitialViewersCapacityProvider selectCapacityProvider() {
        String strategyName = configurationService.get().getString(VIEWER_STRATEGY_PROP);
        Optional<InitialViewersCapacityStrategy> strategyOp = InitialViewersCapacityStrategy.byCode(strategyName);
        if (!strategyOp.isPresent()) {
            log.warn("Strategy not found for name [{}], static will be used", strategyName);
        }
        return strategyOp.map(capacityProviders::get)
                .orElse(staticCapacityProvider);
    }

    private List<Integer> getPercentiles() {
        return configurationService.get().getList(Integer.class, PERCENTILES_PROP, singletonList(50));
    }
}
