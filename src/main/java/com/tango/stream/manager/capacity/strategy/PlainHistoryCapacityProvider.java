package com.tango.stream.manager.capacity.strategy;

import com.tango.stream.manager.capacity.InitialViewersCapacityProvider;
import com.tango.stream.manager.capacity.InitialViewersCapacityStrategy;
import com.tango.stream.manager.dao.BroadcasterStatHistoryDao;
import com.tango.stream.manager.model.ViewersCapacity;
import com.tango.stream.manager.service.impl.ConfigurationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static com.tango.stream.manager.capacity.InitialViewersCapacityStrategy.PLAIN_HISTORY;

@Service
public class PlainHistoryCapacityProvider implements InitialViewersCapacityProvider {
    private final ConfigurationService configurationService;
    private final BroadcasterStatHistoryDao statHistoryDao;

    @Autowired
    public PlainHistoryCapacityProvider(ConfigurationService configurationService,
                                        BroadcasterStatHistoryDao statHistoryDao) {
        this.configurationService = configurationService;
        this.statHistoryDao = statHistoryDao;
    }

    @Override
    public InitialViewersCapacityStrategy getStrategy() {
        return PLAIN_HISTORY;
    }

    @Override
    public ViewersCapacity getInitialViewersCapacity(long accountId) {
        int percentile = getTargetPercentile();
        Instant fromDate = getFromDate();
        Map<String, Integer> stats = statHistoryDao.getAverageFromDate(accountId, percentile, fromDate);
        return ViewersCapacity.builder()
                .regionToViewers(stats)
                .build();
    }

    private Instant getFromDate() {
        int daysCount = configurationService.get().getInt("stats.viewer.strategy.plain-history.days.count");
        return Instant.now().minus(Duration.ofDays(daysCount));
    }

    private int getTargetPercentile() {
        return configurationService.get().getInt("stats.viewer.strategy.plain-history.percentile");
    }
}
