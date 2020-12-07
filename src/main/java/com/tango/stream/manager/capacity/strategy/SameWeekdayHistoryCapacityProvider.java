package com.tango.stream.manager.capacity.strategy;

import com.tango.stream.manager.capacity.InitialViewersCapacityProvider;
import com.tango.stream.manager.capacity.InitialViewersCapacityStrategy;
import com.tango.stream.manager.dao.BroadcasterStatHistoryDao;
import com.tango.stream.manager.model.ViewersCapacity;
import com.tango.stream.manager.service.impl.ConfigurationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

import static com.tango.stream.manager.capacity.InitialViewersCapacityStrategy.SAME_WEEKDAY_HISTORY;

@Service
public class SameWeekdayHistoryCapacityProvider implements InitialViewersCapacityProvider {
    private final ConfigurationService configurationService;
    private final BroadcasterStatHistoryDao statHistoryDao;

    @Autowired
    public SameWeekdayHistoryCapacityProvider(ConfigurationService configurationService,
                                              BroadcasterStatHistoryDao statHistoryDao) {
        this.configurationService = configurationService;
        this.statHistoryDao = statHistoryDao;
    }

    @Override
    public InitialViewersCapacityStrategy getStrategy() {
        return SAME_WEEKDAY_HISTORY;
    }

    @Override
    public ViewersCapacity getInitialViewersCapacity(long accountId) {
        int percentile = getTargetPercentile();
        Instant fromDate = getFromDate();
        DayOfWeek dayOfWeek = LocalDate.now().getDayOfWeek();
        Map<String, Integer> stats = statHistoryDao.getAverageByWeekday(accountId, percentile, fromDate, dayOfWeek);
        return ViewersCapacity.builder()
                .regionToViewers(stats)
                .build();
    }

    private Instant getFromDate() {
        int weeksCount = configurationService.get().getInt("stats.viewer.strategy.same-weekday-history.weeks.count");
        return Instant.now().minus(Duration.ofDays(7L * weeksCount + 1));
    }

    private int getTargetPercentile() {
        return configurationService.get().getInt("stats.viewer.strategy.same-weekday-history.percentile");
    }
}
