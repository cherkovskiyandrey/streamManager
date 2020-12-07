package com.tango.stream.manager.dao;

import com.tango.stream.manager.model.ViewerCountStatsHistory;

import java.time.DayOfWeek;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public interface BroadcasterStatHistoryDao {
    void saveStats(List<ViewerCountStatsHistory> entry);

    Map<String, Integer> getAverageFromDate(long accountId, int percentile, Instant fromDate);

    Map<String, Integer> getAverageByWeekday(long accountId, int percentile, Instant fromDate, DayOfWeek dayOfWeek);
}
