package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.NoKafkaBaseTest;
import com.tango.stream.manager.dao.BroadcasterStatHistoryDao;
import com.tango.stream.manager.model.ViewerCountStatsHistory;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SqlBroadcasterStatHistoryDaoTest extends NoKafkaBaseTest {
    @Autowired
    protected BroadcasterStatHistoryDao statHistoryDao;

    @Test
    void shouldCalcWeekdayAverageCorrectly() {
        int percentile = 50;
        long accountId = RandomUtils.nextLong();
        String region = "eu";

        statHistoryDao.saveStats(Arrays.asList(
                ViewerCountStatsHistory.builder()
                        .accountId(accountId)
                        .percentile(percentile)
                        .region(region)
                        .value(1)
                        .timestamp(Instant.now())
                        .build(),

                ViewerCountStatsHistory.builder()
                        .accountId(accountId)
                        .percentile(percentile)
                        .region(region)
                        .value(100)
                        .timestamp(Instant.now().minus(Duration.ofDays(7)))
                        .build(),

                ViewerCountStatsHistory.builder()
                        .accountId(accountId)
                        .percentile(percentile)
                        .region(region)
                        .value(Integer.MAX_VALUE)
                        .timestamp(Instant.now().minus(Duration.ofDays(1)))
                        .build()
        ));

        Map<String, Integer> stats = statHistoryDao.getAverageByWeekday(accountId, percentile, Instant.now().minus(Duration.ofDays(100)), LocalDate.now().getDayOfWeek());

        assertFalse(stats.isEmpty());
        assertTrue(stats.containsKey(region));
        assertEquals(50, stats.get(region));
    }


    @Test
    void shouldCalcFromDateAverageCorrectly() {
        int percentile = 50;
        long accountId = RandomUtils.nextLong();
        String region = "eu";

        statHistoryDao.saveStats(Arrays.asList(
                ViewerCountStatsHistory.builder()
                        .accountId(accountId)
                        .percentile(percentile)
                        .region(region)
                        .value(1)
                        .timestamp(Instant.now())
                        .build(),

                ViewerCountStatsHistory.builder()
                        .accountId(accountId)
                        .percentile(percentile)
                        .region(region)
                        .value(100)
                        .timestamp(Instant.now().minus(Duration.ofDays(1)))
                        .build(),

                ViewerCountStatsHistory.builder()
                        .accountId(accountId)
                        .percentile(percentile)
                        .region(region)
                        .value(Integer.MAX_VALUE)
                        .timestamp(Instant.now().minus(Duration.ofDays(10)))
                        .build()
        ));

        Map<String, Integer> stats = statHistoryDao.getAverageFromDate(accountId, percentile, Instant.now().minus(Duration.ofDays(2)));

        assertFalse(stats.isEmpty());
        assertTrue(stats.containsKey(region));
        assertEquals(50, stats.get(region));
    }

}