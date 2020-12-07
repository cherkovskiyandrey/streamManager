package com.tango.stream.manager.dao.impl;

import com.google.common.collect.ImmutableMap;
import com.tango.stream.manager.dao.BroadcasterStatHistoryDao;
import com.tango.stream.manager.model.ViewerCountStatsHistory;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Repository
public class SqlBroadcasterStatHistoryDao implements BroadcasterStatHistoryDao {
    @Language("MySQL")
    public static final String INSERT_SQL = "" +
            "INSERT INTO viewers_stats (account_id, region, percentile, value, created) " +
            "VALUES (:accId, :region, :percentile, :value, :created);";

    @Language("MySQL")
    public static final String AVG_BY_WEEKDAY_SQL = "" +
            "SELECT avg(value) as value, region " +
            "FROM viewers_stats " +
            "WHERE account_id = :accId " +
            "AND created >= :fromDate " +
            "AND WEEKDAY(created) = :weekday " +
            "AND percentile = :percentile " +
            "GROUP BY region;";

    @Language("MySQL")
    public static final String AVG_FROM_DATE_SQL = "" +
            "SELECT avg(value) as value, region " +
            "FROM viewers_stats " +
            "WHERE account_id = :accId " +
            "AND created >= :fromDate " +
            "AND percentile = :percentile " +
            "GROUP BY region;";

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public SqlBroadcasterStatHistoryDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void saveStats(List<ViewerCountStatsHistory> stats) {
        @SuppressWarnings("unchecked")
        Map<String, Object>[] batchParams = stats.stream()
                .map(entry -> ImmutableMap.<String, Object>builder()
                        .put("accId", entry.getAccountId())
                        .put("region", entry.getRegion())
                        .put("percentile", entry.getPercentile())
                        .put("value", entry.getValue())
                        .put("created", instantToTimestamp(entry.getTimestamp()))
                        .build()
                )
                .toArray(Map[]::new);

        jdbcTemplate.batchUpdate(INSERT_SQL, batchParams);
    }

    @Override
    public Map<String, Integer> getAverageFromDate(long accountId, int percentile, Instant fromDate) {
        Map<String, ?> params = ImmutableMap.<String, Object>builder()
                .put("accId", accountId)
                .put("percentile", percentile)
                .put("fromDate", instantToTimestamp(fromDate))
                .build();
        return jdbcTemplate.queryForList(AVG_FROM_DATE_SQL, params).stream()
                .collect(Collectors.toMap(
                        r -> (String) r.get("region"),
                        r -> ((Number) r.get("value")).intValue()
                ));
    }

    @Override
    public Map<String, Integer> getAverageByWeekday(long accountId, int percentile, Instant fromDate, DayOfWeek dayOfWeek) {
        Map<String, ?> params = ImmutableMap.<String, Object>builder()
                .put("accId", accountId)
                .put("percentile", percentile)
                .put("fromDate", instantToTimestamp(fromDate))
                .put("weekday", mapWeekday(dayOfWeek))
                .build();
        return jdbcTemplate.queryForList(AVG_BY_WEEKDAY_SQL, params).stream()
                .collect(Collectors.toMap(
                        r -> (String) r.get("region"),
                        r -> ((Number) r.get("value")).intValue()
                ));
    }

    private int mapWeekday(DayOfWeek dayOfWeek) {
        return dayOfWeek.getValue() - 1;
    }

    @Nullable
    private Timestamp instantToTimestamp(Instant instant) {
        return instant == null
                ? null
                : Timestamp.from(instant);
    }


}
