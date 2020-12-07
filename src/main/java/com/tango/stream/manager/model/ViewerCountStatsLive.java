package com.tango.stream.manager.model;

import lombok.Builder;
import lombok.Value;
import org.apache.commons.collections.MapUtils;

import java.util.Map;

@Value
@Builder
public class ViewerCountStatsLive {
    long accountId;
    Map<Long, Long> statistics;

    public boolean isEmpty() {
        return MapUtils.isEmpty(statistics);
    }
}
