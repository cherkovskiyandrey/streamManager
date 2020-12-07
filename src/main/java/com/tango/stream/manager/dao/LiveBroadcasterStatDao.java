package com.tango.stream.manager.dao;

import com.tango.stream.manager.model.LiveStreamData;
import com.tango.stream.manager.model.ViewerCountStatsLive;

import java.util.Optional;

public interface LiveBroadcasterStatDao {
    void addViewer(LiveStreamData stream, String region);

    void removeViewer(LiveStreamData stream, String region);

    Optional<ViewerCountStatsLive> fetchStats(long streamId, String region);
}
