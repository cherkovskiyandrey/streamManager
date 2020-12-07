package com.tango.stream.manager.model;

public interface Metrics {

    String BASE_METRIC_PREFIX = "srt.streamManager";

    interface Counters {
        String KEEP_ALIVE_COUNTER = m("keep.alive.counter");
        String KEEP_ALIVE_ERROR = m("keep.alive.error");

        String NEXT_AVAILABLE_NODE_OK = m("next.available.node.success");
        String NEXT_AVAILABLE_NODE_ERROR = m("next.available.node.error");

        String LOST_NODES = m("nodes.lost");

        String NODES_GS_COUNTER_OK = m("nodes.gc.ok");
        String NODES_GS_COUNTER_FAIL = m("nodes.gc.fail");

        String UNDER_LOCK_ERRORS = m("lock.error");
    }

    interface Timers {
        String BALANCE_OF_AVAILABLE_GATEWAY_OUTSIDE_TIME = m("balance.outside.time");
        String BALANCE_OF_AVAILABLE_GATEWAY_INSIDE_LOCAL_TIME = m("balance.inside.local.time");
        String BALANCE_OF_AVAILABLE_GATEWAY_INSIDE_REMOTE_TIME = m("balance.inside.remote.time");

        String KEEP_ALIVE_OUTSIDE_TIMER = m("keep.alive.outside.time");
        String KEEP_ALIVE_INSIDE_LOCAL_TIMER = m("keep.alive.inside.local.time");
        String KEEP_ALIVE_INSIDE_REMOTE_TIMER = m("keep.alive.inside.remote.time");

        String GC_OUTSIDE_TIMER = m("gc.outside.time");
        String GC_INSIDE_LOCAL_TIMER = m("gc.inside.local.time");
        String GC_INSIDE_REMOTE_TIMER = m("gc.inside.remote.time");
    }

    interface Tags {
        String NODE_TYPE = "node_type";
        String REGION = "region";
        String VERSION = "version";
        String ERROR_TYPE = "error_type";
        String NODE_UPDATE_TYPE = "node_update_type";
    }

    static String m(String metric) {
        return String.join(".", BASE_METRIC_PREFIX, metric);
    }
}
