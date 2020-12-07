package com.tango.stream.manager.utils;

import com.tango.stream.manager.model.NodeType;

import javax.annotation.Nonnull;

public class NamedLocksHelper {
    private static final String DELIMITER = ".";
    private static final String NODE_BALANCE_LIST_LOCAL_LOCK_PREFIX = "NODE_BALANCE_LIST.";
    private static final String STREAM_LOCAL_LOCK_PREFIX = "STREAM.";

    @Nonnull
    public static String getNodeClusterLockName(@Nonnull String region, @Nonnull NodeType nodeType, @Nonnull String version) {
        return NODE_BALANCE_LIST_LOCAL_LOCK_PREFIX + String.join(DELIMITER, region, nodeType.name(), version);
    }

    @Nonnull
    public static String getStreamLockName(@Nonnull NodeType nodeType, @Nonnull String region, @Nonnull String encryptedStreamKey) {
        return STREAM_LOCAL_LOCK_PREFIX + String.join(DELIMITER, nodeType.name(), region, encryptedStreamKey);
    }
}
