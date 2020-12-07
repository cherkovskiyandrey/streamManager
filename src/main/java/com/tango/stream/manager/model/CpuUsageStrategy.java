package com.tango.stream.manager.model;

import javax.annotation.Nonnull;
import java.util.Arrays;

public enum CpuUsageStrategy {
    AVERAGE,
    MEDIAN;

    public static CpuUsageStrategy fromStr(@Nonnull String str) {
        return Arrays.stream(values()).filter(s -> str.equalsIgnoreCase(s.name())).findFirst().orElse(AVERAGE);
    }
}
