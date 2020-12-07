package com.tango.stream.manager.capacity;

import lombok.Getter;

import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public enum InitialViewersCapacityStrategy {
    STATIC("static"),
    SAME_WEEKDAY_HISTORY("same-weekday-history"),
    PLAIN_HISTORY("plain-history");

    private static final List<InitialViewersCapacityStrategy> VALUES = unmodifiableList(asList(values()));

    @Getter
    private final String code;

    InitialViewersCapacityStrategy(String code) {
        this.code = code;
    }

    public static Optional<InitialViewersCapacityStrategy> byCode(String code) {
        return VALUES.stream()
                .filter(strategy -> strategy.code.equalsIgnoreCase(code))
                .findAny();
    }
}
