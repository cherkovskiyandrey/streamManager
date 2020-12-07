package com.tango.stream.manager.model;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;

public enum NodeType {
    GATEWAY,
    RELAY,
    EDGE,
    UNKNOWN;

    @JsonCreator
    public static NodeType byName(String name) {
        return Arrays.stream(NodeType.values())
                .filter(t -> t.name().equalsIgnoreCase(name))
                .findAny()
                .orElse(UNKNOWN);
    }
}
