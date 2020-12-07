package com.tango.stream.manager.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Map;

@Value
@Builder
public class ViewersCapacity {
    @Nonnull
    @NonNull
    Map<String, Integer> regionToViewers;

    public void addMissingRegions(Map<String, Integer> additionalRegions) {
        additionalRegions.forEach(regionToViewers::putIfAbsent);
    }
}
