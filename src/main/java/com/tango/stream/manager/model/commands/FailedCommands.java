package com.tango.stream.manager.model.commands;

import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;

import javax.annotation.Nonnull;
import java.util.Map;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@NoArgsConstructor
public class FailedCommands {
    @Nonnull
    @NonNull
    Map<Command, String> commandToReason = Maps.newHashMap();
}
