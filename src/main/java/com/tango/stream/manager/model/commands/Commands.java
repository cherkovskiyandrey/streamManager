package com.tango.stream.manager.model.commands;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;

import javax.annotation.Nonnull;
import java.util.List;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
public class Commands {
    @Nonnull
    @NonNull
    List<Command> sequentialCommands;
}