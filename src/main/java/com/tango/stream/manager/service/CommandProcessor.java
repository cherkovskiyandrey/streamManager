package com.tango.stream.manager.service;

import com.google.common.collect.Multimap;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.commands.Command;

import javax.annotation.Nonnull;

public interface CommandProcessor {
    void executePreserveOrderAsync(@Nonnull Multimap<NodeDescription, Command> commands);

    void cancelAllCommands(@Nonnull NodeDescription nodeDescription);
}
