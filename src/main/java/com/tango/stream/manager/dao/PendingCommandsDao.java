package com.tango.stream.manager.dao;

import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.commands.Command;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

public interface PendingCommandsDao {

    void addCommand(@Nonnull NodeDescription nodeDescription, @Nonnull Command command);

    void addCommands(NodeDescription nodeDescription, Collection<Command> commands);

    List<Command> get(String nodeUuid, int limit);

    void addFirst(String nodeUuid, List<Command> commands);

    void drain(NodeDescription nodeDescription);
}
