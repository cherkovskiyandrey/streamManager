package com.tango.stream.manager.model;

import com.google.common.collect.Lists;
import com.tango.stream.manager.model.commands.Command;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder
public class ReleaseClients {
    @Nonnull
    String encryptedStreamKey;
    @Nonnull
    NodeDescription nodeDescription;
    @Nullable
    NodeDescription parentNode;
    boolean isStreamReleased;
    @Nonnull
    @Builder.Default
    List<Command> commands = Lists.newArrayList();
}
