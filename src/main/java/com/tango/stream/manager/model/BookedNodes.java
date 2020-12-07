package com.tango.stream.manager.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.tango.stream.manager.model.commands.Command;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder
public class BookedNodes {
    @Nonnull
    @NonNull
    String region;

    @Nonnull
    @NonNull
    @Builder.Default
    List<BookedNode> bookedNodes = Lists.newArrayList();

    @Nonnull
    @NonNull
    @Builder.Default
    List<BookedNode> linkedNodes = Lists.newArrayList();

    //extIp -> list of commands
    @Nonnull
    @NonNull
    @Builder.Default
    Multimap<NodeDescription, Command> commands = MultimapBuilder.linkedHashKeys().linkedListValues().build();

    @Nonnull
    public static BookedNodes empty() {
        return BookedNodes.builder().build();
    }
}
