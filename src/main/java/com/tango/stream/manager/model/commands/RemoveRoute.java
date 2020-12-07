package com.tango.stream.manager.model.commands;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.SuperBuilder;

@Value
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@NoArgsConstructor
public class RemoveRoute extends Command {
}
