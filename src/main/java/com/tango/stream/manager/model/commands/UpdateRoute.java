package com.tango.stream.manager.model.commands;

import com.tango.stream.manager.model.StreamRouteData;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@NoArgsConstructor
public class UpdateRoute extends Command {
    StreamRouteData streamRouteData;
}
