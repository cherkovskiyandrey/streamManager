package com.tango.stream.manager.model.commands;

import com.tango.stream.manager.model.StreamRedirectData;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@NoArgsConstructor
public class RedirectClients extends Command  {
    StreamRedirectData redirectData;
}
