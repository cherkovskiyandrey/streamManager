package com.tango.stream.manager.restClients;

import com.tango.stream.manager.model.commands.Commands;
import com.tango.stream.manager.model.commands.FailedCommands;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;

import java.net.URI;

@FeignClient(name = "stream-manager")
public interface NodeCommandController {
    @PostMapping("/command/execute")
    FailedCommands postCommand(URI targetNode, Commands commands);
}
