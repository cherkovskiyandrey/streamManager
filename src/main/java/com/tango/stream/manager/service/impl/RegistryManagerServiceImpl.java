package com.tango.stream.manager.service.impl;

import com.google.common.collect.ImmutableMap;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.service.NodeRegistryService;
import com.tango.stream.manager.service.RegistryManagerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.util.List;

import static java.lang.String.format;

@Slf4j
@Service
public class RegistryManagerServiceImpl implements RegistryManagerService {

    private final ImmutableMap<NodeType, NodeRegistryService> regServiceByType;

    public RegistryManagerServiceImpl(@Nonnull List<NodeRegistryService> nodeRegistryServices) {
        ImmutableMap.Builder<NodeType, NodeRegistryService> nodeTypeNodeRegistryServiceBuilder = ImmutableMap.builder();
        nodeRegistryServices.forEach(service -> nodeTypeNodeRegistryServiceBuilder.put(
                service.nodeType(),
                service
        ));
        this.regServiceByType = nodeTypeNodeRegistryServiceBuilder.build();
    }

    @Nonnull
    @Override
    public NodeRegistryService getNodeRegistryService(@Nonnull NodeType nodeType) {
        NodeRegistryService result = regServiceByType.get(nodeType);
        if (result == null) {
            log.error("Unsupported node type=[{}]", nodeType);
            throw new IllegalStateException(format("Unsupported node type=[%s]", nodeType));
        }
        return result;
    }
}
