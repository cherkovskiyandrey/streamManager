package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.dao.NodeClusterDao;
import com.tango.stream.manager.dao.NodeClusterDaoFactory;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.service.impl.ConfigurationService;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;

@Service
@RequiredArgsConstructor
public class NodeClusterDaoFactoryImpl implements NodeClusterDaoFactory {
    private final RedissonClient redissonClient;
    private final ConfigurationService configurationService;

    @Override
    @Nonnull
    public NodeClusterDao create(@Nonnull String name, @Nonnull NodeType nodeType) {
        return new NodeClusterDaoImpl(nodeType, name, redissonClient, configurationService);
    }
}
