package com.tango.stream.manager.service.impl;

import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.service.VersionService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;


//TODO: change to implementation from http://jira.tango.corp/browse/ELF-1790
@Service
@RequiredArgsConstructor
public class VersionServiceImpl implements VersionService {
    public static final String DEFAULT_VERSION = "default";
    protected static final String ACTIVE_VERSION_PREFIX = "active.version.";
    private final ConfigurationService configurationService;

    @Nonnull
    @Override
    public String getActualVersion(@Nonnull NodeType nodeType, @Nonnull String region) {
        return configurationService.get().getString(ACTIVE_VERSION_PREFIX + nodeType.name().toLowerCase(), DEFAULT_VERSION);
    }
}
