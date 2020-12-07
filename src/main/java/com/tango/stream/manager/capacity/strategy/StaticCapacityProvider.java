package com.tango.stream.manager.capacity.strategy;

import com.tango.stream.manager.capacity.InitialViewersCapacityProvider;
import com.tango.stream.manager.capacity.InitialViewersCapacityStrategy;
import com.tango.stream.manager.dao.LiveRegionDao;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.model.ViewersCapacity;
import com.tango.stream.manager.service.impl.ConfigurationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.tango.stream.manager.capacity.InitialViewersCapacityStrategy.STATIC;

@Service
public class StaticCapacityProvider implements InitialViewersCapacityProvider {
    public static final String COUNT_PROP = "stats.viewer.strategy.static.count";

    private final ConfigurationService configurationService;
    private final LiveRegionDao liveRegionDao;

    @Autowired
    public StaticCapacityProvider(ConfigurationService configurationService,
                                  LiveRegionDao liveRegionDao) {
        this.configurationService = configurationService;
        this.liveRegionDao = liveRegionDao;
    }

    @Override
    public InitialViewersCapacityStrategy getStrategy() {
        return STATIC;
    }

    @Override
    public ViewersCapacity getInitialViewersCapacity(long accountId) {
        Map<String, Integer> capacities = liveRegionDao.getAllRegions(NodeType.EDGE).stream()
                .collect(Collectors.toMap(Function.identity(), this::forRegion));
        return ViewersCapacity.builder()
                .regionToViewers(capacities)
                .build();
    }

    private int forRegion(String region) {
        Integer regionalCount = configurationService.get().getInteger(COUNT_PROP + "." + region.toLowerCase(), null);
        return regionalCount == null
                ? getDefaultCount()
                : regionalCount;
    }

    private int getDefaultCount() {
        return configurationService.get().getInt(COUNT_PROP, 100);
    }
}
