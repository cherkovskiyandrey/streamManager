package com.tango.stream.manager.service.impl;

import com.tango.stream.manager.dao.LiveNodeVersionDao;
import com.tango.stream.manager.dao.LiveRegionDao;
import com.tango.stream.manager.dao.NodeDao;
import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.service.BalanceManagerService;
import com.tango.stream.manager.service.BalanceService;
import com.tango.stream.manager.service.NodeStatService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class NodeStatServiceImpl implements NodeStatService {
    private final LiveRegionDao liveRegionDao;
    private final LiveNodeVersionDao liveNodeVersionDao;
    private final NodeDao nodeDao;
    private final BalanceManagerService balanceManagerService;

    @Nonnull
    @Override
    public Map<String, Map<String, NodeData>> getNodeData(@Nonnull NodeType nodeType) {
        List<String> liveRegions = liveRegionDao.getAllRegions(nodeType);
        return liveRegions.stream()
                .collect(Collectors.toMap(Function.identity(), region -> getNodeData(nodeType, region)));
    }

    @Nonnull
    public Map<String, NodeData> getNodeData(@Nonnull NodeType nodeType, @Nonnull String region) {
        return liveNodeVersionDao.getAllVersions(nodeType).stream()
                .flatMap(v -> {
                    BalanceService actualBalancer = balanceManagerService.getActualBalancer(nodeType);
                    return actualBalancer.getClusterDao().getCluster(region, v).stream();
                })
                .map(NodeDescription::getNodeUid)
                .map(nodeDao::getNodeById)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(NodeData::getUid, Function.identity()))
                ;
    }

    @Nullable
    public NodeData getNodeData(@Nonnull String uid) {
        return nodeDao.getNodeById(uid);
    }
}
