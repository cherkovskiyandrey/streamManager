package com.tango.stream.manager.controllers;

import com.google.common.collect.ImmutableMap;
import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.service.NodeStatService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/nodeStat/")
@RequiredArgsConstructor
public class NodeStatController {
    private final NodeStatService nodeRegistry;

    @GetMapping("")
    public Map<NodeType, ?> allTypes() {
        return Arrays.stream(NodeType.values())
                .collect(Collectors.toMap(Function.identity(), nodeRegistry::getNodeData));
    }

    @GetMapping("type/{type}")
    public Map<String, ?> type(@PathVariable NodeType type) {
        if (type == null) {
            return ImmutableMap.of();
        }
        return nodeRegistry.getNodeData(type);
    }

    @GetMapping("type/{type}/region/{region}")
    public Map<String, NodeData> region(@PathVariable NodeType type, @PathVariable String region) {
        if (type == null || region == null) {
            return ImmutableMap.of();
        }
        return nodeRegistry.getNodeData(type, region);
    }

    @GetMapping("uid/{uid}")
    public NodeData node(@PathVariable String uid) {
        if (uid == null) {
            return null;
        }
        return nodeRegistry.getNodeData(uid);
    }
}
