package com.tango.stream.manager.controllers;

import com.tango.stream.manager.dao.NodeDao;
import com.tango.stream.manager.model.KeepAliveRequest;
import com.tango.stream.manager.model.KeepAliveResponse;
import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.service.RegistryManagerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Nonnull;

@Slf4j
@RestController
@RequestMapping("/node/")
@RequiredArgsConstructor
public class NodeController {
    private final RegistryManagerService registryManagerService;
    private final NodeDao nodeDao;

    @PostMapping("keepAlive")
    @Nonnull
    public KeepAliveResponse keepAlive(@RequestBody KeepAliveRequest keepAliveRequest) throws Exception {
        if (keepAliveRequest == null) {
            throw new IllegalArgumentException();
        }
        NodeType nodeType = keepAliveRequest.getNodeType();
        if (nodeType == NodeType.UNKNOWN) {
            log.warn("Received keepAlive request from UNKNOWN node type: {}", keepAliveRequest);
            return new KeepAliveResponse(false);
        }
        registryManagerService.getNodeRegistryService(nodeType).onKeepAlive(keepAliveRequest);
        return new KeepAliveResponse(true);
    }

    @GetMapping("unregister/node/{uid}")
    public void unregister(@PathVariable String uid) {
        if (StringUtils.isBlank(uid)) {
            throw new IllegalArgumentException();
        }
        NodeData nodeData = nodeDao.getNodeById(uid);
        if (nodeData == null) {
            log.warn("Receive unregister request from unknown node: {}", uid);
            return;
        }
        registryManagerService.getNodeRegistryService(nodeData.getNodeType()).unregister(uid);
    }
}
