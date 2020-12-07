package com.tango.stream.manager.controllers;

import com.tango.stream.manager.model.AvailableEdgeRequest;
import com.tango.stream.manager.model.AvailableEdgeResponse;
import com.tango.stream.manager.model.AvailableGatewayRequest;
import com.tango.stream.manager.model.AvailableGatewayResponse;
import com.tango.stream.manager.service.TopologyManagerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/manager/")
@RequiredArgsConstructor
public class ManageController {
    private final TopologyManagerService topologyManagerService;

    @PostMapping("availableGateway")
    public AvailableGatewayResponse getAvailableGateway(@RequestBody AvailableGatewayRequest request) throws Exception {
        if (StringUtils.isBlank(request.getEncryptedStreamKey())) {
            throw new IllegalArgumentException("empty key");
        }
        return topologyManagerService.getAvailableGateway(request);
    }

    @PostMapping("availableEdge")
    public AvailableEdgeResponse getAvailableEdge(@RequestBody AvailableEdgeRequest request) throws Exception {
        if (StringUtils.isBlank(request.getEncryptedStreamKey())) {
            throw new IllegalArgumentException("empty key");
        }
        return topologyManagerService.getAvailableEdge(request);
    }
}
