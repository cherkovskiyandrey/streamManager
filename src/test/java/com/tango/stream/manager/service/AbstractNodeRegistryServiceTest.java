package com.tango.stream.manager.service;

import com.google.common.collect.ImmutableMap;
import com.tango.stream.manager.NoKafkaBaseTest;
import com.tango.stream.manager.model.KeepAliveRequest;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.model.StreamData;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static com.tango.stream.manager.service.impl.VersionServiceImpl.DEFAULT_VERSION;

public class AbstractNodeRegistryServiceTest extends NoKafkaBaseTest {
    @Autowired
    private NodeRegistryService nodeRegistryService;

    //todo
//    @Test
//    @Disabled
//    public void shouldUseMeterRegistry() throws Exception {
//        nodeRegistryService.onKeepAlive(
//                KeepAliveRequest.builder()
//                        .extIp("1.1.1.1")
//                        .region(DEFAULT_REGION)
//                        .version(DEFAULT_VERSION)
//                        .uid("123456")
//                        .nodeType(NodeType.GATEWAY)
//                        .activeStreams(ImmutableMap.of(
//                                "1",
//                                StreamData.builder()
//                                        .encryptedStreamKey("1")
//                                        .viewersCount(0)
//                                        .lastFragmentSizeInBytes(
//                                                ImmutableMap.of(
//                                                        "HD-123",
//                                                        10000L
//                                                )
//                                        )
//                                        .build()
//                        ))
//                        .build()
//        );
//    }
}
