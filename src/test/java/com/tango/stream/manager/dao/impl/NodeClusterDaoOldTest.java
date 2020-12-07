package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.NoKafkaBaseTest;
import com.tango.stream.manager.model.NodeType;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.tango.stream.manager.service.impl.VersionServiceImpl.DEFAULT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

//todo: rewrite
public class NodeClusterDaoOldTest extends NoKafkaBaseTest {

//    @Autowired
//    private NodeClusterDaoOld nodeClusterDaoOld;
//    @Autowired
//    private RedissonClient redissonClient;
//
//    @Test
//    public void shouldStoreSuccessfully() {
//        NodeCluster nodeCluster = NodeCluster.builder()
//                .nodeType(NodeType.GATEWAY)
//                .region(DEFAULT_REGION)
//                .version("default")
//                .nodeBalanceItems(Collections.singletonMap("1", NodeBalanceItem.builder()
//                        .uid("1")
//                        .scores(Collections.singletonMap("SOME_BALANCER", 1L))
//                        .pendingStreams(Collections.singletonList(PendingStream.builder()
//                                .encryptedStreamKey("1")
//                                .balanceTime(100)
//                                .build()))
//                        .build()))
//                .build();
//
//        nodeClusterDaoOld.save(nodeCluster);
//        NodeCluster readNodeCluster = nodeClusterDaoOld.getCluster(DEFAULT_REGION, NodeType.GATEWAY, DEFAULT_VERSION);
//        assertEquals(nodeCluster, readNodeCluster);
//    }
//
//    @Test
//    public void shouldDeserializeCorrectlyWhenPresentDeprecatedItemVersion() {
//        String data = "{\"@class\":\"com.tango.stream.manager.model.NodeCluster\"," +
//                "\"itemVersion\":0," + //deprecated
//                "\"nodeBalanceItems\":{\"@class\":\"java.util.Collections$SingletonMap\",\"1\":{\"@class\":\"com.tango.stream.manager.model.NodeBalanceItem\",\"pendingStreams\":[\"java.util.Collections$SingletonList\",[{\"@class\":\"com.tango.stream.manager.model.PendingStream\",\"balanceTime\":100,\"encryptedStreamKey\":\"1\"}]],\"scores\":{\"@class\":\"java.util.Collections$SingletonMap\",\"SOME_BALANCER\":[\"java.lang.Long\",1]},\"uid\":\"1\"}},\"nodeType\":\"GATEWAY\",\"region\":\"default\",\"version\":\"default\"}";
//        RBucket<String> bucket = redissonClient.getBucket("SRT_NODE_CLUSTER:default:GATEWAY:default", StringCodec.INSTANCE);
//        bucket.set(data, 1, TimeUnit.MINUTES);
//
//        NodeCluster readNodeCluster = nodeClusterDaoOld.getCluster(DEFAULT_REGION, NodeType.GATEWAY, DEFAULT_VERSION);
//        assertThat(readNodeCluster).isNotNull();
//    }
}