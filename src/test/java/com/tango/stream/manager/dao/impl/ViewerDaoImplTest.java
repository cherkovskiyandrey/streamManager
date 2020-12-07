package com.tango.stream.manager.dao.impl;

import com.google.common.collect.Multimap;
import com.tango.stream.manager.BaseKafkaTest;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.NodeType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ViewerDaoImplTest extends BaseKafkaTest {
    private static final NodeDescription nodeDescription1 = NodeDescription.builder()
            .nodeUid(UUID.randomUUID().toString())
            .extIp("extIp")
            .nodeType(NodeType.RELAY)
            .region("region")
            .version("version")
            .build();
    private static final NodeDescription nodeDescription2 = NodeDescription.builder()
            .nodeUid(UUID.randomUUID().toString())
            .extIp("extIp2")
            .nodeType(NodeType.EDGE)
            .region("region")
            .version("version2")
            .build();

    private static final ViewerData data1 = new ViewerData("key1", "value1");
    private static final ViewerData data2 = new ViewerData("key2", "value2");

    private static final String streamKey1 = "streamKey1";
    private static final String streamKey2 = "streamKey2";


    private ViewerDaoImpl<ViewerData> dao;

    @BeforeEach
    void setUp() {
        dao = new ViewerDaoImpl<>("viewerDao" + System.currentTimeMillis(), redissonClient, configurationService);
    }

    @Test
    void getSetDelete() {
        dao.addViewer(nodeDescription1, streamKey1, data1);
        assertTrue(dao.isViewerExists(nodeDescription1, streamKey1, data1));
        assertEquals(1, dao.getAllViewersSize(nodeDescription1));
        assertEquals(0, dao.getAllViewersSize(nodeDescription2));
        assertEquals(1, dao.getAllViewersSize(nodeDescription1, streamKey1));
        assertEquals(0, dao.getAllViewersSize(nodeDescription1, streamKey2));
        assertEquals(Sets.newHashSet(Collections.singleton(streamKey1)), Sets.newHashSet(dao.getAllKnownStreams(nodeDescription1)));
        assertEquals(data1, dao.getNodeAllViewers(nodeDescription1).get(streamKey1).iterator().next());

        dao.addViewer(nodeDescription1, streamKey1, data2);
        dao.addViewer(nodeDescription1, streamKey2, data1);
        dao.addViewer(nodeDescription2, streamKey2, data2);

        assertEquals(3, dao.getAllViewersSize(nodeDescription1));
        assertEquals(1, dao.getAllViewersSize(nodeDescription2));
        assertEquals(2, dao.getAllViewersSize(nodeDescription1, streamKey1));
        assertEquals(1, dao.getAllViewersSize(nodeDescription1, streamKey2));
        assertEquals(1, dao.getAllViewersSize(nodeDescription2, streamKey2));
        assertEquals(0, dao.getAllViewersSize(nodeDescription2, streamKey1));
        assertEquals(Sets.newHashSet(Arrays.asList(streamKey1, streamKey2)), Sets.newHashSet(dao.getAllKnownStreams(nodeDescription1)));
        Multimap<String, ViewerData> node1AllViewers = dao.getNodeAllViewers(nodeDescription1);
        assertEquals(Sets.newHashSet(Arrays.asList(data1, data2)), Sets.newHashSet(node1AllViewers.get(streamKey1)));
        assertEquals(Sets.newHashSet(Collections.singletonList(data1)), Sets.newHashSet(node1AllViewers.get(streamKey2)));

        Multimap<String, ViewerData> node2AllViewers = dao.getNodeAllViewers(nodeDescription2);
        assertEquals(data2, node2AllViewers.get(streamKey2).iterator().next());

        dao.removeAllViewers(nodeDescription1, streamKey1);
        assertEquals(1, dao.getAllViewersSize(nodeDescription1));

        dao.removeAllViewers(nodeDescription1, streamKey2);
        assertEquals(0, dao.getAllViewersSize(nodeDescription1));

        dao.removeAllViewers(nodeDescription2);
        assertEquals(0, dao.getAllViewersSize(nodeDescription2));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class ViewerData {
        private String key;
        private String value;
    }
}