package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.BaseKafkaTest;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.NodeType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamKeyDaoImplTest extends BaseKafkaTest {

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

    private static final StreamTestData data1 = new StreamTestData("key1", "value1");
    private static final StreamTestData data2 = new StreamTestData("key2", "value2");

    private StreamKeyDaoImpl<StreamTestData> streamTestDataDao;

    @BeforeEach
    void setUp() {
        streamTestDataDao = new StreamKeyDaoImpl<>("streamTestData" + System.currentTimeMillis(),
                redissonClient, configurationService, StreamTestData::getKey);
    }

    @Test
    void getSetDelete() {
        streamTestDataDao.addStream(nodeDescription1, data1);
        assertEquals(Optional.of(data1), streamTestDataDao.findStream(nodeDescription1, data1.getKey()));
        assertTrue(streamTestDataDao.getAllStreams(nodeDescription1).contains(data1));


        streamTestDataDao.addStream(nodeDescription2, data1);
        streamTestDataDao.addStream(nodeDescription2, data2);
        Collection<NodeDescription> nodesByStream = streamTestDataDao.getNodesByStream(data1.getKey());
        assertTrue(nodesByStream.contains(nodeDescription1));
        assertTrue(nodesByStream.contains(nodeDescription2));

        assertEquals(2, streamTestDataDao.size(nodeDescription2));
        assertEquals(2, streamTestDataDao.getNodesByStreamInRegion(data1.getKey(), nodeDescription1.getRegion()).size());
        assertEquals(1, streamTestDataDao.getNodesByStreamInRegion(data2.getKey(), nodeDescription1.getRegion()).size());
        
        streamTestDataDao.removeStream(nodeDescription1, data1.getKey());
        assertEquals(0, streamTestDataDao.size(nodeDescription1));
        streamTestDataDao.removeAllStreams(nodeDescription2);
        assertEquals(0, streamTestDataDao.size(nodeDescription2));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class StreamTestData {
        private String key;
        private String value;
    }
}