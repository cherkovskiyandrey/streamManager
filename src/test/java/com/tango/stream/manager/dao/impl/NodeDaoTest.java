package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.NoKafkaBaseTest;
import com.tango.stream.manager.dao.NodeDao;
import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.NodeType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static com.tango.stream.manager.service.impl.VersionServiceImpl.DEFAULT_VERSION;
import static org.junit.jupiter.api.Assertions.*;

public class NodeDaoTest extends NoKafkaBaseTest {
    @Autowired
    private NodeDao nodeDao;

    @Test
    public void shouldStoreNodeDao() {
        NodeData nodeData = NodeData.builder()
                .uid("1")
                .extIp("1.1.1.1")
                .nodeType(NodeType.GATEWAY)
                .region(DEFAULT_REGION)
                .version(DEFAULT_VERSION)
                .avgCpuUsage(1.)
                .medCpuUsage(1.)
                .build();
        nodeDao.save(nodeData);
        assertTrue(nodeDao.isExist("1"));
        NodeData nodeById = nodeDao.getNodeById("1");
        assertEquals(nodeData, nodeById);
        nodeDao.remove(nodeData);
        assertFalse(nodeDao.isExist("1"));
    }

}