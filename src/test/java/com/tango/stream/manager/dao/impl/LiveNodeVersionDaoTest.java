package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.NoKafkaBaseTest;
import com.tango.stream.manager.dao.LiveNodeVersionDao;
import com.tango.stream.manager.model.NodeType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

public class LiveNodeVersionDaoTest extends NoKafkaBaseTest {
    @Autowired
    private LiveNodeVersionDao liveNodeVersionDao;

    @Test
    public void shouldStore() {
        liveNodeVersionDao.addVersion(NodeType.GATEWAY, "default");
        liveNodeVersionDao.addVersion(NodeType.GATEWAY, "1");

        liveNodeVersionDao.addVersion(NodeType.RELAY, "default");
        liveNodeVersionDao.addVersion(NodeType.RELAY, "2");

        assertThat(liveNodeVersionDao.getAllVersions(NodeType.GATEWAY)).containsOnly("default", "1");
        assertThat(liveNodeVersionDao.getAllVersions(NodeType.RELAY)).containsOnly("default", "2");
    }
}