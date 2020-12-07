package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.NoKafkaBaseTest;
import com.tango.stream.manager.dao.LiveRegionDao;
import com.tango.stream.manager.model.NodeType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

public class LiveRegionDaoTest extends NoKafkaBaseTest {
    @Autowired
    private LiveRegionDao liveRegionDao;

    @Test
    public void shouldStore() {
        liveRegionDao.addRegion(NodeType.GATEWAY, "default");
        liveRegionDao.addRegion(NodeType.GATEWAY, "eu");

        liveRegionDao.addRegion(NodeType.RELAY, "default");
        liveRegionDao.addRegion(NodeType.RELAY, "in");

        assertThat(liveRegionDao.getAllRegions(NodeType.GATEWAY)).containsOnly("default", "eu");
        assertThat(liveRegionDao.getAllRegions(NodeType.RELAY)).containsOnly("default", "in");
    }

}