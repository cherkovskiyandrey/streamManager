package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.BaseKafkaTest;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.util.TestClock;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpiringStreamKeyDaoImplTest extends BaseKafkaTest {

    private static final NodeDescription nodeDescription1 = NodeDescription.builder()
            .nodeUid(UUID.randomUUID().toString())
            .extIp("extIp")
            .nodeType(NodeType.RELAY)
            .region("region")
            .version("version")
            .build();

    private static final StreamTestData data1 = new StreamTestData("key1", "value1");
    private static final StreamTestData data2 = new StreamTestData("key2", "value2");
    private static final int EXPIRE_SEC = 10;

    private ExpiringStreamKeyDaoImpl<StreamTestData> dao;
    private TestClock testClock;

    @BeforeEach
    void setUp() {
        testClock = new TestClock(Clock.systemDefaultZone());
        dao = new ExpiringStreamKeyDaoImpl<>("expireStreamTestData" + System.currentTimeMillis(),
                redissonClient, configurationService, StreamTestData::getKey, testClock,
                configurationService -> EXPIRE_SEC);
    }

    @Test
    void expire() throws InterruptedException {
        testClock.setFixed(100);
        dao.addStream(nodeDescription1, data1);
        Thread.sleep(TimeUnit.SECONDS.toMillis(EXPIRE_SEC));
        testClock.setFixedPlusSeconds(EXPIRE_SEC);
        dao.addStream(nodeDescription1, data2);
        assertTrue(dao.getAndRemoveExpired(nodeDescription1).contains(data1.getKey()));
        assertEquals(1, dao.size(nodeDescription1));

        dao.removeAllStreams(nodeDescription1);
        assertTrue(dao.getAndRemoveExpired(nodeDescription1).isEmpty());
        assertEquals(0, dao.size(nodeDescription1));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class StreamTestData {
        private String key;
        private String value;
    }

}