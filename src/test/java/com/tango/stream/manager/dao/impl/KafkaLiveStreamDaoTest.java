package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.BaseKafkaTest;
import com.tango.stream.manager.dao.LiveStreamDao;
import com.tango.stream.manager.model.Stream;
import com.tango.stream.manager.model.StreamKey;
import com.tango.stream.manager.utils.EncryptUtil;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.awaitility.Awaitility.waitAtMost;

class KafkaLiveStreamDaoTest extends BaseKafkaTest {
    public static final Duration TIMEOUT = Duration.ofSeconds(10L);

    @Value("${stream.live.unfiltered.topic}")
    private String liveStreamsTopic;

    private KafkaTemplate<Long, Stream> kafkaTemplate;

    @Autowired
    private LiveStreamDao liveStreamDao;

    @BeforeEach
    void setUp() {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaServers);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerConfig.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        DefaultKafkaProducerFactory<Long, Stream> producerFactory = new DefaultKafkaProducerFactory<>(producerConfig);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    @Test
    public void shouldBuildLiveStreamsStoreCorrectly() {
        long streamId = 42L;
        kafkaTemplate.send(liveStreamsTopic, streamId, createLiveStream());

        assertStreamIsLive(streamId);

        kafkaTemplate.send(liveStreamsTopic, streamId, null);

        assertStreamIsNotLive(streamId);
    }

    private void assertStreamIsLive(long streamId) {
        String encryptedStreamId = encrypt(streamId);
        waitAtMost(TIMEOUT).until(() -> liveStreamDao.getStream(encryptedStreamId).isPresent());
    }

    private void assertStreamIsNotLive(long streamId) {
        String encryptedStreamId = encrypt(streamId);
        waitAtMost(TIMEOUT).until(() -> !liveStreamDao.getStream(encryptedStreamId).isPresent());
    }

    @SneakyThrows
    private String encrypt(long streamId) {
        return EncryptUtil.encryptStreamKey(StreamKey.builder()
                .streamId(streamId)
                .initTime(Instant.now().toEpochMilli())
                .build());
    }

    private Stream createLiveStream() {
        Stream stream = new Stream();
        stream.setStartTime(new Date());
        stream.setSource("source");
        stream.setHidden(0);
        return stream;
    }
}