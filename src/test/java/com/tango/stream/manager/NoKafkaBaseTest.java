package com.tango.stream.manager;

import com.tango.stream.manager.dao.LiveStreamDao;
import com.tango.stream.manager.service.CommandProcessor;
import org.junit.jupiter.api.Disabled;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import static com.tango.stream.manager.conf.KafkaConfig.TERMINATED_STREAM_KAFKA_CONTAINER_FACTORY_NAME;

@ActiveProfiles("no-kafka")
@Disabled
public class NoKafkaBaseTest extends BaseTest {
    @MockBean
    protected LiveStreamDao liveStreamDao;
    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;
    @MockBean
    private CommandProcessor commandProcessor;
    @MockBean(name = TERMINATED_STREAM_KAFKA_CONTAINER_FACTORY_NAME)
    private ConcurrentKafkaListenerContainerFactory<String, Object> stringObjectConcurrentKafkaListenerContainerFactory;
}
