package com.tango.stream.manager;

import org.junit.jupiter.api.Disabled;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

@EmbeddedKafka(
        partitions = 1,
        topics = {
                "${stream.live.unfiltered.topic}",
                "${commands.kafka.topic}",
                "${kafka.stream.terminated.topic}"
        }
)
@ActiveProfiles("kafka")
@Disabled
public class BaseKafkaTest extends BaseTest {
    @Value("${spring.embedded.kafka.brokers}")
    protected String embeddedKafkaServers;


}
