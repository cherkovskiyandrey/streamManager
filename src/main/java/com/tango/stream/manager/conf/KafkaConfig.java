package com.tango.stream.manager.conf;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import javax.annotation.Nonnull;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

@Configuration
@Slf4j
@Profile("kafka")
@EnableKafka
public class KafkaConfig {
    public static final String TERMINATED_STREAMS_TOPIC = "kafka.stream.terminated.topic";
    public static final String TERMINATED_STREAM_KAFKA_CONTAINER_FACTORY_NAME = "terminatedStreamKafkaContainerFactory";

    @Bean("terminatedStreamKafkaConsumerPropertiesMap")
    public Supplier<Map<String, Object>> terminatedStreamKafkaConsumerPropertiesMap(KafkaProperties properties,
                                                                                    @Value("${kafka.stream.terminated.group.id}") String groupId,
                                                                                    @Value("${kafka.stream.terminated.auto-offset-reset}") String autoOffsetReset) {
        return () -> {
            Map<String, Object> props = new HashMap<>(properties.buildConsumerProperties());
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, buildClientId());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
            return props;
        };
    }

    @Bean("terminatedStreamConsumerFactory")
    public ConsumerFactory<String, Object> terminatedStreamConsumerFactory(@Qualifier("terminatedStreamKafkaConsumerPropertiesMap") Supplier<Map<String, Object>> map) {
        return new DefaultKafkaConsumerFactory<>(map.get());
    }

    @Bean(TERMINATED_STREAM_KAFKA_CONTAINER_FACTORY_NAME)
    public ConcurrentKafkaListenerContainerFactory<String, Object> terminatedStreamKafkaContainerFactory(
            @Qualifier("terminatedStreamConsumerFactory") ConsumerFactory<String, Object> kafkaConsumerFactory,
            @Value("${kafka.stream.terminated.listener.factory.concurrency:10}") int concurrency) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.setConcurrency(concurrency);
        ContainerProperties containerProperties = factory.getContainerProperties();
        containerProperties.setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

    @Nonnull
    protected String buildClientId() {
        String idSuffix;
        try {
            idSuffix = InetAddress.getLocalHost().getHostName();
            log.info("Resolved hostname: {}", idSuffix);
        } catch (UnknownHostException var3) {
            log.error("Unable to resolve localhost host name.", var3);
            idSuffix = UUID.randomUUID().toString();
        }

        String id = "streammanager-" + idSuffix;
        log.info("Kafka client id set: {}", id);
        return id;
    }
}
