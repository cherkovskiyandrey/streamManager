package com.tango.stream.manager;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.TestPropertySource;

@Configuration
@Profile("no-kafka")
@TestPropertySource(properties = "terminated.streams.listener.autostart.enabled=false")
public class NoKafkaBaseConfiguration implements KafkaListenerConfigurer {

    //Because of fac..ing spring.
    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        registrar.setEndpointRegistry(new KafkaListenerEndpointRegistry() {
            @Override
            public void registerListenerContainer(KafkaListenerEndpoint endpoint, KafkaListenerContainerFactory<?> factory) {
            }

            @Override
            public void registerListenerContainer(KafkaListenerEndpoint endpoint, KafkaListenerContainerFactory<?> factory, boolean startImmediately) {
            }
        });
    }
}
