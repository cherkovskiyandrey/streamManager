package com.tango.stream.manager.conf;

import com.tango.stream.manager.dao.LiveStreamDao;
import com.tango.stream.manager.dao.impl.KafkaLiveStreamDao;
import com.tango.stream.manager.model.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.time.Instant.now;

@Configuration
@Slf4j
@Profile("kafka")
@EnableKafka
public class KafkaStreamsConfig {
    private static final String LIVE_STREAMS_STORE = "liveStreamsStore";

    @Value("${spring.kafka.bootstrap-servers:localhost:8092}")
    protected String kafkaServers;
    @Value("${stream.live.unfiltered.topic}")
    private String liveStreamsTopic;
    @Value("${kafka.streams.timeout.seconds}")
    private int kafkaStreamsTimeoutSec;
    @Value("${stream.live.unfiltered.application.id}")
    private String liveStreamsAppId;

    @Bean(destroyMethod = "close")
    public KafkaStreams kafkaStreams() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.globalTable(
                liveStreamsTopic,
                Consumed.with(Serdes.Long(), new JsonSerde<>(Stream.class)),
                Materialized.as(LIVE_STREAMS_STORE)
        );

        Topology topology = builder.build();
        log.info("Constructed topology for Kafka Streams: {}", topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProperties());
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        return kafkaStreams;
    }

    @Bean
    @Autowired
    public LiveStreamDao liveStreamsDao(KafkaStreams kafkaStreams) {
        waitForStreamsToBeRunning(kafkaStreams);

        StoreQueryParameters<ReadOnlyKeyValueStore<Long, Stream>> storeParams = StoreQueryParameters.fromNameAndType(LIVE_STREAMS_STORE, QueryableStoreTypes.keyValueStore());
        ReadOnlyKeyValueStore<Long, Stream> store = kafkaStreams.store(storeParams);
        return new KafkaLiveStreamDao(store);
    }

    @SneakyThrows
    private void waitForStreamsToBeRunning(KafkaStreams kafkaStreams) {
        Instant exitOnTimeout = now().plusSeconds(kafkaStreamsTimeoutSec);
        while (kafkaStreams.state() != KafkaStreams.State.RUNNING
                && exitOnTimeout.isAfter(now())) {
            log.info("Kafka streams not running yet (current state is {}), waiting...", kafkaStreams.state());
            TimeUnit.SECONDS.sleep(1L);
        }

        KafkaStreams.State state = kafkaStreams.state();
        if (state != KafkaStreams.State.RUNNING) {
            log.warn("Kafka streams still not running, current state is {}", state);
        } else {
            log.info("Kafka streams started, current state is {}", state);
        }
    }

    private Properties streamsProperties() {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, liveStreamsAppId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        return properties;
    }
}
