package com.tango.stream.manager;

import com.tango.stream.manager.util.TestClock;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Clock;

@Configuration
public class TestConfigurationClass {

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(@Value("${redis.clusterAddress}") String address) {
        Config config = new Config();
        config.useSingleServer().setAddress(address);
        config.setCodec(new JsonJacksonCodec());
        return Redisson.create(config);
    }

    @Bean
    @Primary
    public Clock clock() {
        return new TestClock(Clock.systemDefaultZone());
    }
}
