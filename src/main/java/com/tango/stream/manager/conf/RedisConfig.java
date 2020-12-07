package com.tango.stream.manager.conf;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import org.redisson.config.SubscriptionMode;
import org.redisson.config.TransportMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Slf4j
@Configuration
@Profile("production")
public class RedisConfig {

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(@Value("${redis.clusterAddress}") String address,
                                         @Value("${redis.ping.connection.interval:3000}") int pingConnectionInterval,
                                         @Value("${redis.scan.interval.ms:5000}") int scanInterval,
                                         @Value("${redis.decodeInExecutor:true}") boolean decodeInExecutor,
                                         @Value("${redis.executorThreads:16}") int executorThreads,
                                         @Value("${redis.nettyThreads:32}") int nettyThreads,
                                         @Value("${redis.retryAttempts:5}") int retryAttempts,
                                         @Value("${redis.retryInterval}") int retryInterval,
                                         @Value("${redis.tcpNoDelay:true}") boolean tcpNoDelay,
                                         @Value("${redis.keepAlive:true}") boolean keepAlive) {
        if (StringUtils.isBlank(address)) {
            throw new IllegalStateException("Redis clusterAddress is empty!");
        }
        log.info("Create Redisson client for address {}, pingConnectionInterval {}, readMode {}, decodeInExecutor {}, nettyThreads {}, executorThreads {}",
                address, pingConnectionInterval, ReadMode.MASTER, decodeInExecutor, nettyThreads, executorThreads);
        Config config = new Config()
                .setThreads(executorThreads)
                .setCodec(new JsonJacksonCodec())
                .setTransportMode(TransportMode.NIO)
                .setNettyThreads(nettyThreads);
        config.useClusterServers().addNodeAddress(address)
                .setRetryAttempts(retryAttempts)
                .setRetryInterval(retryInterval)
                .setTcpNoDelay(tcpNoDelay)
                .setKeepAlive(keepAlive)
                .setPingConnectionInterval(pingConnectionInterval)
                .setReadMode(ReadMode.MASTER)
                .setScanInterval(scanInterval)
                .setSubscriptionMode(SubscriptionMode.MASTER);

        return Redisson.create(config);
    }
}

