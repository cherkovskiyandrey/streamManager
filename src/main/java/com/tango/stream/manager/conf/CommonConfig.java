package com.tango.stream.manager.conf;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tango.stream.manager.dao.NodeClusterDao;
import com.tango.stream.manager.dao.StreamDao;
import com.tango.stream.manager.model.NodeType;
import com.tango.stream.manager.model.StreamData;
import com.tango.stream.manager.model.StreamDescription;
import com.tango.stream.manager.service.impl.ConfigurationService;
import com.tango.stream.manager.service.impl.CpuUsageBalanceHelper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

import static com.tango.stream.manager.conf.DaoConfig.*;

@Configuration
public class CommonConfig {
    public final static String RELAY_CPU_USAGE_BALANCE_HELPER = "relayCpuUsageBalanceHelper";
    public final static String EDGE_CPU_USAGE_BALANCE_HELPER = "edgeCpuUsageBalanceHelper";

    public static ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Bean
    public ObjectMapper objectMapper() {
        return DEFAULT_OBJECT_MAPPER;
    }

    @Bean(RELAY_CPU_USAGE_BALANCE_HELPER)
    public CpuUsageBalanceHelper relayCpuUsageBalanceHelper(ConfigurationService configurationService,
                                                            @Qualifier(NODE_CLUSTER_DAO_FOR_RELAY_CPU_USAGE_BALANCER) NodeClusterDao nodeClusterDao,
                                                            @Qualifier(RELAY_STREAM) StreamDao<StreamDescription> streamDao) {
        return new CpuUsageBalanceHelper(
                configurationService,
                nodeClusterDao,
                streamDao,
                NodeType.RELAY
        );
    }

    @Bean(EDGE_CPU_USAGE_BALANCE_HELPER)
    public CpuUsageBalanceHelper edgeCpuUsageBalanceHelper(ConfigurationService configurationService,
                                                           @Qualifier(NODE_CLUSTER_DAO_FOR_EDGE_CPU_USAGE_BALANCER) NodeClusterDao nodeClusterDao,
                                                           @Qualifier(EDGE_STREAM) StreamDao<StreamDescription> streamDao) {
        return new CpuUsageBalanceHelper(
                configurationService,
                nodeClusterDao,
                streamDao,
                NodeType.EDGE
        );
    }

    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }
}
