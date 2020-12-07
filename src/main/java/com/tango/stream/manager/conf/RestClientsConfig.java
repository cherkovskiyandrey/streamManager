package com.tango.stream.manager.conf;

import feign.Client;
import feign.httpclient.ApacheHttpClient;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.commons.httpclient.ApacheHttpClientConnectionManagerFactory;
import org.springframework.cloud.commons.httpclient.ApacheHttpClientFactory;
import org.springframework.cloud.openfeign.support.FeignHttpClientProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RestClientsConfig {
    @Bean
    @Autowired
    public HttpClientConnectionManager connectionManager(ApacheHttpClientConnectionManagerFactory connectionManagerFactory,
                                                         FeignHttpClientProperties httpClientProperties) {
        return connectionManagerFactory
                .newConnectionManager(httpClientProperties.isDisableSslValidation(),
                        httpClientProperties.getMaxConnections(),
                        httpClientProperties.getMaxConnectionsPerRoute(),
                        httpClientProperties.getTimeToLive(),
                        httpClientProperties.getTimeToLiveUnit(),
                        null);
    }

    @Bean
    public CloseableHttpClient httpClient(ApacheHttpClientFactory httpClientFactory,
                                          HttpClientConnectionManager httpClientConnectionManager,
                                          FeignHttpClientProperties httpClientProperties,
                                          @Value("${feign.httpclient.requestTimeout}") int requestTimeout,
                                          @Value("${feign.httpclient.socketTimeout}") int socketTimeout) {
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setConnectTimeout(httpClientProperties.getConnectionTimeout())
                .setRedirectsEnabled(httpClientProperties.isFollowRedirects())
                .setConnectionRequestTimeout(requestTimeout)
                .setSocketTimeout(socketTimeout)
                .build();
        return httpClientFactory.createBuilder()
                .setConnectionManager(httpClientConnectionManager)
                .setDefaultRequestConfig(defaultRequestConfig).build();
    }

    @Bean
    public Client feignClient(HttpClient httpClient) {
        return new ApacheHttpClient(httpClient);
    }

}
