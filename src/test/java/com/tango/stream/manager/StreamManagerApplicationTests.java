package com.tango.stream.manager;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StreamManagerApplicationTests extends NoKafkaBaseTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    public void contextLoads() {
    }

    @Test
    public void shouldReturnHealthCheck() {
        assertEquals(HttpStatus.OK, restTemplate.getForEntity("http://localhost:" + port + "/actuator/health", String.class).getStatusCode());
    }
}
