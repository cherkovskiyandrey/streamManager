package com.tango.stream.manager.controllers;

import com.tango.stream.manager.NoKafkaBaseTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class NodeControllerTest extends NoKafkaBaseTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void shouldProcessKeepAlive() throws Exception {
        mockMvc.perform(post("/node/keepAlive")
                .content("{\"uid\":\"1\",\"extIp\":\"1.1.1.1\",\"nodeType\":\"GATEWAY\",\"region\":\"default\"," +
                        "\"version\":\"default\",\"avgCpuUsage\":1.12,\"medCpuUsage\":1.2,\"unknownNewProperty\":123}")
                .contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk())
        ;
    }
}