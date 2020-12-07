package com.tango.stream.manager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@EnableFeignClients
@SpringBootApplication
public class StreamManagerApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamManagerApplication.class, args);
    }
}
