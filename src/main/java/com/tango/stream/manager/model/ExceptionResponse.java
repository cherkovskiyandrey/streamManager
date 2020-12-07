package com.tango.stream.manager.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Value;

import java.time.Instant;

@Value
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExceptionResponse {
    Instant timestamp = Instant.now();
    String message;
    Object details;
    String exceptionName;
}
