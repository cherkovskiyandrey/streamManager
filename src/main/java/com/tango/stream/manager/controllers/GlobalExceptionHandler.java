package com.tango.stream.manager.controllers;

import com.tango.stream.manager.StreamManagerApplication;
import com.tango.stream.manager.model.ExceptionResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

import java.util.Map;

import static com.tango.stream.manager.conf.CommonConfig.DEFAULT_OBJECT_MAPPER;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ExceptionResponse handleIllegalArgumentException(IllegalArgumentException e, WebRequest request) {
        return handleServerException(e, request);
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ExceptionResponse handleServerException(Exception e, WebRequest request) {
        String description = request.getDescription(false);
        Map<String, String[]> parameterMap = request.getParameterMap();
        log.error("controller exception for request: {}, params: {}", description, serialize(parameterMap), e);
        return new ExceptionResponse(e.getMessage(), description, e.getClass().getName());
    }

    private String serialize(Object object) {
        try {
            return DEFAULT_OBJECT_MAPPER.writeValueAsString(object);
        } catch (Exception e) {
            return null;
        }
    }
}

