package com.tango.stream.manager.utils;

@FunctionalInterface
public interface RunnableWithException<E extends Exception> {
    void run() throws E;
}
