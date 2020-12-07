package com.tango.stream.manager.exceptions;

public class LockAcquiringFail extends RuntimeException {
    public LockAcquiringFail(String name) {
        super(name);
    }
}
