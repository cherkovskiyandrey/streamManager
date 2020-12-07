package com.tango.stream.manager.service;

import com.tango.stream.manager.model.ActionSpec;
import com.tango.stream.manager.model.CallSpec;

import javax.annotation.Nonnull;

public interface LocksService {

    <E extends Exception> void doUnderLock(@Nonnull ActionSpec<E> actionSpec) throws E;

    <T> T doUnderLock(@Nonnull CallSpec<T> callSpec) throws Exception;
}
