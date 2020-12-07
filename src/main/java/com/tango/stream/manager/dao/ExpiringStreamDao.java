package com.tango.stream.manager.dao;

import com.tango.stream.manager.model.NodeDescription;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface ExpiringStreamDao<T> extends StreamDao<T> {
    @Nonnull
    Collection<String> getAndRemoveExpired(@Nonnull NodeDescription nodeUid);

    int getExpirationPeriodInSec();
}
