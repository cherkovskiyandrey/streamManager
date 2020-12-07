package com.tango.stream.manager.capacity;

import com.tango.stream.manager.model.ViewersCapacity;

public interface InitialViewersCapacityProvider {
    InitialViewersCapacityStrategy getStrategy();

    ViewersCapacity getInitialViewersCapacity(long accountId);
}
