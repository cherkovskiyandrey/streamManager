package com.tango.stream.manager.dao;

import com.tango.stream.manager.model.LiveStreamData;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface LiveStreamDao {

    Optional<LiveStreamData> getStream(@Nonnull String encryptedStreamId);
}
