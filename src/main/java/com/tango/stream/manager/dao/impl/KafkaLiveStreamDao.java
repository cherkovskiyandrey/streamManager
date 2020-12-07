package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.dao.LiveStreamDao;
import com.tango.stream.manager.model.LiveStreamData;
import com.tango.stream.manager.model.Stream;
import com.tango.stream.manager.model.StreamKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.tango.stream.manager.utils.EncryptUtil.decryptStreamKey;

@Slf4j
public class KafkaLiveStreamDao implements LiveStreamDao {
    private final ReadOnlyKeyValueStore<Long, Stream> store;

    public KafkaLiveStreamDao(ReadOnlyKeyValueStore<Long, Stream> store) {
        this.store = store;
    }

    @Override
    public Optional<LiveStreamData> getStream(@Nonnull String encryptedStreamId) {
        return decryptStreamKey(encryptedStreamId)
                .map(StreamKey::getStreamId)
                .map(store::get)
                .map(stream -> toLiveStreamData(stream, encryptedStreamId));
    }

    private LiveStreamData toLiveStreamData(Stream stream, String encryptedStreamId) {
        return LiveStreamData.builder()
                .encryptedStreamKey(encryptedStreamId)
                .streamerAccountId(stream.getAccountId())
                .streamId(stream.getId())
                .build();
    }
}
