package com.tango.stream.manager.utils;

import com.tango.stream.manager.model.StreamKey;
import com.tango.util.crypto.CryptoUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Optional;

import static org.apache.commons.lang.StringUtils.isEmpty;

@Slf4j
public class EncryptUtil {
    private static final String KEY_SEPARATOR = ":";

    //todo: move this to a common lib for manager, gw & stream
    public static Optional<Long> getStreamId(String encryptedStreamKey) {
        return decryptStreamKey(encryptedStreamKey)
                .map(StreamKey::getStreamId);
    }

    public static Optional<StreamKey> decryptStreamKey(String encryptedStreamKey) {
        try {
            String decrypted = CryptoUtils.symetricDecryptToString(encryptedStreamKey);
            int separatorIndex = decrypted.indexOf(KEY_SEPARATOR);
            if (separatorIndex < 0) {
                log.warn("decryptStreamKey(): malformed key {} decrypted to {}", encryptedStreamKey, decrypted);
                return Optional.empty();
            }
            String idStr = decrypted.substring(0, separatorIndex);
            String initTimeStr = decrypted.substring(separatorIndex + 1);

            if (isEmpty(initTimeStr)) {
                log.warn("Encrypted stream key has no init time: key={}", encryptedStreamKey);
                return Optional.empty();
            }

            long id = Long.parseLong(idStr);
            StreamKey streamKey = StreamKey.builder()
                    .streamId(id)
                    .initTime(Long.parseLong(initTimeStr))
                    .build();
            return Optional.of(streamKey);
        } catch (Exception e) {
            log.warn("Couldn't parse streamKey: {}", encryptedStreamKey, e);
            return Optional.empty();
        }
    }

    @SneakyThrows
    public static String encryptStreamKey(StreamKey streamKey) {
        String str = streamKey.getStreamId() + KEY_SEPARATOR + streamKey.getInitTime();
        return CryptoUtils.symetricEncrypt(str);
    }

    @Nonnull
    public static String encryptViewer(long viewerAccountId, @Nonnull String viewerUsername) {
        //todo
        return null;
    }
}
