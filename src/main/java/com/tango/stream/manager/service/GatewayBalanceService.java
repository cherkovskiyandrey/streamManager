package com.tango.stream.manager.service;

import com.tango.stream.manager.exceptions.BalanceException;
import com.tango.stream.manager.model.AvailableGatewayRequest;
import com.tango.stream.manager.model.BookedNode;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.ReleaseClients;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface GatewayBalanceService extends BalanceService {

    /**
     * Book gateway for stream.
     *
     * @param request
     * @return
     * @throws Exception
     */
    @Nonnull
    NodeDescription getAvailableGateway(@Nonnull AvailableGatewayRequest request) throws Exception;

    /**
     * Book preliminarily estimated nodes.
     * IMPORTANT: unsafe operation and should be invoked under distributed lock.
     *
     * @param encryptedStreamKey
     * @param gateway
     * @param gatewayClients
     * @return
     */
    BookedNode bookClients(@Nonnull String encryptedStreamKey,
                           @Nonnull NodeDescription gateway,
                           int gatewayClients) throws BalanceException;

    /**
     * Release particular clients on gateway for stream.
     *
     * @param encryptedStreamKey
     * @param gateway
     */
    void releaseClients(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription gateway, int capacity);

    /**
     * Release stream.
     *
     * @param encryptedStreamKeys
     * @param gateway
     * @return
     */
    void releaseStreams(@Nonnull Collection<String> encryptedStreamKeys, @Nonnull NodeDescription gateway);
}
