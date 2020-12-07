package com.tango.stream.manager.service;

import com.tango.stream.manager.exceptions.BalanceException;
import com.tango.stream.manager.model.BookedNodes;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.ReleaseClients;

import javax.annotation.Nonnull;
import java.util.Set;

public interface EdgeOrRelayBalanceService extends BalanceService {

    /**
     * Choose appropriate (less loaded) node in current region and try to locate input clients on them.
     * IMPORTANT: unsafe operation and should be invoked under distributed lock.
     *
     * @param encryptedStreamKey
     * @param region
     * @param clients
     * @return preliminarily chosen nodes
     */
    @Nonnull
    BookedNodes chooseNodesInRegion(@Nonnull String encryptedStreamKey, @Nonnull String region, int clients, @Nonnull Set<NodeDescription> voidedNodes);

    /**
     * Book preliminarily chosen nodes according to booked host nodes.
     *
     * @param chosenNodes
     * @param bookedHostNodes
     * @return booked nodes with commands.
     */
    @Nonnull
    BookedNodes bookNodes(@Nonnull BookedNodes chosenNodes, @Nonnull BookedNodes bookedHostNodes) throws BalanceException;


    /**
     * Release stream on current node and return parent node if exists.
     *
     * @param encryptedStreamKey
     * @param node
     * @return
     */
    @Nonnull
    ReleaseClients releaseStream(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription node);


    /**
     * Release particular clients on node for stream, return parent if exists.
     *
     * @param encryptedStreamKey
     * @param node
     * @return
     */
    @Nonnull
    ReleaseClients releaseClientCapacity(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription node, int capacity);
}
