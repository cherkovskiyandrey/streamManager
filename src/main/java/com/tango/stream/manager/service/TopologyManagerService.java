package com.tango.stream.manager.service;

import com.tango.stream.manager.model.*;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Lock ordering:
 * 1. stream
 * 2. edge cluster
 * 3. relay cluster
 * 4. gateway cluster
 */
public interface TopologyManagerService {

    @Nonnull
    AvailableGatewayResponse getAvailableGateway(@Nonnull AvailableGatewayRequest request) throws Exception;

    @Nonnull
    AvailableEdgeResponse getAvailableEdge(@Nonnull AvailableEdgeRequest request) throws Exception;

    /**
     * Build connection graph for new streams on provided gateway.
     * WARNING: don't invoke under any lock to avoid deadlock, because implementation should acquires locks for needed clusters in predefined order.
     *
     * @param gateway
     * @param streams
     * @throws Exception
     */
    void buildConnectionGraphs(@Nonnull NodeDescription gateway, @Nonnull List<AvailableGatewayRequest> streams) throws Exception;

    /**
     * Async version of {@link TopologyManagerService#buildConnectionGraphs(NodeDescription, List)}
     *
     * @param gateway
     * @param streams
     */
    void buildConnectionGraphsAsync(@Nonnull NodeDescription gateway, @Nonnull List<AvailableGatewayRequest> streams);

    /**
     * Try to expand capacity for stream.
     * WARNING: don't invoke under any lock to avoid deadlock, because implementation should acquires locks for needed clusters in predefined order.
     *
     * @param encryptedStreamKey
     * @param capacityToAdd      - client connections, not slots!
     */
    void addViewerCapacity(@Nonnull String encryptedStreamKey, int capacityToAdd);

    /**
     * Async version of {@link TopologyManagerService#addViewerCapacity(String, int)}
     *
     * @param encryptedStreamKey
     * @param capacityToAdd      - client connections, not slots!
     */
    void addViewerCapacityAsync(@Nonnull String encryptedStreamKey, int capacityToAdd);

    /**
     * Remove viewer capacity for stream.
     * WARNING: don't invoke under any lock to avoid deadlock, because implementation should acquires locks for needed clusters in predefined order.
     *
     * @param encryptedStreamKey
     * @param capacityToRemove
     */
    void removeViewerCapacity(@Nonnull String encryptedStreamKey, @Nonnull Map<NodeDescription, Integer> capacityToRemove);

    /**
     * Async version of {@link TopologyManagerService#removeViewerCapacity(String, Map)}
     *
     * @param encryptedStreamKey
     * @param capacityToRemove
     */
    void removeViewerCapacityAsync(@Nonnull String encryptedStreamKey, @Nonnull Map<NodeDescription, Integer> capacityToRemove);

    /**
     * Rebuild connection graph for stream, excluding voidedNode from path.
     * WARNING: don't invoke under any lock to avoid deadlock, because implementation should acquires locks for needed clusters in predefined order.
     * To be lock agnostic use async version of the method.
     *
     * @param encryptedStreamKey
     * @param voidedNode
     * @throws Exception
     */
    void rebuildConnectionGraph(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription voidedNode) throws Exception;

    /**
     * Async version of the {@link TopologyManagerService#rebuildConnectionGraph(String, NodeDescription)}
     *
     * @param encryptedStreamKey
     * @param voidedNode
     * @throws Exception
     */
    void rebuildConnectionGraphAsync(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription voidedNode);

    /**
     * Rebuild connection graph for all live streams on provided nodes and avoid it.
     * WARNING: don't invoke under any lock to avoid deadlock, because implementation should acquires locks for needed clusters in predefined order.
     * To be lock agnostic use async version of the method.
     *
     * @param goneNodes
     */
    void rebuildConnectionGraph(@Nonnull List<NodeDescription> goneNodes);

    /**
     * Async version of {@link TopologyManagerService#rebuildConnectionGraph(List)}
     *
     * @param goneNodes
     */
    void rebuildConnectionGraphAsync(@Nonnull List<NodeDescription> goneNodes);

    /**
     * Destroy connection graph from provided node to graph below.
     * WARNING: don't invoke under any lock to avoid deadlock, because implementation should acquires locks for needed clusters in predefined order.
     *
     * @param node
     * @param streams
     */
    void destroyConnectionGraph(@Nonnull NodeDescription node, @Nonnull Collection<String> streams);

    /**
     * Async version of {@link TopologyManagerService#destroyConnectionGraph(NodeDescription, Collection)}
     *
     * @param node
     * @param streams
     */
    void destroyConnectionGraphAsync(@Nonnull NodeDescription node, @Nonnull Collection<String> streams);
}
