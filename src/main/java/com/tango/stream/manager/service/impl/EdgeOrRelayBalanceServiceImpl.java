package com.tango.stream.manager.service.impl;

import com.google.common.collect.*;
import com.tango.stream.manager.conf.DaoConfig;
import com.tango.stream.manager.dao.LiveRegionDao;
import com.tango.stream.manager.dao.NodeDao;
import com.tango.stream.manager.dao.StreamDao;
import com.tango.stream.manager.dao.ViewerDao;
import com.tango.stream.manager.exceptions.BalanceException;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.model.commands.*;
import com.tango.stream.manager.service.EdgeOrRelayBalanceService;
import com.tango.stream.manager.service.LocksService;
import com.tango.stream.manager.service.VersionService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

@Slf4j
public abstract class EdgeOrRelayBalanceServiceImpl implements EdgeOrRelayBalanceService {

    @Autowired
    protected LocksService locksService;
    @Autowired
    protected RedissonClient redissonClient;
    @Autowired
    protected NodeDao nodeDao;
    @Autowired
    @Qualifier(DaoConfig.GATEWAY_VIEWER)
    protected ViewerDao<NodeDescription> gatewayViewerDao;
    @Autowired
    @Qualifier(DaoConfig.RELAY_VIEWER)
    protected ViewerDao<NodeDescription> relayViewerDao;
    @Autowired
    protected LiveRegionDao liveRegionDao;
    @Autowired
    protected VersionService versionService;
    @Autowired
    protected MeterRegistry meterRegistry;
    @Autowired
    protected ConfigurationService configurationService;

    @Nonnull
    protected abstract NodeType getNodeType();

    protected abstract int toBookedClients(int clients);

    protected abstract StreamDao<StreamDescription> getStreamDao();

    @Nonnull
    @Override
    public BookedNodes chooseNodesInRegion(@Nonnull String encryptedStreamKey, @Nonnull String region, int clients, @Nonnull Set<NodeDescription> voidedNodes) {
        NodeType nodeType = getNodeType();
        String activeVersion = versionService.getActualVersion(nodeType, region);
        Set<String> liveRegions = ImmutableSet.copyOf(liveRegionDao.getAllRegions(nodeType));

        if (!liveRegions.contains(region)) {
            log.info("Region=[{}] doesn't contain live edge nodes. Stream=[{}]", region, encryptedStreamKey);
            return BookedNodes.empty();
        }
        int bookedClientsInRegion = toBookedClients(clients);
        log.info("Try to book clients=[{}] for stream=[{}] in region=[{}], type=[{}]", bookedClientsInRegion, encryptedStreamKey, region, nodeType);

        boolean isEmptyCluster = getClusterDao().isEmpty(region, activeVersion);
        if (isEmptyCluster) {
            log.warn("Can't find available nodes type=[{}], region=[{}], version=[{}], stream=[{}]", nodeType, region, activeVersion, encryptedStreamKey);
            return BookedNodes.empty();
        }

        List<BookedNode> bookedNodes = Lists.newArrayList();
        List<BookedNode> linkedNodes = Lists.newArrayList();
        int restedClients = bookedClientsInRegion;
        Collection<NodeDescription> sortedNodes = getClusterDao().getCluster(region, activeVersion);
        List<Pair<NodeDescription, Boolean>> sortedNodesWithStream = reorderByStream(sortedNodes, encryptedStreamKey);
        Iterator<Pair<NodeDescription, Boolean>> iterator = sortedNodesWithStream.iterator();

        while (iterator.hasNext() && restedClients > 0) {
            Pair<NodeDescription, Boolean> node = iterator.next();
            NodeDescription relayDesc = node.getLeft();
            if (voidedNodes.contains(relayDesc)) {
                continue;
            }
            Boolean isLinked = node.getRight();
            List<BookedNode> nodeList = isLinked ? linkedNodes : bookedNodes;
            //TODO: check isPotentialUnreachable
            long currentScore = getClusterDao().getScore(relayDesc).orElse(0L);
            ScoreResult scoreResult = tryToAddClients(currentScore, restedClients);
            if (scoreResult.getReservedClientPlaces() == 0) {
                break;
            }
            restedClients -= scoreResult.getReservedClientPlaces();
            nodeList.add(BookedNode.builder()
                    .encryptedStreamId(encryptedStreamKey)
                    .nodeDescription(relayDesc)
                    .bookedClients(scoreResult.getReservedClientPlaces())
                    .build());
        }
        if (restedClients > 0) {
            log.warn("Booked clients: requested=[{}], reserved=[{}], stream=[{}], region=[{}], type=[{}]",
                    bookedClientsInRegion, (bookedClientsInRegion - restedClients), encryptedStreamKey, region, nodeType);
            //todo: signal to expand
        }
        if (bookedNodes.isEmpty() && linkedNodes.isEmpty()) {
            log.warn("All nodes overloaded: type=[{}], region=[{}], version=[{}], stream=[{}]", nodeType, region, activeVersion, encryptedStreamKey);
            return BookedNodes.empty();
        }

        return BookedNodes.builder()
                .region(region)
                .bookedNodes(bookedNodes)
                .linkedNodes(linkedNodes)
                .build();
    }

    //2 paths:
    //1 - nodes already have processed stream in order from max capacity to min
    //2 - rest nodes in nature order (less loaded)
    @Nonnull
    private List<Pair<NodeDescription, Boolean>> reorderByStream(@Nonnull Collection<NodeDescription> sortedNodes, @Nonnull String encryptedStreamKey) {
        List<Pair<NodeDescription, Boolean>> result = Lists.newArrayList();
        Set<NodeDescription> linked = Sets.newHashSet();
        sortedNodes.stream()
                .map(nd -> Pair.of(nd, getStreamDao().findStream(nd, encryptedStreamKey).map(StreamDescription::getMaxAvailableClients).orElse(0)))
                .filter(pair -> pair.getRight() > 0)
                .sorted(Comparator.comparingInt((ToIntFunction<Pair<NodeDescription, Integer>>) Pair::getRight).reversed())
                .map(Pair::getLeft)
                .forEach(nd -> {
                    result.add(Pair.of(nd, true));
                    linked.add(nd);
                });
        sortedNodes.forEach(nd -> {
            if (!linked.contains(nd)) {
                result.add(Pair.of(nd, false));
            }
        });
        return result;
    }

    @Nonnull
    @Override
    public BookedNodes bookNodes(@Nonnull BookedNodes chosenNodes, @Nonnull BookedNodes bookedHostNodes) throws BalanceException {
        List<BookedNode> bookedNodes = Lists.newArrayList();
        List<BookedNode> linkedNodes = Lists.newArrayList();
        Multimap<NodeDescription, Command> commands = MultimapBuilder.linkedHashKeys().arrayListValues().build();

        bookNewNodesInRegion(chosenNodes.getBookedNodes(), bookedHostNodes, bookedNodes, commands);
        updateLinkedNodesInRegion(chosenNodes.getLinkedNodes(), linkedNodes, commands);

        return BookedNodes.builder()
                .region(chosenNodes.getRegion())
                .bookedNodes(bookedNodes)
                .linkedNodes(linkedNodes)
                .commands(commands)
                .build();
    }

    private void bookNewNodesInRegion(@Nonnull List<BookedNode> chosenNodes,
                                      @Nonnull BookedNodes bookedHostNodes,
                                      @Nonnull List<BookedNode> bookedNodesOutput,
                                      @Nonnull Multimap<NodeDescription, Command> commandsOutput) throws BalanceException {
        Queue<BookedNode> chosenNodesOrdered = chosenNodes.stream()
                .sorted(Comparator.comparingInt(BookedNode::getBookedClients).reversed())
                .collect(Collectors.toCollection(ArrayDeque::new));
        List<BookedNode> bookedHostNodesOrdered = Stream.concat(bookedHostNodes.getLinkedNodes().stream(), bookedHostNodes.getBookedNodes().stream())
                .sorted(Comparator.comparing(BookedNode::getBookedClients).reversed())
                .collect(Collectors.toList());
        int bookedHostClientsTotal = bookedHostNodesOrdered.stream().mapToInt(BookedNode::getBookedClients).sum();
        int chosenNodesTotal = chosenNodes.size();
        if (bookedHostClientsTotal > chosenNodesTotal) {
            log.error("Inconsistent state: Host clients=[{}] is more than chosen nodes=[{}]", bookedHostClientsTotal, chosenNodesTotal);
            throw new BalanceException(format("Host clients=[%d] is more than chosen nodes=[%d]", bookedHostClientsTotal, chosenNodesTotal));
        }
        if (bookedHostClientsTotal == 0) {
            log.warn("Insufficient capacity for chosenNodes=[{}]", chosenNodes);
            return;
        }

        for (BookedNode hostNode : bookedHostNodesOrdered) {
            for (int clientPlaceHolder = 0; clientPlaceHolder < hostNode.getBookedClients(); clientPlaceHolder++) {
                BookedNode chosenNode = Objects.requireNonNull(chosenNodesOrdered.poll());
                bookedNodesOutput.add(chosenNode);
                Optional<StreamDescription> stream = getStreamDao().findStream(chosenNode.getNodeDescription(), chosenNode.getEncryptedStreamId());
                if (stream.isPresent()) {
                    Command command = generateUpdateRouteCommand(chosenNode.getEncryptedStreamId(), hostNode.getNodeDescription(), chosenNode.getBookedClients());
                    updateNode(chosenNode, hostNode.getNodeDescription(), stream.get(), command, chosenNode.getBookedClients());
                    commandsOutput.put(chosenNode.getNodeDescription(), command);
                } else {
                    Command command = generateAddRouteCommand(chosenNode, hostNode);
                    addScore(chosenNode, chosenNode.getBookedClients() + 1);
                    regNode(chosenNode, hostNode.getNodeDescription(), command);
                    commandsOutput.put(chosenNode.getNodeDescription(), command);
                }
            }
        }
    }

    private void updateLinkedNodesInRegion(@Nonnull List<BookedNode> linkedNodes,
                                           @Nonnull List<BookedNode> bookedNodes,
                                           @Nonnull Multimap<NodeDescription, Command> commands) throws BalanceException {
        for (BookedNode linkedNode : linkedNodes) {
            NodeDescription hostNode = linkedNode.getHostNode();
            if (hostNode == null) {
                log.error("updateLinkedNodesInRegion(): incorrect state, for stream=[{}] already linked node=[{}] doesn't have hostNode",
                        linkedNode.getEncryptedStreamId(), linkedNode);
                throw new BalanceException(format("bookNodesInRegion(): incorrect state, for stream=[%s] already linked node=[%s] doesn't have hostNode",
                        linkedNode.getEncryptedStreamId(), linkedNode));
            }
            Optional<StreamDescription> currentStreamData = getStreamDao().findStream(linkedNode.getNodeDescription(), linkedNode.getEncryptedStreamId());
            if (!currentStreamData.isPresent()) {
                log.error("updateLinkedNodesInRegion(): incorrect state, for stream=[{}] already linked node=[{}] doesn't streamData",
                        linkedNode.getEncryptedStreamId(), linkedNode);
                throw new BalanceException(format("updateLinkedNodesInRegion(): incorrect state, for stream=[%s] already linked node=[%s] doesn't streamData", linkedNode.getEncryptedStreamId(), linkedNode));
            }
            StreamDescription currentStream = currentStreamData.get();
            int currentViewerSize = currentStream.getMaxAvailableClients();
            int newViewerCapacity = currentViewerSize + linkedNode.getBookedClients();

            Command command = generateUpdateRouteCommand(linkedNode.getEncryptedStreamId(), hostNode, newViewerCapacity);
            bookedNodes.add(linkedNode.withBookedClients(linkedNode.getBookedClients())); //save only new free capacity
            addScore(linkedNode, linkedNode.getBookedClients());
            updateNode(linkedNode, hostNode, currentStream, command, newViewerCapacity);
            commands.put(linkedNode.getNodeDescription(), command);
        }
    }

    private void regNode(@Nonnull BookedNode chosenNode, @Nonnull NodeDescription hostNode, @Nonnull Command command) {
        NodeDescription clientDescription = chosenNode.getNodeDescription();
        NodeType hostNodeType = hostNode.getNodeType();

        ViewerDao<NodeDescription> parentViewerDao = getViewerDao(hostNodeType);
        parentViewerDao.addViewer(hostNode, chosenNode.getEncryptedStreamId(), clientDescription);

        getStreamDao().addStream(clientDescription, StreamDescription.builder()
                .encryptedStreamKey(chosenNode.getEncryptedStreamId())
                .commandUid(command.getCommandUid())
                .commandTimestamp(Instant.now().toEpochMilli())
                .maxAvailableClients(chosenNode.getBookedClients())
                .hostNode(hostNode)
                .build());
    }

    private void updateNode(@Nonnull BookedNode clientNode,
                            @Nonnull NodeDescription newHostNode,
                            @Nonnull StreamDescription currentStream,
                            @Nonnull Command command,
                            int newViewerCapacity) {
        NodeDescription clientDescription = clientNode.getNodeDescription();
        NodeDescription oldHostNode = currentStream.getHostNode();
        NodeType oldHostNodeType = oldHostNode.getNodeType();
        NodeType hostNodeType = newHostNode.getNodeType();

        ViewerDao<NodeDescription> oldParentViewerDao = getViewerDao(oldHostNodeType);
        oldParentViewerDao.removeViewers(oldHostNode, currentStream.getEncryptedStreamKey(), ImmutableSet.of(clientDescription));

        ViewerDao<NodeDescription> parentViewerDao = getViewerDao(hostNodeType);
        parentViewerDao.addViewer(newHostNode, clientNode.getEncryptedStreamId(), clientDescription);

        currentStream.setCommandUid(command.getCommandUid());
        currentStream.setCommandTimestamp(Instant.now().toEpochMilli());
        currentStream.setMaxAvailableClients(newViewerCapacity);
        currentStream.setHostNode(newHostNode);
        getStreamDao().addStream(clientNode.getNodeDescription(), currentStream);
    }

    @Nonnull
    @Override
    public ReleaseClients releaseStream(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription node) {
        Optional<StreamDescription> stream = getStreamDao().findStream(node, encryptedStreamKey);
        if (!stream.isPresent()) {
            log.warn("releaseStream(): there isn't streamData on node=[{}] for stream=[{}]", node, encryptedStreamKey);
            return ReleaseClients.builder()
                    .encryptedStreamKey(encryptedStreamKey)
                    .isStreamReleased(true)
                    .nodeDescription(node)
                    .build();
        }
        StreamDescription streamData = stream.get();
        NodeDescription parentNode = streamData.getHostNode();
        ViewerDao<NodeDescription> parentViewerDao = getViewerDao(parentNode.getNodeType());
        parentViewerDao.removeViewers(parentNode, encryptedStreamKey, ImmutableSet.of(node));
        getStreamDao().removeStream(node, encryptedStreamKey);
        Command removeRouteCommand = generateRemoveRouteCommand(encryptedStreamKey);
        Command stopStreamCommand = generateStopStreamCommand(encryptedStreamKey);
        deductScore(node, streamData.getMaxAvailableClients() + 1);

        return ReleaseClients.builder()
                .encryptedStreamKey(encryptedStreamKey)
                .isStreamReleased(true)
                .nodeDescription(node)
                .parentNode(parentNode)
                .commands(ImmutableList.of(removeRouteCommand, stopStreamCommand))
                .build();
    }

    @Nonnull
    @Override
    public ReleaseClients releaseClientCapacity(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription node, int capacity) {
        Optional<StreamDescription> stream = getStreamDao().findStream(node, encryptedStreamKey);
        if (!stream.isPresent()) {
            log.warn("releaseClient(): there isn't streamData on node=[{}] for stream=[{}]", node, encryptedStreamKey);
            return ReleaseClients.builder()
                    .encryptedStreamKey(encryptedStreamKey)
                    .isStreamReleased(true)
                    .nodeDescription(node)
                    .build();
        }
        StreamDescription streamData = stream.get();
        int curMaxClients = streamData.getMaxAvailableClients();
        int newMaxClients = curMaxClients - capacity;
        if (newMaxClients <= 0) {
            if (newMaxClients < 0) {
                log.error("releaseClients(): curMaxClients=[{}], but asked to release=[{}] on node=[{}] for stream=[{}] inconsistent state.",
                        curMaxClients, capacity, node, encryptedStreamKey);
            }
            return releaseStream(encryptedStreamKey, node);
        }
        getStreamDao().addStream(node, streamData.withMaxAvailableClients(newMaxClients));
        NodeDescription hostNodeDescription = streamData.getHostNode();
        Command command = generateUpdateRouteCommand(encryptedStreamKey, hostNodeDescription, newMaxClients);
        deductScore(node, capacity);

        return ReleaseClients.builder()
                .encryptedStreamKey(encryptedStreamKey)
                .isStreamReleased(true)
                .nodeDescription(node)
                .parentNode(hostNodeDescription)
                .commands(ImmutableList.of(command))
                .build();
    }

    private void addScore(@Nonnull BookedNode chosenNode, int scoreDelta) {
        getClusterDao().addScore(chosenNode.getNodeDescription(), clientsToScore(scoreDelta));
    }

    private void deductScore(@Nonnull NodeDescription chosenNode, int scoreDelta) {
        getClusterDao().deductScore(chosenNode, clientsToScore(scoreDelta));
    }

    @Nonnull
    private Command generateAddRouteCommand(@Nonnull BookedNode clientNode, @Nonnull BookedNode hostNode) {
        NodeDescription hostNodeDescription = hostNode.getNodeDescription();
        return AddRoute.builder()
                .encryptedStreamKey(clientNode.getEncryptedStreamId())
                .commandUid(UUID.randomUUID().toString())
                .streamRouteData(StreamRouteData.builder()
                        .encryptedStreamKey(clientNode.getEncryptedStreamId())
                        .maxAvailableClients(clientNode.getBookedClients())
                        .nodeReference(NodeReference.builder()
                                .nodeUid(hostNodeDescription.getNodeUid())
                                .extIp(hostNodeDescription.getExtIp())
                                .region(hostNodeDescription.getRegion())
                                .build())
                        .build())
                .build();
    }

    @Nonnull
    private Command generateUpdateRouteCommand(@Nonnull String encryptedStreamKey, @Nonnull NodeDescription hostNode, int newViewerCapacity) {
        return UpdateRoute.builder()
                .encryptedStreamKey(encryptedStreamKey)
                .commandUid(UUID.randomUUID().toString())
                .streamRouteData(StreamRouteData.builder()
                        .encryptedStreamKey(encryptedStreamKey)
                        .maxAvailableClients(newViewerCapacity)
                        .nodeReference(NodeReference.builder()
                                .nodeUid(hostNode.getNodeUid())
                                .extIp(hostNode.getExtIp())
                                .region(hostNode.getRegion())
                                .build())
                        .build()
                )
                .build();
    }

    @Nonnull
    private Command generateStopStreamCommand(@Nonnull String encryptedStreamKey) {
        return StopStream.builder()
                .commandUid(UUID.randomUUID().toString())
                .encryptedStreamKey(encryptedStreamKey)
                .build();
    }

    @Nonnull
    private Command generateRemoveRouteCommand(@Nonnull String encryptedStreamKey) {
        return RemoveRoute.builder()
                .commandUid(UUID.randomUUID().toString())
                .encryptedStreamKey(encryptedStreamKey)
                .build();
    }

    @Nonnull
    private ViewerDao<NodeDescription> getViewerDao(@Nonnull NodeType nodeType) {
        switch (nodeType) {
            case GATEWAY:
                return gatewayViewerDao;
            case RELAY:
                return relayViewerDao;
            default:
                throw new IllegalArgumentException("Unknown host node type: " + nodeType);
        }
    }
}
