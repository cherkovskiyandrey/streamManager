package com.tango.stream.manager.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.tango.stream.manager.dao.NodeDao;
import com.tango.stream.manager.dao.PendingCommandsDao;
import com.tango.stream.manager.model.ActionSpec;
import com.tango.stream.manager.model.NodeData;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.commands.Command;
import com.tango.stream.manager.model.commands.Commands;
import com.tango.stream.manager.restClients.NodeCommandController;
import com.tango.stream.manager.service.CommandProcessor;
import com.tango.stream.manager.service.LocksService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class CommandProcessorImpl implements CommandProcessor {

    private final String commandsTopic;
    private final KafkaTemplate<String, String> template;
    private final PendingCommandsDao pendingCommandsDao;
    private final NodeDao nodeDao;
    private final LocksService locksService;
    private final ConfigurationService configurationService;
    private final NodeCommandController nodeCommandController;

    @Autowired
    public CommandProcessorImpl(@Value("${commands.kafka.topic}") String commandsTopic,
                                KafkaTemplate<String, String> template,
                                PendingCommandsDao pendingCommandsDao,
                                NodeDao nodeDao,
                                LocksService locksService,
                                ConfigurationService configurationService,
                                NodeCommandController nodeCommandController) {
        this.commandsTopic = commandsTopic;
        this.template = template;
        this.pendingCommandsDao = pendingCommandsDao;
        this.nodeDao = nodeDao;
        this.locksService = locksService;
        this.configurationService = configurationService;
        this.nodeCommandController = nodeCommandController;
    }

    @Override
    public void executePreserveOrderAsync(@Nonnull Multimap<NodeDescription, Command> commands) {
        commands.asMap().forEach(pendingCommandsDao::addCommands);
        commands.keySet().forEach(nodeDescription -> notifyExecutor(nodeDescription.getNodeUid()));
    }

    private void notifyExecutor(String nodeUid) {
        template.send(commandsTopic, nodeUid, "{}");
        log.debug("Execute command signal sent for node: {}", nodeUid);
    }

    @KafkaListener(
            topics = "${commands.kafka.topic}"
    )
    public void executeListener(ConsumerRecord<String, String> cr) throws Exception {
        String nodeUuid = cr.key();
        log.info("Got execute signal for node: {}", nodeUuid);
        locksService.doUnderLock(ActionSpec.builder()
                .lockNames(ImmutableList.of("exec." + nodeUuid))
                .leaseTimeMs(getLockLeaseTimeMs())
                .waitTimeMs(getLockWaitTimeMs())
                .action(() -> fetchAndSendCommands(nodeUuid))
                .build()
        );
    }

    //TODO: we have to send eventually until we sent or commands have been canceled
    private void fetchAndSendCommands(String nodeUuid) {
        int batchSize = getExecuteBatchSize();
        List<Command> commands = pendingCommandsDao.get(nodeUuid, batchSize);
        try {
            sendCommands(nodeUuid, commands);
            log.info("Commands sent successfully, node={}, count={}", nodeUuid, commands.size());
        } catch (Exception e) {
            log.error("Cannot send commands, node={}", nodeUuid, e);
            List<Command> retryCommands = getRetryableCommands(nodeUuid, commands);
            pendingCommandsDao.addFirst(nodeUuid, retryCommands);
            log.info("Commands requeued: node={}, count={}", nodeUuid, commands.size());
            notifyExecutor(nodeUuid);
        }
    }

    @NotNull
    private List<Command> getRetryableCommands(String nodeUuid, List<Command> commands) {
        int maxFailures = getMaxFailures();
        List<Command> retryCommands = new ArrayList<>(commands.size());
        for (Command command : commands) {
            int fails = command.incAndGetFailures();
            if (fails >= maxFailures) {
                log.warn("Command reached max failures count and will be discarded: nodeUuid={}, command={}", nodeUuid, command);
            } else {
                retryCommands.add(command);
            }
        }
        return retryCommands;
    }

    private void sendCommands(String nodeUuid, List<Command> commands) {
        NodeData node = nodeDao.getNodeById(nodeUuid);
        if (node == null) {
            log.warn("Node not found, commands skipped: uuid={}, count={}", nodeUuid, commands.size());
            return;
        }
        try {
            nodeCommandController.postCommand(buildNodeUri(node), new Commands(commands));
        } catch (URISyntaxException e) {
            log.error("Invalid uri for node: node={}", node, e);
        }
    }

    @NotNull
    private URI buildNodeUri(NodeData node) throws URISyntaxException {
        return new URI("http://" + node.getExtIp() + ":" + getNodePort());
    }

    @Override
    public void cancelAllCommands(@Nonnull NodeDescription nodeDescription) {
        log.warn("All pending commands will be removed for node: uid={}", nodeDescription.getNodeUid());
        pendingCommandsDao.drain(nodeDescription);
    }

    private int getExecuteBatchSize() {
        return configurationService.get().getInt("commands.execute.batch.size", 10);
    }

    private String getNodePort() {
        return configurationService.get().getString("commands.node.port", "8080");
    }

    private long getLockLeaseTimeMs() {
        return configurationService.get().getInt("commands.execute.lock.lease.ms", 10000);
    }

    private int getMaxFailures() {
        return configurationService.get().getInt("commands.failures.max", 5);
    }

    private long getLockWaitTimeMs() {
        return configurationService.get().getInt("commands.execute.lock.wait.ms", 10000);
    }
}
