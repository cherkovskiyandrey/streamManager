package com.tango.stream.manager.service.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.tango.stream.manager.BaseKafkaTest;
import com.tango.stream.manager.dao.NodeDao;
import com.tango.stream.manager.model.*;
import com.tango.stream.manager.model.commands.*;
import com.tango.stream.manager.utils.EncryptUtil;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.net.URI;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.verify;

class CommandProcessorImplTest extends BaseKafkaTest {
    public static final String ENCRYPTED_STREAM_KEY = EncryptUtil.encryptStreamKey(StreamKey.builder()
            .initTime(System.currentTimeMillis())
            .streamId(42L)
            .build());
    public static final NodeReference NODE_REFERENCE = NodeReference.builder()
            .extIp("extId")
            .nodeUid("nodeUid")
            .region("eu")
            .build();
    public static final StreamRouteData ROUTE_DATA = StreamRouteData.builder()
            .encryptedStreamKey(ENCRYPTED_STREAM_KEY)
            .nodeReference(NODE_REFERENCE)
            .build();

    @Autowired
    CommandProcessorImpl commandProcessor;
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    NodeDao nodeDao;

    @Value("${commands.kafka.topic}")
    String commandsTopic;

    @Captor
    ArgumentCaptor<URI> uriCaptor;
    @Captor
    ArgumentCaptor<Commands> commandsCaptor;


    @Test
    void shouldSendCommandsToExecutionViaKafka() {
        Multimap<NodeDescription, Command> commands = MultimapBuilder.linkedHashKeys().arrayListValues().build();
        NodeDescription nodeDescription = getNode();

        commands.put(nodeDescription, createStopStreamCommand());
        commands.put(nodeDescription, createRedirectClientsCommand());
        commands.put(nodeDescription, createRemoveRouteCommand());
        commands.put(nodeDescription, createUpdateRouteCommand());
        commands.put(nodeDescription, createAddRouteCommand());

        commandProcessor.executePreserveOrderAsync(commands);

        verify(nodeCommandController, after(20000)).postCommand(uriCaptor.capture(), commandsCaptor.capture());
        assertTrue(uriCaptor.getValue().toString().contains(nodeDescription.getExtIp()));
        assertEquals(commands.size(), commandsCaptor.getValue().getSequentialCommands().size());
    }

    private NodeDescription getNode() {
        NodeDescription nodeDescription = NodeDescription.builder()
                .nodeUid(UUID.randomUUID().toString())
                .extIp("127.0.0.1")
                .region("eu")
                .nodeType(NodeType.EDGE)
                .version("1")
                .build();

        NodeData nodeData = new NodeData();
        nodeData.setUid(nodeDescription.getNodeUid());
        nodeData.setExtIp(nodeDescription.getExtIp());
        nodeData.setNodeType(nodeDescription.getNodeType());
        nodeData.setRegion(nodeDescription.getRegion());
        nodeData.setVersion(nodeDescription.getVersion());
        nodeDao.save(nodeData);

        return nodeDescription;
    }


    private Command createAddRouteCommand() {
        return AddRoute.builder()
                .commandUid(UUID.randomUUID().toString())
                .streamRouteData(ROUTE_DATA)
                .encryptedStreamKey(ENCRYPTED_STREAM_KEY)
                .build();
    }

    private Command createUpdateRouteCommand() {
        return UpdateRoute.builder()
                .commandUid(UUID.randomUUID().toString())
                .streamRouteData(ROUTE_DATA)
                .encryptedStreamKey(ENCRYPTED_STREAM_KEY)
                .build();
    }

    private Command createRemoveRouteCommand() {
        return RemoveRoute.builder()
                .commandUid(UUID.randomUUID().toString())
                .encryptedStreamKey(ENCRYPTED_STREAM_KEY)
                .build();
    }

    private Command createRedirectClientsCommand() {
        return RedirectClients.builder()
                .encryptedStreamKey(ENCRYPTED_STREAM_KEY)
                .commandUid(UUID.randomUUID().toString())
                .redirectData(StreamRedirectData.builder()
                        .encryptedStreamKey(ENCRYPTED_STREAM_KEY)
                        .nodeReference(NODE_REFERENCE)
                        .clients(Collections.emptySet())
                        .build())
                .build();
    }

    private StopStream createStopStreamCommand() {
        return StopStream.builder()
                .commandUid(UUID.randomUUID().toString())
                .encryptedStreamKey(ENCRYPTED_STREAM_KEY)
                .build();
    }
}