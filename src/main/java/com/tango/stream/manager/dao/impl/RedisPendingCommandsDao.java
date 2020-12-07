package com.tango.stream.manager.dao.impl;

import com.tango.stream.manager.dao.PendingCommandsDao;
import com.tango.stream.manager.model.NodeDescription;
import com.tango.stream.manager.model.commands.Command;
import com.tango.stream.manager.service.impl.ConfigurationService;
import org.redisson.api.RDeque;
import org.redisson.api.RedissonClient;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Repository
public class RedisPendingCommandsDao implements PendingCommandsDao {
    public static final TypedJsonJacksonCodec CODEC = new TypedJsonJacksonCodec(Command.class);

    private final RedissonClient redissonClient;
    private final ConfigurationService configurationService;

    @Autowired
    public RedisPendingCommandsDao(RedissonClient redissonClient,
                                   ConfigurationService configurationService) {
        this.redissonClient = redissonClient;
        this.configurationService = configurationService;
    }

    @Override
    public void addCommand(@Nonnull NodeDescription nodeDescription, @Nonnull Command command) {
        RDeque<Command> deque = getDeque(nodeDescription.getNodeUid());
        deque.add(command);
        deque.expireAsync(getCommandsTtlSec(), TimeUnit.SECONDS);
    }

    @Override
    public void addCommands(NodeDescription nodeDescription, Collection<Command> commands) {
        RDeque<Command> deque = getDeque(nodeDescription.getNodeUid());
        deque.addAll(commands);
        deque.expireAsync(getCommandsTtlSec(), TimeUnit.SECONDS);
    }

    @Override
    public List<Command> get(String nodeUuid, int limit) {
        RDeque<Command> deque = getDeque(nodeUuid);
        List<Command> first = deque.pollFirst(limit);
        deque.expireAsync(getCommandsTtlSec(), TimeUnit.SECONDS);
        return first;
    }

    @Override
    public void addFirst(String nodeUuid, List<Command> commands) {
        RDeque<Command> deque = getDeque(nodeUuid);
        deque.addFirstIfExists(commands.toArray(new Command[0]));
        deque.expireAsync(getCommandsTtlSec(), TimeUnit.SECONDS);
    }

    @Override
    public void drain(NodeDescription nodeDescription) {
        getDeque(nodeDescription.getNodeUid()).unlink();
    }

    private RDeque<Command> getDeque(String nodeUid) {
        return redissonClient.getDeque("CMD_QUEUE:" + nodeUid, CODEC);
    }

    private long getCommandsTtlSec() {
        return configurationService.get().getInt("commands.storage.ttl.sec", 300);
    }
}
