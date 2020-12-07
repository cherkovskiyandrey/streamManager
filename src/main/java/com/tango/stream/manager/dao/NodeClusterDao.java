package com.tango.stream.manager.dao;

import com.google.common.annotations.VisibleForTesting;
import com.tango.stream.manager.model.NodeDescription;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface NodeClusterDao {

    void addNodeToCluster(@Nonnull NodeDescription nodeDescription, long score);

    void removeNodeFromCluster(@Nonnull NodeDescription nodeDescription);

    @Nonnull
    Collection<NodeDescription> getCluster(@Nonnull String region, @Nonnull String version);

    @Nonnull
    List<NodeDescription> getCluster(@Nonnull String region, @Nonnull String version, int offset, int size);

    @Nonnull
    Collection<NodeDescription> getClusterByScores(@Nonnull String region, @Nonnull String version, long fromScoreInclude, long toScoreExclude);

    boolean isEmpty(@Nonnull String region, @Nonnull String version);

    @Nonnull
    Optional<Long> getScore(@Nonnull NodeDescription nodeDescription);

    void addScore(@Nonnull NodeDescription nodeDescription, long scoreDelta);

    void deductScore(@Nonnull NodeDescription nodeDescription, long scoreDelta);

    @VisibleForTesting
    void removeAll(@Nonnull String region, @Nonnull String version);
}
