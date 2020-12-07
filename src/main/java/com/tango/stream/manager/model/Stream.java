package com.tango.stream.manager.model;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Date;

/**
 * copied from kafka-util
 */
@Data
@NoArgsConstructor
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class Stream {
    private long id;
    private long accountId;
    private String username;
    @Nullable
    private String title;
    private int targetDuration;
    @Nullable
    private Date startTime;
    @Nullable
    private Date endTime;
    @Nullable
    private Date suspendTime;
    @Nullable
    private Boolean isPublic;
    @Nonnull
    private String source;
    @Nullable
    private Long peerId;
    @Nullable
    private Integer peerType;
    @Nullable
    private String thumbnailUrl;
    @Nullable
    private Integer thumbnailWidth;
    @Nullable
    private Integer thumbnailHeight;
    private boolean expired;
    @Nullable
    private Long initTime;
    @Nullable
    private String countryCode;
    @Nullable
    private Integer ticketPrice;
    @Nullable
    private Long uniqueViewerCount;
    private int hidden;
    private int approved;
    private long viewersOnTermination;
    @Nullable
    private String liveListUri;
    @Nullable
    private String completeListUri;
    @Nullable
    private String previewListUri;
}