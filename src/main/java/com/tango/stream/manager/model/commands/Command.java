package com.tango.stream.manager.model.commands;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.*;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;

import javax.annotation.Nonnull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.MINIMAL_CLASS,
        property = "type"
)
@FieldDefaults(level = AccessLevel.PROTECTED)
@AllArgsConstructor
@SuperBuilder
@Getter
@NoArgsConstructor
public abstract class Command {
    @Nonnull
    @NonNull
    String encryptedStreamKey;
    @Nonnull
    @NonNull
    String commandUid;

    int failedAttempts;

    public int incAndGetFailures() {
        failedAttempts++;
        return failedAttempts;
    }
}