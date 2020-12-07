package com.tango.stream.manager.model;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RelayItem {
    @NonNull
    String uid;
    @NonNull
    String extIp;
}
