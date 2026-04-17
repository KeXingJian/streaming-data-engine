package com.kxj.streamingdataengine.state;

import java.time.Instant;
import java.util.Map;

/**
 * 状态快照
 */
public record Snapshot(
    String checkpointId,
    Instant timestamp,
    Map<String, byte[]> stateData,
    long stateSize
) {
    public Snapshot {
        if (checkpointId == null || checkpointId.isBlank()) {
            throw new IllegalArgumentException("checkpointId cannot be null or blank");
        }
    }
    
    public Snapshot(String checkpointId, Map<String, byte[]> stateData) {
        this(checkpointId, Instant.now(), stateData, 
             stateData.values().stream().mapToLong(b -> b.length).sum());
    }
}
