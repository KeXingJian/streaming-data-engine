package com.kxj.streamingdataengine.checkpoint;

import lombok.Builder;
import lombok.Getter;

import java.time.Duration;
import java.time.Instant;

/**
 * Checkpoint 配置
 */
@Getter
@Builder
public class CheckpointConfig {
    
    // 是否启用 Checkpoint
    @Builder.Default
    private boolean enabled = true;
    
    // Checkpoint 间隔（默认 1 分钟）
    @Builder.Default
    private Duration interval = Duration.ofMinutes(1);
    
    // Checkpoint 超时时间（默认 10 分钟）
    @Builder.Default
    private Duration timeout = Duration.ofMinutes(10);
    
    // 两次 Checkpoint 之间的最小间隔（默认 500ms）
    @Builder.Default
    private Duration minPauseBetweenCheckpoints = Duration.ofMillis(500);
    
    // 最大并发 Checkpoint 数（默认 1）
    @Builder.Default
    private int maxConcurrentCheckpoints = 1;
    
    // 是否启用 Unaligned Checkpoint（默认 false，使用对齐模式）
    @Builder.Default
    private boolean unalignedCheckpointsEnabled = false;
    
    // 对齐超时时间（默认 30 秒）
    @Builder.Default
    private Duration alignmentTimeout = Duration.ofSeconds(30);
    
    // 是否启用增量 Checkpoint（默认 true，利用 LSM-Tree 特性）
    @Builder.Default
    private boolean incrementalCheckpointsEnabled = true;
    
    // 外部化 Checkpoint（默认 false）
    @Builder.Default
    private boolean externalizedCheckpointsEnabled = false;
    
    // 失败时是否保留 Checkpoint
    @Builder.Default
    private boolean retainOnCancellation = false;
    
    public static CheckpointConfig defaultConfig() {
        return CheckpointConfig.builder().build();
    }
    
    public static CheckpointConfig disabled() {
        return CheckpointConfig.builder().enabled(false).build();
    }
}
