package com.kxj.streamingdataengine.checkpoint;

import com.kxj.streamingdataengine.state.Snapshot;
import lombok.Builder;
import lombok.Getter;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Checkpoint 状态
 * 跟踪单个 Checkpoint 的生命周期和状态
 */
@Getter
public class Checkpoint {
    
    private final String checkpointId;
    private final long checkpointNumber;
    private final Instant triggerTimestamp;
    private volatile Instant completedTimestamp;
    private volatile CheckpointStatus status;
    
    // 各算子的快照状态
    private final Map<String, OperatorSnapshotStatus> operatorSnapshots;
    
    // 汇总快照
    private volatile Snapshot consolidatedSnapshot;
    
    public Checkpoint(String checkpointId, long checkpointNumber) {
        this.checkpointId = checkpointId;
        this.checkpointNumber = checkpointNumber;
        this.triggerTimestamp = Instant.now();
        this.status = CheckpointStatus.IN_PROGRESS;
        this.operatorSnapshots = new ConcurrentHashMap<>();
    }
    
    /**
     * 记录算子快照完成
     */
    public void recordOperatorSnapshot(String operatorId, Snapshot snapshot) {
        operatorSnapshots.put(operatorId, 
            new OperatorSnapshotStatus(operatorId, snapshot, Instant.now(), true));
        checkCompletion();
    }
    
    /**
     * 记录算子快照失败
     */
    public void recordOperatorFailure(String operatorId, Throwable error) {
        operatorSnapshots.put(operatorId,
            new OperatorSnapshotStatus(operatorId, null, Instant.now(), false, error));
        status = CheckpointStatus.FAILED;
    }
    
    /**
     * 检查是否所有算子都已完成快照
     */
    private void checkCompletion() {
        boolean allCompleted = operatorSnapshots.values().stream()
            .allMatch(OperatorSnapshotStatus::isSuccess);
        
        if (allCompleted && status == CheckpointStatus.IN_PROGRESS) {
            status = CheckpointStatus.COMPLETED;
            completedTimestamp = Instant.now();
        }
    }
    
    /**
     * 汇总所有算子快照
     */
    public void consolidateSnapshots() {
        // 汇总所有算子的快照数据
        // 实际实现需要合并各个算子的状态
        this.consolidatedSnapshot = null; // 简化实现
    }
    
    /**
     * 获取 Checkpoint 耗时（毫秒）
     */
    public long getDurationMs() {
        Instant end = completedTimestamp != null ? completedTimestamp : Instant.now();
        return end.toEpochMilli() - triggerTimestamp.toEpochMilli();
    }
    
    public enum CheckpointStatus {
        IN_PROGRESS,    // 进行中
        COMPLETED,      // 已完成
        FAILED,         // 失败
        EXPIRED         // 过期
    }
    
    @Getter
    public static class OperatorSnapshotStatus {
        private final String operatorId;
        private final Snapshot snapshot;
        private final Instant completionTime;
        private final boolean success;
        private final Throwable error;
        
        public OperatorSnapshotStatus(String operatorId, Snapshot snapshot, 
                                     Instant completionTime, boolean success) {
            this(operatorId, snapshot, completionTime, success, null);
        }
        
        public OperatorSnapshotStatus(String operatorId, Snapshot snapshot,
                                     Instant completionTime, boolean success, Throwable error) {
            this.operatorId = operatorId;
            this.snapshot = snapshot;
            this.completionTime = completionTime;
            this.success = success;
            this.error = error;
        }
    }
}
