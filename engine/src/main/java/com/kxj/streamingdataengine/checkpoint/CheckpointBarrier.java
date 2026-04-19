package com.kxj.streamingdataengine.checkpoint;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

/**
 * Checkpoint Barrier - 检查点屏障
 * 
 * 在数据流中插入的特殊标记，用于触发 Checkpoint。
 * 所有算子收到 Barrier 后，会保存当前状态，然后继续处理后续数据。
 * 
 * 借鉴 Flink 的 Barrier 机制：
 * 1. Barrier 将数据流分为 "当前 Checkpoint" 和 "下一个 Checkpoint" 两部分
 * 2. 算子收到 Barrier 后，对 Barrier 之前的数据状态做快照
 * 3. 支持 Exactly-Once 语义（对齐模式）
 */
@Getter
public class CheckpointBarrier implements StreamRecord.SpecialRecord {
    
    private final String checkpointId;
    private final long checkpointNumber;
    private final Instant timestamp;
    private final long barrierTimestamp;
    private final int priority; // 优先级，用于处理多个 Barrier 的情况
    
    public CheckpointBarrier(long checkpointNumber) {
        this(checkpointNumber, System.currentTimeMillis());
    }
    
    public CheckpointBarrier(long checkpointNumber, long barrierTimestamp) {
        this.checkpointId = UUID.randomUUID().toString();
        this.checkpointNumber = checkpointNumber;
        this.timestamp = Instant.now();
        this.barrierTimestamp = barrierTimestamp;
        this.priority = 0;
    }
    
    public CheckpointBarrier(String checkpointId, long checkpointNumber, 
                           Instant timestamp, long barrierTimestamp, int priority) {
        this.checkpointId = checkpointId;
        this.checkpointNumber = checkpointNumber;
        this.timestamp = timestamp;
        this.barrierTimestamp = barrierTimestamp;
        this.priority = priority;
    }
    
    /**
     * 创建对齐后的 Barrier（用于多输入算子）
     */
    public CheckpointBarrier alignWith(CheckpointBarrier other) {
        return new CheckpointBarrier(
            this.checkpointId,
            Math.max(this.checkpointNumber, other.checkpointNumber),
            this.timestamp,
            Math.max(this.barrierTimestamp, other.barrierTimestamp),
            this.priority + 1
        );
    }
    
    @Override
    public String toString() {
        return String.format("CheckpointBarrier#%d(id=%s, time=%s)", 
                           checkpointNumber, checkpointId.substring(0, 8), timestamp);
    }
}
