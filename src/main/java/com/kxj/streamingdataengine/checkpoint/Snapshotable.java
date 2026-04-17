package com.kxj.streamingdataengine.checkpoint;

import com.kxj.streamingdataengine.state.Snapshot;
import com.kxj.streamingdataengine.state.StateBackend;

/**
 * 支持快照的算子接口
 * 所有需要状态恢复的算子都应实现此接口
 */
public interface Snapshotable {
    
    /**
     * 获取算子 ID
     */
    String getOperatorId();
    
    /**
     * 获取算子名称
     */
    default String getOperatorName() {
        return getClass().getSimpleName();
    }
    
    /**
     * 触发快照
     * 
     * @param checkpointId Checkpoint ID
     * @param checkpointNumber Checkpoint 序号
     * @param stateBackend 状态后端
     * @return 算子状态快照
     */
    Snapshot snapshotState(String checkpointId, long checkpointNumber, StateBackend stateBackend);
    
    /**
     * 从快照恢复状态
     * 
     * @param snapshot 状态快照
     */
    void restoreState(Snapshot snapshot);
    
    /**
     * 初始化状态后端
     * 
     * @param stateBackend 状态后端
     */
    void initializeState(StateBackend stateBackend);
}
