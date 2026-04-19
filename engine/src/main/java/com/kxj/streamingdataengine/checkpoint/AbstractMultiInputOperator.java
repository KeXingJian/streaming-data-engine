package com.kxj.streamingdataengine.checkpoint;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.operator.StreamOperator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 支持多输入的算子基类
 * 
 * 参考 Flink 的 AbstractStreamOperator 设计。
 * 为 union、join、coGroup 等多输入场景提供 Barrier 对齐支持。
 * 
 * 单输入算子可以直接继承此类，使用默认的一个 InputChannel。
 */
@Slf4j
public abstract class AbstractMultiInputOperator<T> implements StreamOperator<T>, Snapshotable {
    
    @Getter
    protected final String operatorId;
    
    @Getter
    protected final String name;
    
    // 输入通道列表
    protected final List<InputChannel> inputChannels;
    
    // Barrier 对齐处理器
    protected AligningBarrierHandler barrierHandler;
    
    // 当前 Checkpoint ID（对齐完成后设置）
    protected volatile long currentCheckpointId = -1;
    
    /**
     * 创建单输入算子（默认）
     */
    public AbstractMultiInputOperator(String operatorId, String name) {
        this(operatorId, name, List.of(new InputChannel("default")));
    }
    
    /**
     * 创建多输入算子
     */
    public AbstractMultiInputOperator(String operatorId, String name, List<InputChannel> inputChannels) {
        this.operatorId = operatorId;
        this.name = name;
        this.inputChannels = new ArrayList<>(inputChannels);
        
        // 初始化 Barrier 对齐处理器
        initializeBarrierHandler();
    }
    
    /**
     * 初始化 Barrier Handler
     */
    protected void initializeBarrierHandler() {
        this.barrierHandler = new AligningBarrierHandler(
            inputChannels,
            (checkpointId, bufferedRecords) -> {
                // 对齐完成后触发快照
                log.info("[kxj: 算子 Barrier 对齐完成] operator={} checkpoint={}", 
                        name, checkpointId);
                
                this.currentCheckpointId = checkpointId;
                
                // 触发算子快照
                performSnapshot(checkpointId);
                
                // 处理缓存的数据（可选：继续处理或丢弃）
                processBufferedRecords(bufferedRecords);
            }
        );
    }
    
    /**
     * 处理元素（带 Barrier 对齐支持）
     * 
     * 子类应该覆盖 processElementInternal 而不是此方法。
     */
    @Override
    @SuppressWarnings("unchecked")
    public final List<StreamRecord<T>> processElement(StreamRecord<T> record) {
        // 1. 检查是否是 Checkpoint Barrier
        if (record.value() instanceof CheckpointBarrier barrier) {
            // 默认使用第一个输入通道（单输入场景）
            InputChannel channel = inputChannels.get(0);
            AligningBarrierHandler.BarrierProcessResult result = 
                barrierHandler.onBarrier(barrier, channel);
            
            if (result.isBuffer()) {
                // 需要等待对齐，返回空（数据已缓存）
                return List.of();
            } else if (result.isAligned()) {
                // 对齐完成，继续处理后续数据
                log.debug("[kxj: 算子 Barrier 对齐后继续处理] operator={}", name);
            }
            
            return List.of(); // Barrier 不产生输出
        }
        
        // 2. 普通数据记录，检查是否需要缓存
        InputChannel channel = inputChannels.get(0);
        boolean shouldProceed = barrierHandler.onRecord(record, channel);
        
        if (!shouldProceed) {
            // 数据已缓存，不继续处理
            return List.of();
        }
        
        // 3. 正常处理
        return processElementInternal(record);
    }
    
    /**
     * 子类实现：处理单条数据记录
     */
    protected abstract List<StreamRecord<T>> processElementInternal(StreamRecord<T> record);
    
    /**
     * 处理来自特定输入通道的元素（多输入场景）
     * 
     * @param record 数据记录
     * @param channel 输入通道
     * @return 处理结果
     */
    public <R> List<StreamRecord<T>> processElement(StreamRecord<R> record, InputChannel channel) {
        // 类型转换并处理
        @SuppressWarnings("unchecked")
        StreamRecord<T> castedRecord = (StreamRecord<T>) record;
        return processElement(castedRecord);
    }
    
    /**
     * 处理 Checkpoint Barrier（指定输入通道）
     * 
     * 多输入算子（如 union）应该调用此方法
     */
    public AligningBarrierHandler.BarrierProcessResult processBarrier(
            CheckpointBarrier barrier, InputChannel channel) {
        return barrierHandler.onBarrier(barrier, channel);
    }
    
    /**
     * 执行快照
     */
    protected void performSnapshot(long checkpointId) {
        // 子类可以覆盖此方法添加自定义快照逻辑
        log.debug("[kxj: 算子执行快照] operator={} checkpoint={}", name, checkpointId);
    }
    
    /**
     * 处理对齐期间缓存的数据
     * 
     * 默认行为：继续处理这些数据
     * 子类可以覆盖以自定义行为（如丢弃）
     */
    protected void processBufferedRecords(Map<InputChannel, List<StreamRecord<?>>> bufferedRecords) {
        int totalRecords = bufferedRecords.values().stream()
            .mapToInt(List::size)
            .sum();
        
        if (totalRecords > 0) {
            log.debug("[kxj: 处理对齐缓存数据] operator={} records={}", name, totalRecords);
            
            // 继续处理缓存的数据
            for (Map.Entry<InputChannel, List<StreamRecord<?>>> entry : bufferedRecords.entrySet()) {
                for (StreamRecord<?> record : entry.getValue()) {
                    @SuppressWarnings("unchecked")
                    StreamRecord<T> castedRecord = (StreamRecord<T>) record;
                    processElementInternal(castedRecord);
                }
            }
        }
    }
    
    /**
     * 获取对齐状态（用于监控）
     */
    public AligningBarrierHandler.AlignmentStatus getAlignmentStatus() {
        return barrierHandler.getStatus();
    }
    
    @Override
    public void open() {
        log.info("[kxj: 多输入算子打开] operator={} channels={}", name, inputChannels.size());
    }
    
    @Override
    public void close() {
        log.info("[kxj: 多输入算子关闭] operator={}", name);
    }
}
