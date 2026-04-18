package com.kxj.streamingdataengine.checkpoint;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Checkpoint Barrier 对齐处理器
 * 
 * 核心思想（参考 Flink）：
 * 1. 多输入算子为每个输入维护一个 InputChannel
 * 2. 收到 Barrier 后，缓存该输入的后续数据
 * 3. 当所有输入都收到相同 Checkpoint 的 Barrier 后，触发快照
 * 4. 快照完成后，释放所有缓存的数据继续处理
 * 
 * 为什么需要对齐？
 * - 保证快照时，所有输入都处理到 Barrier 对应的时间点
 * - 避免快照包含 Barrier 之后的数据（不一致）
 */
@Slf4j
public class AligningBarrierHandler {
    
    private final List<InputChannel> inputChannels;
    private final BarrierHandlerListener listener;
    
    // 每个通道的状态
    private final Map<InputChannel, ChannelState> channelStates;
    
    // 当前正在处理的 Checkpoint
    private volatile long currentCheckpointId = -1;
    
    // 是否正在等待对齐
    private volatile boolean isAligning = false;
    
    public AligningBarrierHandler(List<InputChannel> inputChannels, BarrierHandlerListener listener) {
        this.inputChannels = new ArrayList<>(inputChannels);
        this.listener = listener;
        this.channelStates = new ConcurrentHashMap<>();
        
        // 初始化每个通道的状态
        for (InputChannel channel : inputChannels) {
            channelStates.put(channel, new ChannelState());
        }
    }
    
    /**
     * 处理收到的 Checkpoint Barrier
     * 
     * @param barrier 收到的 Barrier
     * @param channel 来自哪个输入通道
     * @return BarrierProcessResult 指示如何处理后续数据
     */
    public BarrierProcessResult onBarrier(CheckpointBarrier barrier, InputChannel channel) {
        long checkpointId = barrier.getCheckpointNumber();
        
        log.debug("[kxj: Barrier 到达] checkpoint={} channel={}", checkpointId, channel.getName());
        
        ChannelState state = channelStates.get(channel);
        if (state == null) {
            log.error("[kxj: 未知输入通道] channel={}", channel);
            return BarrierProcessResult.proceed(); // 继续处理
        }
        
        synchronized (this) {
            // 如果正在等待旧的 Checkpoint，新的 Barrier 到达，说明旧 Checkpoint 超时或失败
            // 取消旧的对齐，开始新的
            if (isAligning && checkpointId > currentCheckpointId) {
                log.warn("[kxj: 新的 Checkpoint 打断旧对齐] old={}, new={}", 
                        currentCheckpointId, checkpointId);
                abortAlignment();
            }
            
            // 标记该通道收到 Barrier
            state.setBarrierReceived(true);
            state.setReceivedBarrierId(checkpointId);
            
            // 如果还没开始对齐，标记开始
            if (!isAligning) {
                isAligning = true;
                currentCheckpointId = checkpointId;
                log.info("[kxj: 开始 Barrier 对齐] checkpoint={}", checkpointId);
            }
            
            // 检查是否所有通道都收到了 Barrier
            if (checkAllBarriersReceived(checkpointId)) {
                // 对齐完成！触发快照
                log.info("[kxj: Barrier 对齐完成] checkpoint={}", checkpointId);
                
                // 通知监听器触发快照
                if (listener != null) {
                    listener.onBarrierAligned(checkpointId, getBufferedRecords());
                }
                
                // 重置状态，准备下一个 Checkpoint
                resetAlignment();
                
                return BarrierProcessResult.alignedAndProceed(checkpointId);
            } else {
                // 还需要等待其他通道
                int receivedCount = countReceivedBarriers(checkpointId);
                log.debug("[kxj: 等待其他通道 Barrier] checkpoint={} received={}/total={}", 
                        checkpointId, receivedCount, inputChannels.size());
                
                return BarrierProcessResult.buffer(); // 缓存后续数据
            }
        }
    }
    
    /**
     * 处理普通数据记录（非 Barrier）
     * 
     * @param record 数据记录
     * @param channel 来自哪个输入通道
     * @return 是否继续处理该记录
     */
    public <T> boolean onRecord(StreamRecord<T> record, InputChannel channel) {
        ChannelState state = channelStates.get(channel);
        if (state == null) {
            return true; // 继续处理
        }
        
        // 如果该通道已经收到 Barrier，需要缓存后续数据
        if (state.isBarrierReceived()) {
            state.bufferRecord(record);
            log.trace("[kxj: 缓存 Barrier 后数据] channel={} checkpoint={}", 
                    channel.getName(), currentCheckpointId);
            return false; // 不继续处理，已缓存
        }
        
        return true; // 继续正常处理
    }
    
    /**
     * 获取所有缓存的数据（按通道组织）
     */
    public Map<InputChannel, List<StreamRecord<?>>> getBufferedRecords() {
        Map<InputChannel, List<StreamRecord<?>>> result = new HashMap<>();
        for (Map.Entry<InputChannel, ChannelState> entry : channelStates.entrySet()) {
            result.put(entry.getKey(), entry.getValue().drainBufferedRecords());
        }
        return result;
    }
    
    /**
     * 检查是否所有通道都收到了指定 Checkpoint 的 Barrier
     */
    private boolean checkAllBarriersReceived(long checkpointId) {
        for (ChannelState state : channelStates.values()) {
            if (!state.isBarrierReceived() || state.getReceivedBarrierId() != checkpointId) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * 统计已收到 Barrier 的通道数
     */
    private int countReceivedBarriers(long checkpointId) {
        int count = 0;
        for (ChannelState state : channelStates.values()) {
            if (state.isBarrierReceived() && state.getReceivedBarrierId() == checkpointId) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * 中止当前对齐（如超时或取消）
     */
    public synchronized void abortAlignment() {
        log.warn("[kxj: 中止 Barrier 对齐] checkpoint={}", currentCheckpointId);
        
        // 释放所有缓存的数据（继续处理）
        for (ChannelState state : channelStates.values()) {
            state.drainBufferedRecords(); // 丢弃缓存
        }
        
        resetAlignment();
    }
    
    /**
     * 重置对齐状态
     */
    private synchronized void resetAlignment() {
        isAligning = false;
        currentCheckpointId = -1;
        
        for (ChannelState state : channelStates.values()) {
            state.reset();
        }
    }
    
    /**
     * 获取当前对齐状态（用于监控）
     */
    public AlignmentStatus getStatus() {
        return new AlignmentStatus(
            isAligning,
            currentCheckpointId,
            countReceivedBarriers(currentCheckpointId),
            inputChannels.size(),
            getTotalBufferedRecords()
        );
    }
    
    private int getTotalBufferedRecords() {
        return channelStates.values().stream()
            .mapToInt(ChannelState::getBufferedCount)
            .sum();
    }
    
    // ===== 内部类 =====
    
    /**
     * 单个通道的状态
     */
    private static class ChannelState {
        private volatile boolean barrierReceived = false;
        private volatile long receivedBarrierId = -1;
        private final LinkedBlockingQueue<StreamRecord<?>> buffer = new LinkedBlockingQueue<>();
        
        void setBarrierReceived(boolean received) {
            this.barrierReceived = received;
        }
        
        boolean isBarrierReceived() {
            return barrierReceived;
        }
        
        void setReceivedBarrierId(long id) {
            this.receivedBarrierId = id;
        }
        
        long getReceivedBarrierId() {
            return receivedBarrierId;
        }
        
        void bufferRecord(StreamRecord<?> record) {
            buffer.offer(record);
        }
        
        List<StreamRecord<?>> drainBufferedRecords() {
            List<StreamRecord<?>> records = new ArrayList<>();
            buffer.drainTo(records);
            return records;
        }
        
        int getBufferedCount() {
            return buffer.size();
        }
        
        void reset() {
            barrierReceived = false;
            receivedBarrierId = -1;
            buffer.clear();
        }
    }
    
    /**
     * 处理结果
     */
    public static class BarrierProcessResult {
        public enum Action {
            PROCEED,           // 继续正常处理
            BUFFER,            // 缓存数据，等待对齐
            ALIGNED_PROCEED    // 对齐完成，继续处理
        }
        
        private final Action action;
        private final long alignedCheckpointId;
        
        private BarrierProcessResult(Action action, long checkpointId) {
            this.action = action;
            this.alignedCheckpointId = checkpointId;
        }
        
        public static BarrierProcessResult proceed() {
            return new BarrierProcessResult(Action.PROCEED, -1);
        }
        
        public static BarrierProcessResult buffer() {
            return new BarrierProcessResult(Action.BUFFER, -1);
        }
        
        public static BarrierProcessResult alignedAndProceed(long checkpointId) {
            return new BarrierProcessResult(Action.ALIGNED_PROCEED, checkpointId);
        }
        
        public Action getAction() {
            return action;
        }
        
        public long getAlignedCheckpointId() {
            return alignedCheckpointId;
        }
        
        public boolean isProceed() {
            return action == Action.PROCEED || action == Action.ALIGNED_PROCEED;
        }
        
        public boolean isBuffer() {
            return action == Action.BUFFER;
        }
        
        public boolean isAligned() {
            return action == Action.ALIGNED_PROCEED;
        }
    }
    
    /**
     * 对齐状态（监控用）
     */
    public record AlignmentStatus(
        boolean isAligning,
        long currentCheckpointId,
        int receivedBarriers,
        int totalChannels,
        int bufferedRecords
    ) {
        public boolean isComplete() {
            return receivedBarriers == totalChannels;
        }
        
        public double getProgress() {
            return totalChannels == 0 ? 0 : (double) receivedBarriers / totalChannels;
        }
    }
    
    /**
     * 监听器接口
     */
    public interface BarrierHandlerListener {
        /**
         * 当 Barrier 对齐完成时调用
         * 
         * @param checkpointId Checkpoint ID
         * @param bufferedRecords 对齐期间缓存的数据（可用于后续处理或丢弃）
         */
        void onBarrierAligned(long checkpointId, Map<InputChannel, List<StreamRecord<?>>> bufferedRecords);
    }
}
