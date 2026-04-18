package com.kxj.streamingdataengine.core.watermark;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 分区级 Watermark 管理器
 * 
 * 核心思想（参考 Flink）：
 * 1. 每个数据源分区维护独立的 Watermark
 * 2. 全局 Watermark = min(所有活跃分区的 Watermark)
 * 3. 空闲分区检测：长时间无数据的分区标记为 idle，不参与最小值计算
 * 
 * 为什么取最小值？
 * - 保证所有分区都推进到该时间点，才不会丢失数据
 * - 避免"快分区"掩盖"慢分区"的延迟
 */
@Slf4j
public class PartitionedWatermarkManager {
    
    // 每个分区的 Watermark（分区ID -> Watermark时间戳）
    private final Map<Integer, PartitionWatermark> partitionWatermarks = new ConcurrentHashMap<>();
    
    // 全局 Watermark（所有活跃分区的最小值）
    @Getter
    private volatile long globalWatermark = Long.MIN_VALUE;
    
    // 空闲检测配置
    private final long idleTimeoutMillis;
    
    // 是否启用空闲检测
    private final boolean idleDetectionEnabled;
    
    // 监听器
    private final WatermarkUpdateListener listener;
    
    public PartitionedWatermarkManager() {
        this(60000, true, null); // 默认 60 秒空闲超时
    }
    
    public PartitionedWatermarkManager(long idleTimeoutMillis, 
                                      boolean idleDetectionEnabled,
                                      WatermarkUpdateListener listener) {
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.idleDetectionEnabled = idleDetectionEnabled;
        this.listener = listener;
    }
    
    /**
     * 更新指定分区的 Watermark
     * 
     * @param partitionId 分区 ID
     * @param watermark 新的 Watermark 时间戳
     * @return 是否更新了全局 Watermark
     */
    public boolean updatePartitionWatermark(int partitionId, long watermark) {
        PartitionWatermark pw = partitionWatermarks.computeIfAbsent(
            partitionId, 
            id -> new PartitionWatermark(id, Long.MIN_VALUE)
        );
        
        // 只接受递增的 Watermark
        if (watermark <= pw.getWatermark()) {
            return false; // 乱序或重复，忽略
        }
        
        // 更新分区 Watermark
        pw.updateWatermark(watermark);
        
        // 重新计算全局 Watermark
        return recalculateGlobalWatermark();
    }
    
    /**
     * 获取指定分区的当前 Watermark
     */
    public long getPartitionWatermark(int partitionId) {
        PartitionWatermark pw = partitionWatermarks.get(partitionId);
        return pw != null ? pw.getWatermark() : Long.MIN_VALUE;
    }
    
    /**
     * 标记分区为空闲（idle）
     * 
     * 当某个分区长时间无数据时调用，该分区不再参与全局 Watermark 计算
     */
    public void markPartitionIdle(int partitionId) {
        PartitionWatermark pw = partitionWatermarks.get(partitionId);
        if (pw != null && !pw.isIdle()) {
            pw.markIdle();
            log.info("[kxj: 分区标记为空闲] partition={}", partitionId);
            
            // 重新计算全局 Watermark（该分区被排除）
            recalculateGlobalWatermark();
        }
    }
    
    /**
     * 标记分区为活跃
     * 
     * 当空闲分区收到新数据时调用
     */
    public void markPartitionActive(int partitionId) {
        PartitionWatermark pw = partitionWatermarks.get(partitionId);
        if (pw != null && pw.isIdle()) {
            pw.markActive();
            log.info("[kxj: 分区恢复活跃] partition={}", partitionId);
        }
    }
    
    /**
     * 重新计算全局 Watermark
     * 
     * @return 是否发生了变化
     */
    private synchronized boolean recalculateGlobalWatermark() {
        long newGlobalWatermark = Long.MAX_VALUE;
        int activePartitions = 0;
        
        for (PartitionWatermark pw : partitionWatermarks.values()) {
            if (!pw.isIdle()) {
                newGlobalWatermark = Math.min(newGlobalWatermark, pw.getWatermark());
                activePartitions++;
            }
        }
        
        // 如果没有活跃分区，保持当前值
        if (activePartitions == 0) {
            return false;
        }
        
        // 如果没有分区，使用 MIN_VALUE
        if (newGlobalWatermark == Long.MAX_VALUE) {
            newGlobalWatermark = Long.MIN_VALUE;
        }
        
        // 检查是否变化
        if (newGlobalWatermark > globalWatermark) {
            long oldWatermark = globalWatermark;
            globalWatermark = newGlobalWatermark;
            
            log.debug("[kxj: 全局 Watermark 更新] {} -> {} (活跃分区: {})",
                    oldWatermark, newGlobalWatermark, activePartitions);
            
            if (listener != null) {
                listener.onGlobalWatermarkUpdate(oldWatermark, newGlobalWatermark);
            }
            
            return true;
        }
        
        return false;
    }
    
    /**
     * 检测并处理空闲分区
     * 
     * 应该定期调用（如每秒一次）
     */
    public void detectIdlePartitions() {
        if (!idleDetectionEnabled) {
            return;
        }
        
        long now = System.currentTimeMillis();
        
        for (PartitionWatermark pw : partitionWatermarks.values()) {
            if (!pw.isIdle() && pw.isIdle(now, idleTimeoutMillis)) {
                markPartitionIdle(pw.getPartitionId());
            }
        }
    }
    
    /**
     * 获取所有分区状态（用于监控）
     */
    public Map<Integer, PartitionStatus> getAllPartitionStatus() {
        Map<Integer, PartitionStatus> status = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, PartitionWatermark> entry : partitionWatermarks.entrySet()) {
            PartitionWatermark pw = entry.getValue();
            status.put(entry.getKey(), new PartitionStatus(
                pw.getPartitionId(),
                pw.getWatermark(),
                pw.isIdle(),
                pw.getLastUpdateTime()
            ));
        }
        return status;
    }
    
    /**
     * 获取活跃分区数
     */
    public int getActivePartitionCount() {
        int count = 0;
        for (PartitionWatermark pw : partitionWatermarks.values()) {
            if (!pw.isIdle()) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * 获取总分区数
     */
    public int getTotalPartitionCount() {
        return partitionWatermarks.size();
    }
    
    /**
     * 注册分区（初始化）
     */
    public void registerPartition(int partitionId) {
        partitionWatermarks.computeIfAbsent(
            partitionId,
            id -> {
                log.info("[kxj: 注册分区 Watermark] partition={}", id);
                return new PartitionWatermark(id, Long.MIN_VALUE);
            }
        );
    }
    
    /**
     * 注销分区
     */
    public void unregisterPartition(int partitionId) {
        PartitionWatermark removed = partitionWatermarks.remove(partitionId);
        if (removed != null) {
            log.info("[kxj: 注销分区 Watermark] partition={}", partitionId);
            recalculateGlobalWatermark();
        }
    }
    
    // ===== 内部类 =====
    
    /**
     * 单个分区的 Watermark 状态
     */
    private static class PartitionWatermark {
        @Getter
        private final int partitionId;
        
        @Getter
        private volatile long watermark;
        
        @Getter
        private volatile long lastUpdateTime;
        
        @Getter
        private volatile boolean idle = false;
        
        PartitionWatermark(int partitionId, long initialWatermark) {
            this.partitionId = partitionId;
            this.watermark = initialWatermark;
            this.lastUpdateTime = System.currentTimeMillis();
        }
        
        void updateWatermark(long newWatermark) {
            this.watermark = newWatermark;
            this.lastUpdateTime = System.currentTimeMillis();
            this.idle = false; // 更新 Watermark 时自动恢复活跃
        }
        
        void markIdle() {
            this.idle = true;
        }
        
        void markActive() {
            this.idle = false;
            this.lastUpdateTime = System.currentTimeMillis();
        }
        
        boolean isIdle(long currentTime, long timeoutMillis) {
            return (currentTime - lastUpdateTime) > timeoutMillis;
        }
    }
    
    /**
     * 分区状态（监控用）
     */
    public record PartitionStatus(
        int partitionId,
        long watermark,
        boolean idle,
        long lastUpdateTime
    ) {
        public long getIdleDuration(long currentTime) {
            return currentTime - lastUpdateTime;
        }
    }
    
    /**
     * 监听器接口
     */
    public interface WatermarkUpdateListener {
        void onGlobalWatermarkUpdate(long oldWatermark, long newWatermark);
    }
}
