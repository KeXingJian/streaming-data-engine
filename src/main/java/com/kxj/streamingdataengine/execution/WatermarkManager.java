package com.kxj.streamingdataengine.execution;

import com.kxj.streamingdataengine.core.model.Watermark;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Watermark管理器
 * 管理多分区Watermark，处理乱序数据
 *
 * 借鉴Flink的Watermark机制：
 * 1. 每个分区独立追踪Watermark
 * 2. 全局Watermark取最小值（保证正确性）
 * 3. 支持空闲分区检测
 */
@Slf4j
public class WatermarkManager {

    /**
     * 各分区的Watermark
     */
    private final Map<Integer, PartitionWatermark> partitionWatermarks;

    /**
     * 空闲超时时间（毫秒）
     */
    private static final long IDLE_TIMEOUT_MS = 60000;

    /**
     * 全局Watermark
     */
    private final AtomicLong globalWatermark;

    public WatermarkManager() {
        this.partitionWatermarks = new ConcurrentHashMap<>();
        this.globalWatermark = new AtomicLong(Long.MIN_VALUE);
    }

    /**
     * 更新分区Watermark
     */
    public void updateWatermark(Watermark watermark) {
        if (watermark == null) {
            return;
        }

        int partition = watermark.getPartitionId();
        if (partition < 0) {
            // 全局Watermark
            updateGlobalWatermark(watermark.getTimestamp());
            return;
        }

        PartitionWatermark pw = partitionWatermarks.computeIfAbsent(
                partition, k -> new PartitionWatermark());

        long oldTimestamp = pw.timestamp.get();
        if (watermark.getTimestamp() > oldTimestamp) {
            if (pw.timestamp.compareAndSet(oldTimestamp, watermark.getTimestamp())) {
                pw.lastUpdateTime = System.currentTimeMillis();
                updateGlobalWatermark();
            }
        }
    }

    /**
     * 更新全局Watermark
     * 取所有非空闲分区Watermark的最小值
     */
    private void updateGlobalWatermark() {
        long minWatermark = Long.MAX_VALUE;
        long now = System.currentTimeMillis();

        for (PartitionWatermark pw : partitionWatermarks.values()) {
            // 跳过空闲分区
            if (now - pw.lastUpdateTime > IDLE_TIMEOUT_MS) {
                continue;
            }

            long partitionWatermark = pw.timestamp.get();
            if (partitionWatermark < minWatermark) {
                minWatermark = partitionWatermark;
            }
        }

        if (minWatermark != Long.MAX_VALUE &&
            minWatermark > globalWatermark.get()) {
            globalWatermark.set(minWatermark);
            log.debug("Global watermark updated to {}", minWatermark);
        }
    }

    private void updateGlobalWatermark(long timestamp) {
        long oldWatermark = globalWatermark.get();
        if (timestamp > oldWatermark) {
            globalWatermark.set(timestamp);
        }
    }

    /**
     * 获取当前全局Watermark
     */
    public long getCurrentWatermark() {
        return globalWatermark.get();
    }

    /**
     * 获取指定分区的Watermark
     */
    public long getPartitionWatermark(int partition) {
        PartitionWatermark pw = partitionWatermarks.get(partition);
        return pw != null ? pw.timestamp.get() : Long.MIN_VALUE;
    }

    /**
     * 检查事件是否迟到
     */
    public boolean isLate(long eventTimestamp) {
        return eventTimestamp < globalWatermark.get();
    }

    /**
     * 获取迟到时间（相对于当前Watermark）
     */
    public long getLateness(long eventTimestamp) {
        return globalWatermark.get() - eventTimestamp;
    }

    /**
     * 注册分区
     */
    public void registerPartition(int partition) {
        partitionWatermarks.computeIfAbsent(partition, k -> new PartitionWatermark());
        log.info("Registered partition {} for watermark tracking", partition);
    }

    /**
     * 注销分区
     */
    public void unregisterPartition(int partition) {
        partitionWatermarks.remove(partition);
        updateGlobalWatermark();
        log.info("Unregistered partition {} from watermark tracking", partition);
    }

    /**
     * 标记分区为空闲
     */
    public void markIdle(int partition) {
        PartitionWatermark pw = partitionWatermarks.get(partition);
        if (pw != null) {
            pw.lastUpdateTime = 0; // 强制标记为空闲
            updateGlobalWatermark();
        }
    }

    /**
     * 获取统计信息
     */
    public WatermarkStats getStats() {
        int totalPartitions = partitionWatermarks.size();
        int idlePartitions = 0;
        long now = System.currentTimeMillis();

        for (PartitionWatermark pw : partitionWatermarks.values()) {
            if (now - pw.lastUpdateTime > IDLE_TIMEOUT_MS) {
                idlePartitions++;
            }
        }

        return new WatermarkStats(
                globalWatermark.get(),
                totalPartitions,
                idlePartitions,
                totalPartitions - idlePartitions
        );
    }

    // ============== 内部类 ==============

    private static class PartitionWatermark {
        final AtomicLong timestamp = new AtomicLong(Long.MIN_VALUE);
        volatile long lastUpdateTime = System.currentTimeMillis();
    }

    public record WatermarkStats(
            long globalWatermark,
            int totalPartitions,
            int idlePartitions,
            int activePartitions
    ) {
        public boolean hasIdlePartitions() {
            return idlePartitions > 0;
        }
    }
}
