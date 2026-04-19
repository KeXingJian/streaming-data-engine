package com.kxj.streamingdataengine.core.model;

import lombok.Builder;
import lombok.Getter;

import java.util.Map;

/**
 * 流处理结果
 */
@Getter
@Builder
public class StreamResult {

    private final long processedCount; // 处理记录数

    private final long outputCount; // 输出记录数

    private final long lateRecordCount; // 迟到记录数

    private final long droppedCount; // 丢弃记录数

    private final long startTime; // 处理开始时间

    private final long endTime; // 处理结束时间

    private final Map<String, OperatorStats> operatorStats; // 各算子处理统计

    private final PerformanceMetrics metrics; // 性能指标

    @Getter
    @Builder
    public static class OperatorStats {
        private final String operatorName;
        private final long inputCount;
        private final long outputCount;
        private final long processingTimeMs;
        private final double throughput;
    }

    @Getter
    @Builder
    public static class PerformanceMetrics {
        private final double recordsPerSecond;
        private final double avgLatencyMs;
        private final double p99LatencyMs;
        private final double memoryUsageMB;
        private final double cpuUsagePercent;
    }

    /**
     * 获取处理耗时（毫秒）
     */
    public long getDurationMs() {
        return endTime - startTime;
    }

    /**
     * 获取吞吐量（记录/秒）
     */
    public double getThroughput() {
        long duration = getDurationMs();
        return duration > 0 ? (double) processedCount / duration * 1000 : 0;
    }
}
