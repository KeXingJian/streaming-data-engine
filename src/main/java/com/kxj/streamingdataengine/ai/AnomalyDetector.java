package com.kxj.streamingdataengine.ai;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 异常流量检测器
 * 基于统计方法和机器学习检测流量异常
 * 检测方法：
 * 1. 基于3-sigma原则的统计异常检测
 * 2. 基于滑动窗口的变化率检测
 * 3. 周期性模式识别
 * 4. 多级阈值告警
 */
@Slf4j
public class AnomalyDetector {

    private static final int WINDOW_SIZE = 1000; // 滑动窗口大小，支持更多样本
    private static final int HISTORY_SIZE = 1440; // 保存24小时历史数据

    private final Queue<TrafficRecord> trafficHistory; // 流量历史记录

    private final SlidingWindowStatistics currentStats; // 当前窗口统计

    private final DetectionConfig config; // 检测器配置

    private final AlertHandler alertHandler; // 告警处理器

    public AnomalyDetector(AlertHandler alertHandler) {
        this(DetectionConfig.defaultConfig(), alertHandler);
    }

    public AnomalyDetector(DetectionConfig config, AlertHandler alertHandler) {
        this.config = config;
        this.alertHandler = alertHandler;
        this.trafficHistory = new ConcurrentLinkedQueue<>();
        this.currentStats = new SlidingWindowStatistics(WINDOW_SIZE);
    }

    /**
     * 记录流量数据点
     */
    public void recordSample(double value) {
        // [kxj: 异常检测器采样 - 维护滑动窗口统计，支持3-sigma/变化率/周期性三种检测]
        long now = System.currentTimeMillis();
        TrafficRecord record = new TrafficRecord(now, value);

        trafficHistory.offer(record);
        currentStats.add(value);

        // 清理过期数据
        cleanupOldRecords(now);

        // [kxj: 执行多维度异常检测，综合评分确定异常级别]
        AnomalyResult result = detect(value);

        if (result.isAnomaly()) {
            log.debug("[kxj: 异常检测到] level={}, value={}, zScore={}", result.getLevel(), value, result.getZScore());
            alertHandler.onAlert(result);
        }
    }

    /**
     * 执行异常检测
     */
    private AnomalyResult detect(double currentValue) {
        // [kxj: 1. 3-sigma统计检测 - 偏离均值超过3倍标准差视为异常]
        double mean = currentStats.getMean();
        double stdDev = currentStats.getStdDev();

        boolean is3SigmaAnomaly = false;
        double zScore = 0;

        if (stdDev > 0) {
            zScore = Math.abs(currentValue - mean) / stdDev;
            is3SigmaAnomaly = zScore > config.sigmaThreshold();
        }

        // [kxj: 2. 变化率检测 - 突变往往预示系统问题，权重最高(2分)]
        double changeRate = calculateChangeRate(currentValue);
        boolean isChangeRateAnomaly = Math.abs(changeRate) > config.maxChangeRate();

        // [kxj: 3. 历史同比检测 - 识别周期性模式中的异常]
        double seasonalDeviation = calculateSeasonalDeviation(currentValue);
        boolean isSeasonalAnomaly = Math.abs(seasonalDeviation) > config.seasonalThreshold();

        // [kxj: 综合评分 - 1分MEDIUM, 2分HIGH, 3+分CRITICAL]
        int anomalyScore = 0;
        if (is3SigmaAnomaly) anomalyScore += 1;
        if (isChangeRateAnomaly) anomalyScore += 2;
        if (isSeasonalAnomaly) anomalyScore += 1;

        AnomalyLevel level = AnomalyLevel.NORMAL;
        if (anomalyScore >= 3) {
            level = AnomalyLevel.CRITICAL;
        } else if (anomalyScore == 2) {
            level = AnomalyLevel.HIGH;
        } else if (anomalyScore == 1) {
            level = AnomalyLevel.MEDIUM;
        }

        return new AnomalyResult(
                level,
                currentValue,
                mean,
                zScore,
                changeRate,
                seasonalDeviation,
                System.currentTimeMillis()
        );
    }

    /**
     * 计算变化率
     */
    private double calculateChangeRate(double currentValue) {
        List<Double> recent = currentStats.getRecentValues(10);
        if (recent.isEmpty()) {
            return 0;
        }

        double previous = recent.getFirst();
        if (previous == 0) {
            return currentValue > 0 ? Double.MAX_VALUE : 0;
        }

        return (currentValue - previous) / previous;
    }

    /**
     * 计算历史同比偏差
     */
    private double calculateSeasonalDeviation(double currentValue) {
        long oneHourAgo = System.currentTimeMillis() - 3600000;
        List<Double> historicalValues = new ArrayList<>();

        for (TrafficRecord record : trafficHistory) {
            // 找出1小时前相近时间的数据
            if (Math.abs(record.timestamp - oneHourAgo) < 60000) { // ±1分钟
                historicalValues.add(record.value);
            }
        }

        if (historicalValues.isEmpty()) {
            return 0;
        }

        double historicalMean = historicalValues.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(currentValue);

        if (historicalMean == 0) {
            return currentValue > 0 ? 1 : 0;
        }

        return (currentValue - historicalMean) / historicalMean;
    }

    /**
     * 清理过期记录
     */
    private void cleanupOldRecords(long now) {
        long cutoff = now - HISTORY_SIZE * 60000L; // 保留HISTORY_SIZE分钟

        while (!trafficHistory.isEmpty()) {
            TrafficRecord oldest = trafficHistory.peek();
            if (oldest != null && oldest.timestamp < cutoff) {
                trafficHistory.poll();
            } else {
                break;
            }
        }
    }

    /**
     * 获取当前流量统计
     */
    public TrafficStatistics getStatistics() {
        return new TrafficStatistics(
                currentStats.getMean(),
                currentStats.getStdDev(),
                currentStats.getMin(),
                currentStats.getMax(),
                currentStats.getCount()
        );
    }

    // ============== 内部类和接口 ==============

    @FunctionalInterface
    public interface AlertHandler {
        void onAlert(AnomalyResult result);
    }

    public record TrafficRecord(long timestamp, double value) {
    }

    @lombok.AllArgsConstructor
    @lombok.Getter
    public enum AnomalyLevel {
        NORMAL(0),
        MEDIUM(1),
        HIGH(2),
        CRITICAL(3);

        private final int severity;
    }

    @lombok.AllArgsConstructor
    @lombok.Getter
    @lombok.ToString
    public static class AnomalyResult {
        private final AnomalyLevel level;
        private final double currentValue;
        private final double baseline;
        private final double zScore;
        private final double changeRate;
        private final double seasonalDeviation;
        private final long timestamp;

        public boolean isAnomaly() {
            return level != AnomalyLevel.NORMAL;
        }
    }

    /**
     * @param sigmaThreshold    3-sigma阈值
     * @param maxChangeRate     最大变化率
     * @param seasonalThreshold 同比偏差阈值
     */

    public record DetectionConfig(double sigmaThreshold, double maxChangeRate, double seasonalThreshold) {
            public static DetectionConfig defaultConfig() {
                return new DetectionConfig(3.0, 0.5, 0.3);
            }

            public static DetectionConfig sensitiveConfig() {
                return new DetectionConfig(2.0, 0.3, 0.2);
            }
        }


    public record TrafficStatistics(double mean, double stdDev, double min, double max, long count) {
    }

    /**
     * 滑动窗口统计
     */
    private static class SlidingWindowStatistics {
        private final Queue<Double> values;
        private final int maxSize;
        private double sum;
        private double sumSquares;

        SlidingWindowStatistics(int maxSize) {
            this.maxSize = maxSize;
            this.values = new ArrayDeque<>(maxSize);
            this.sum = 0;
            this.sumSquares = 0;
        }

        void add(double value) {
            values.offer(value);
            sum += value;
            sumSquares += value * value;

            if (values.size() > maxSize) {
                double removed = values.poll();
                sum -= removed;
                sumSquares -= removed * removed;
            }
        }

        double getMean() {
            return values.isEmpty() ? 0 : sum / values.size();
        }

        double getStdDev() {
            if (values.isEmpty()) return 0;
            double mean = getMean();
            double variance = sumSquares / values.size() - mean * mean;
            return Math.sqrt(Math.max(0, variance));
        }

        double getMin() {
            return values.stream().mapToDouble(Double::doubleValue).min().orElse(0);
        }

        double getMax() {
            return values.stream().mapToDouble(Double::doubleValue).max().orElse(0);
        }

        long getCount() {
            return values.size();
        }

        List<Double> getRecentValues(int n) {
            List<Double> result = new ArrayList<>();
            Iterator<Double> it = values.iterator();
            while (it.hasNext() && result.size() < n) {
                result.add(it.next());
            }
            return result;
        }
    }
}
