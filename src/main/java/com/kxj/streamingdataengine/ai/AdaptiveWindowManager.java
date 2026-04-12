package com.kxj.streamingdataengine.ai;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.window.Window;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 自适应窗口管理器
 * 基于数据特征学习，动态调整窗口大小
 *
 * 核心算法：
 * 1. 数据到达率分析（流量预测）
 * 2. 事件时间分布分析（乱序程度）
 * 3. 使用EWMA（指数加权移动平均）预测
 * 4. 强化学习调整窗口参数
 */
@Slf4j
public class AdaptiveWindowManager {

    private static final long MIN_WINDOW_SIZE_MS = 1000;
    private static final long MAX_WINDOW_SIZE_MS = 300000; // 5分钟
    private static final double LEARNING_RATE = 0.1;

    @Getter
    private volatile Duration currentWindowSize;           // 当前窗口大小
    @Getter
    private volatile Duration maxOutOfOrderness;           // 允许的最大乱序时间
    private final StatisticsCollector statisticsCollector; // 历史数据统计
    private final AdaptiveModel model;                     // 机器学习模型（简化版使用EWMA和启发式规则）
    private final Queue<Long> latencySamples;              // 最近的事件延迟样本
    private static final int MAX_SAMPLES = 1000;           // 样本数量限制
    private final AtomicLong lastAdjustmentTime;           // 调整频率控制
    private static final long MIN_ADJUSTMENT_INTERVAL_MS = 10000; // 10秒

    public AdaptiveWindowManager(Duration initialWindowSize) {
        this.currentWindowSize = initialWindowSize;
        this.maxOutOfOrderness = Duration.ofSeconds(5);
        this.statisticsCollector = new StatisticsCollector();
        this.model = new AdaptiveModel();
        this.latencySamples = new ConcurrentLinkedQueue<>();
        this.lastAdjustmentTime = new AtomicLong(0);
    }

    /**
     * 收集事件样本
     */
    public void collectSample(StreamRecord<?> record) {
        // [kxj: 自适应窗口采样 - 收集延迟样本用于EWMA预测和窗口参数调整]
        long latency = record.getLatency();
        latencySamples.offer(latency);

        // [kxj: 滑动窗口维护，保留最近MAX_SAMPLES个样本]
        while (latencySamples.size() > MAX_SAMPLES) {
            latencySamples.poll();
        }

        // 更新统计
        statisticsCollector.update();

        // [kxj: 每10秒触发一次窗口参数自适应调整]
        long now = System.currentTimeMillis();
        if (now - lastAdjustmentTime.get() > MIN_ADJUSTMENT_INTERVAL_MS) {
            adjustWindowParameters();
            lastAdjustmentTime.set(now);
        }
    }

    /**
     * 调整窗口参数
     */
    private void adjustWindowParameters() {
        if (latencySamples.isEmpty()) {
            return;
        }

        // 计算统计数据
        List<Long> sortedSamples = new ArrayList<>(latencySamples);
        Collections.sort(sortedSamples);

        double medianLatency = sortedSamples.get(sortedSamples.size() / 2);
        double p95Latency = sortedSamples.get((int) (sortedSamples.size() * 0.95));
        double p99Latency = sortedSamples.get((int) (sortedSamples.size() * 0.99));

        // 计算到达率
        double arrivalRate = statisticsCollector.getArrivalRate();

        // 使用模型预测最优窗口大小
        long optimalWindowSize = model.predictOptimalWindowSize(
                medianLatency, p95Latency, p99Latency, arrivalRate,
                currentWindowSize.toMillis()
        );

        // 平滑调整（避免剧烈变化）
        long currentMs = currentWindowSize.toMillis();
        long adjustedMs = (long) (currentMs + LEARNING_RATE * (optimalWindowSize - currentMs));

        // 限制在合理范围内
        adjustedMs = Math.clamp(adjustedMs, MIN_WINDOW_SIZE_MS, MAX_WINDOW_SIZE_MS);

        // 更新乱序容忍度
        long newOutOfOrderness = (long) (p95Latency * 1.2); // P95延迟的1.2倍
        newOutOfOrderness = Math.clamp(newOutOfOrderness, 100, 60000);

        if (Math.abs(adjustedMs - currentMs) > currentMs * 0.1) { // 变化超过10%才更新
            currentWindowSize = Duration.ofMillis(adjustedMs);
            maxOutOfOrderness = Duration.ofMillis(newOutOfOrderness);

            log.info("Window parameters adjusted: size={}, outOfOrderness={}, " +
                    "latency(median={}, p95={}, p99={}), rate={}",
                    currentWindowSize, maxOutOfOrderness,
                    medianLatency, p95Latency, p99Latency, arrivalRate);
        }
    }

    /**
     * 为事件分配窗口
     */
    public List<Window> assignWindows(long timestamp) {
        long windowSize = currentWindowSize.toMillis();
        long start = timestamp - (timestamp % windowSize);
        return List.of(new Window.TimeWindow(start, start + windowSize));
    }

    /**
     * 获取当前推荐的Watermark延迟（毫秒）
     */
    public long getRecommendedWatermarkDelayMs() {
        return maxOutOfOrderness.toMillis();
    }

    /**
     * 自适应模型（简化实现）
     */
    private static class AdaptiveModel {

        /**
         * 预测最优窗口大小
         *
         * 策略：
         * 1. 低延迟 + 高吞吐：大窗口
         * 2. 高延迟 + 低吞吐：小窗口
         * 3. 乱序严重：增加乱序容忍度
         */
        public long predictOptimalWindowSize(double medianLatency, double p95Latency,
                                              double p99Latency, double arrivalRate,
                                              long currentWindowSize) {
            // 基于延迟调整
            double latencyFactor;
            if (p99Latency > 10000) { // 延迟超过10秒
                latencyFactor = 0.5; // 减小窗口
            } else if (p95Latency < 100) { // 延迟很低
                latencyFactor = 2.0; // 增大窗口
            } else {
                latencyFactor = 1.0;
            }

            // 基于到达率调整
            double rateFactor;
            if (arrivalRate > 10000) { // 高吞吐
                rateFactor = 1.5; // 大窗口，减少触发次数
            } else if (arrivalRate < 100) { // 低吞吐
                rateFactor = 0.8; // 小窗口，更快输出
            } else {
                rateFactor = 1.0;
            }

            // 综合计算
            long predicted = (long) (currentWindowSize * latencyFactor * rateFactor);

            // 根据乱序程度微调
            double disorderRatio = p99Latency / Math.max(medianLatency, 1);
            if (disorderRatio > 5) {
                // 乱序严重，稍微增大窗口以等待更多数据
                predicted = (long) (predicted * 1.2);
            }

            return predicted;
        }
    }

    /**
     * 统计收集器
     */
    private static class StatisticsCollector {
        private final AtomicLong eventCount = new AtomicLong(0);
        private volatile long lastCountTime = System.currentTimeMillis();
        private volatile double currentRate = 0;

        void update() {
            eventCount.incrementAndGet();

            // 每秒更新一次速率
            long now = System.currentTimeMillis();
            if (now - lastCountTime >= 1000) {
                long count = eventCount.getAndSet(0);
                currentRate = count * 1000.0 / (now - lastCountTime);
                lastCountTime = now;
            }
        }

        double getArrivalRate() {
            return currentRate;
        }
    }
}
