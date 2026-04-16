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
 * 基于 Little's Law (L = λW) + PID 控制器动态调整窗口大小
 * 核心算法：
 * 1. 数据到达率分析（流量预测）
 * 2. 事件时间分布分析（乱序程度）
 * 3. PID 控制器平滑窗口参数调整
 */
@Slf4j
public class AdaptiveWindowManager {

    private static final long MIN_WINDOW_SIZE_MS = 1000;
    private static final long MAX_WINDOW_SIZE_MS = 300000; // 5分钟
    private static final double LEARNING_RATE = 0.1;

    // Little's Law + PID 参数
    private static final long TARGET_LATENCY_MS = 2000;

    @Getter
    private volatile Duration currentWindowSize;           // 当前窗口大小
    @Getter
    private volatile Duration maxOutOfOrderness;           // 允许的最大乱序时间
    private final RateStatistics rateStatistics;           // 历史数据统计
    private final Queue<Long> latencySamples;              // 最近的事件延迟样本
    private static final int MAX_SAMPLES = 1000;           // 样本数量限制
    private final AtomicLong lastAdjustmentTime;           // 调整频率控制
    private static final long MIN_ADJUSTMENT_INTERVAL_MS = 10000; // 10秒
    private final PIDController pidController;             // PID控制器用于平滑窗口调整

    public AdaptiveWindowManager(Duration initialWindowSize) {
        this.currentWindowSize = initialWindowSize;
        this.maxOutOfOrderness = Duration.ofSeconds(5);
        this.rateStatistics = new RateStatistics();
        this.latencySamples = new ConcurrentLinkedQueue<>();
        this.lastAdjustmentTime = new AtomicLong(0);
        this.pidController = new PIDController(0.3, 0.05, 0.02);
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
        rateStatistics.update();

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
        double arrivalRate = rateStatistics.getCurrentRate();

        // 使用Little's Law + PID预测最优窗口大小
        long optimalWindowSize = predictOptimalWindowSize(
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
     * 基于Little's Law + PID控制器的窗口大小预测（V2）
     * 理论背书：排队论 Little's Law (L = λW) + 控制论 PID 平滑
     */
    public long predictOptimalWindowSize(double medianLatency, double p95Latency,
                                              double p99Latency, double arrivalRate,
                                              long currentWindowSize) {
        // Little's Law: 在目标延迟下，系统需要缓冲的时间跨度
        double optimalSize = TARGET_LATENCY_MS * arrivalRate / 1000.0;

        // PID 修正：根据当前延迟与目标延迟的偏差进行平滑调整
        double latencyError = (TARGET_LATENCY_MS - medianLatency) / (double) TARGET_LATENCY_MS;
        double pidAdjustment = pidController.calculate(latencyError);

        long predicted = (long) (optimalSize * (1 + pidAdjustment));

        // 乱序补偿：p99/median 衡量尾部膨胀
        double disorderRatio = p99Latency / Math.max(medianLatency, 1);
        if (disorderRatio > 3.0) {
            predicted = (long) (predicted * (1 + Math.min((disorderRatio - 3) * 0.1, 0.3)));
        }

        return predicted;
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
}
