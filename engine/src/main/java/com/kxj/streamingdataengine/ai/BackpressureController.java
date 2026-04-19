package com.kxj.streamingdataengine.ai;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 动态背压控制器
 * 基于 Little's Law (L = λW) + PID 控制器预测式限流
 * 核心策略：
 * 1. EWMA 实时估计数据到达率
 * 2. 用延迟和到达率推导理想处理速率
 * 3. PID 平滑修正，避免剧烈震荡
 * 4. 与 ExecutionEngine 的 BlockingQueue 自然背压协同工作
 */
@Slf4j
public class BackpressureController {

    private static final double LEARNING_RATE = 0.1;
    private static final long MIN_RATE_LIMIT = 1000;
    private static final long MAX_RATE_LIMIT = 100_000;

    @Getter
    private final AtomicReference<SeverityLevel> currentLevel;
    private final AtomicInteger currentRateLimit;
    private final ConcurrentLinkedQueue<Sample> samples;
    private final LatencyStatistics latencyStatistics;
    private final PIDController pidController;
    private final long targetLatencyMs;
    private double lastLatencyError = 0;

    public BackpressureController() {
        this(500);
    }

    public BackpressureController(long targetLatencyMs) {
        this.targetLatencyMs = targetLatencyMs;
        this.currentLevel = new AtomicReference<>(SeverityLevel.NORMAL);
        this.currentRateLimit = new AtomicInteger(Integer.MAX_VALUE);
        this.samples = new ConcurrentLinkedQueue<>();
        this.latencyStatistics = new LatencyStatistics();
        // 和 AdaptiveWindowManager 对齐的 PID 参数
        this.pidController = new PIDController(0.3, 0.05, 0.02, 30.0);
    }

    /**
     * 记录处理样本
     */
    public void recordSample(StreamRecord<?> record, long processingLatencyMs) {
        // [kxj: 背压控制器采样 - 收集延迟用于 Little's Law 预测式限流]
        Sample sample = new Sample(System.currentTimeMillis(), processingLatencyMs);
        samples.offer(sample);

        cleanupOldSamples();
        latencyStatistics.update(processingLatencyMs);
        evaluateAndAdjust();
    }

    /**
     * 评估系统状态并调整背压
     * 基于 Little's Law + PID 控制器
     */
    private void evaluateAndAdjust() {
        long avgLatency = latencyStatistics.getAvgLatency();
        double arrivalRate = latencyStatistics.ewmaRate;

        if (avgLatency <= 0) {
            return;
        }

        long predicted;
        double latencyError;

        // 到达率过低时（< 1条/秒视为无效），退化为纯延迟比例调整
        if (arrivalRate < 1.0) {
            // [kxj: 到达率样本不足，退化为纯延迟比例调整]
            double ratio = targetLatencyMs / Math.max(avgLatency, 1.0);
            predicted = (long) (MAX_RATE_LIMIT * ratio);
            latencyError = (targetLatencyMs - avgLatency) / (double) targetLatencyMs;

            // [kxj: PID 积分饱和保护]
            if (lastLatencyError <= 0 && latencyError > 0) {
                pidController.reset();
            }
            lastLatencyError = latencyError;
        } else {
            // [kxj: Little's Law 预测理想处理速率 - optimalRate = λ * (targetLatency / avgLatency)]
            double optimalRate = arrivalRate * targetLatencyMs / Math.max(avgLatency, 1.0);

            // PID 修正：根据延迟误差（和 AdaptiveWindowManager 对齐）
            latencyError = (targetLatencyMs - avgLatency) / (double) targetLatencyMs;

            // [kxj: PID 积分饱和保护 - 误差符号由负变正时重置积分器，防止历史过载影响恢复]
            if (lastLatencyError <= 0 && latencyError > 0) {
                pidController.reset();
            }
            lastLatencyError = latencyError;

            double pidAdjustment = pidController.calculate(latencyError);

            predicted = (long) (optimalRate * (1 + pidAdjustment));
        }

        // 平滑调整
        long currentLimit = currentRateLimit.get() == Integer.MAX_VALUE ? MAX_RATE_LIMIT : currentRateLimit.get();
        long adjusted = (long) (currentLimit + LEARNING_RATE * (predicted - currentLimit));
        adjusted = Math.clamp(adjusted, MIN_RATE_LIMIT, MAX_RATE_LIMIT);

        // 更新压力等级（兼容外部监控）
        SeverityLevel newLevel = mapErrorToLevel(latencyError);
        currentLevel.set(newLevel);

        if (adjusted != currentLimit || currentRateLimit.get() == Integer.MAX_VALUE) {
            currentRateLimit.set((int) adjusted);
            log.info("[kxj: 背压限流调整] rateLimit={}, avgLatency={}, arrivalRate={}, error={}, level={}",
                    adjusted, avgLatency,
                    String.format("%.2f", arrivalRate),
                    String.format("%.4f", latencyError),
                    newLevel);
        }
    }

    private SeverityLevel mapErrorToLevel(double latencyError) {
        if (latencyError > -0.1) return SeverityLevel.NORMAL;
        if (latencyError > -0.3) return SeverityLevel.MEDIUM;
        if (latencyError > -0.6) return SeverityLevel.HIGH;
        return SeverityLevel.CRITICAL;
    }

    /**
     * 获取当前限流速率（监控用）
     */
    public int getCurrentRateLimit() {
        return currentRateLimit.get();
    }

    /**
     * 清理旧样本
     */
    private void cleanupOldSamples() {
        long cutoff = System.currentTimeMillis() - 60000; // 保留1分钟

        while (!samples.isEmpty()) {
            Sample oldest = samples.peek();
            if (oldest != null && oldest.timestamp < cutoff) {
                samples.poll();
            } else {
                break;
            }
        }
    }

    /**
     * 获取当前系统状态报告
     */
    public SystemStatus getStatus() {
        long avgLatency = latencyStatistics.getAvgLatency();
        double arrivalRate = latencyStatistics.ewmaRate;
        // Little's Law 计算等效队列长度: L = λW
        long equivalentQueueSize = avgLatency > 0 ? (long) (arrivalRate * avgLatency / 1000.0) : 0;
        return new SystemStatus(
                currentLevel.get(),
                currentRateLimit.get(),
                avgLatency,
                equivalentQueueSize,
                arrivalRate
        );
    }

    // ============== 内部类和记录 ==============

    public record SystemStatus(SeverityLevel pressureLevel, int rateLimit, long avgLatencyMs, long queueSize,
                               double currentRate) {
    }

    private record Sample(long timestamp, long latency) {
    }

    /**
     * 延迟统计
     * 使用 EWMA 实时估计到达率，避免固定窗口的盲区
     */
    private static class LatencyStatistics {
        private final AtomicLong totalLatency = new AtomicLong(0);
        private final AtomicLong sampleCount = new AtomicLong(0);
        private final AtomicLong lastUpdateTime = new AtomicLong(0);
        volatile double ewmaRate = 0;
        private static final double ALPHA = 0.3;

        void update(long latency) {
            totalLatency.addAndGet(latency);
            sampleCount.incrementAndGet();

            // EWMA 到达率：每条样本都贡献一个瞬时到达率估计
            long now = System.currentTimeMillis();
            long lastTime = lastUpdateTime.getAndSet(now);
            if (lastTime > 0) {
                double elapsed = now - lastTime;
                double instantRate = 1000.0 / Math.max(elapsed, 1);
                ewmaRate = ALPHA * instantRate + (1 - ALPHA) * ewmaRate;
            }
        }

        long getAvgLatency() {
            long count = sampleCount.get();
            return count > 0 ? totalLatency.get() / count : 0;
        }

        void reset() {
            totalLatency.set(0);
            sampleCount.set(0);
            lastUpdateTime.set(0);
            ewmaRate = 0;
        }
    }
}
