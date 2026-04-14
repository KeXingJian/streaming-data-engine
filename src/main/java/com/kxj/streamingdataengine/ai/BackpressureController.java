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
 * 基于系统负载自动调整处理速率
 *
 * 控制策略：
 * 1. 队列长度监控
 * 2. 处理延迟监控
 * 3. 自适应限流（令牌桶算法）
 * 4. 多级背压响应
 */
@Slf4j
public class BackpressureController {

    /**
     * 队列长度阈值
     */
    private static final int QUEUE_LOW_THRESHOLD = 1000;
    private static final int QUEUE_MEDIUM_THRESHOLD = 5000;
    private static final int QUEUE_HIGH_THRESHOLD = 10000;

    /**
     * 延迟阈值（毫秒）
     */
    private static final long LATENCY_LOW_THRESHOLD = 100;
    private static final long LATENCY_MEDIUM_THRESHOLD = 500;
    private static final long LATENCY_HIGH_THRESHOLD = 1000;

    /**
     * 当前压力等级
     */
    @Getter
    private final AtomicReference<PressureLevel> currentLevel;

    /**
     * 当前限流速率（记录/秒）
     */
    private final AtomicInteger currentRateLimit;

    /**
     * 采样队列
     */
    private final ConcurrentLinkedQueue<Sample> samples;

    /**
     * 队列大小监控
     */
    private final AtomicInteger monitoredQueueSize;

    /**
     * 统计信息
     */
    private final Statistics statistics;

    /**
     * 自适应PID控制器参数
     */
    private final PIDController pidController;

    /**
     * 目标延迟
     */
    private final long targetLatencyMs;

    public BackpressureController() {
        this(500); // 默认目标延迟500ms
    }

    public BackpressureController(long targetLatencyMs) {
        this.targetLatencyMs = targetLatencyMs;
        this.currentLevel = new AtomicReference<>(PressureLevel.NORMAL);
        this.currentRateLimit = new AtomicInteger(Integer.MAX_VALUE);
        this.samples = new ConcurrentLinkedQueue<>();
        this.monitoredQueueSize = new AtomicInteger(0);
        this.statistics = new Statistics();
        this.pidController = new PIDController(0.5, 0.1, 0.05);
    }

    /**
     * 记录处理样本
     */
    public void recordSample(StreamRecord<?> record, long processingLatencyMs) {
        // [kxj: 背压控制器采样 - 收集延迟和队列大小用于动态调整限流]
        Sample sample = new Sample(
                System.currentTimeMillis(),
                processingLatencyMs,
                monitoredQueueSize.get()
        );
        samples.offer(sample);

        // 清理旧样本
        cleanupOldSamples();

        // 更新统计
        statistics.update(processingLatencyMs, monitoredQueueSize.get());

        // [kxj: 评估系统压力并动态调整限流，确保系统稳定运行]
        evaluateAndAdjust();
    }

    /**
     * 设置当前队列大小
     */
    public void setQueueSize(int size) {
        monitoredQueueSize.set(size);
    }

    /**
     * 获取当前限流速率
     */
    public int getCurrentRateLimit() {
        return currentRateLimit.get();
    }

    /**
     * 检查是否允许处理（限流检查）
     */
    public boolean tryAcquire() {
        int limit = currentRateLimit.get();
        if (limit == Integer.MAX_VALUE) {
            return true;
        }

        // 简化的令牌桶检查
        long now = System.currentTimeMillis();
        return now % 1000 < (1000 * statistics.currentRate / Math.max(limit, 1));
    }

    /**
     * 评估系统状态并调整背压
     */
    private void evaluateAndAdjust() {
        long avgLatency = statistics.getAvgLatency();
        int queueSize = monitoredQueueSize.get();

        // 确定当前压力等级
        PressureLevel newLevel = determinePressureLevel(avgLatency, queueSize);
        PressureLevel oldLevel = currentLevel.getAndSet(newLevel);

        if (newLevel != oldLevel) {
            log.info("Pressure level changed: {} -> {}, latency={}, queue={}",
                    oldLevel, newLevel, avgLatency, queueSize);
            applyPressureControl(newLevel, oldLevel);
        } else {
            // 同等级内微调
            fineTuneControl(avgLatency);
        }
    }

    /**
     * 确定压力等级
     */
    private PressureLevel determinePressureLevel(long avgLatency, int queueSize) {
        int pressureScore = 0;

        // 队列长度评分
        if (queueSize > QUEUE_HIGH_THRESHOLD) {
            pressureScore += 3;
        } else if (queueSize > QUEUE_MEDIUM_THRESHOLD) {
            pressureScore += 2;
        } else if (queueSize > QUEUE_LOW_THRESHOLD) {
            pressureScore += 1;
        }

        // 延迟评分
        if (avgLatency > LATENCY_HIGH_THRESHOLD) {
            pressureScore += 3;
        } else if (avgLatency > LATENCY_MEDIUM_THRESHOLD) {
            pressureScore += 2;
        } else if (avgLatency > LATENCY_LOW_THRESHOLD) {
            pressureScore += 1;
        }

        // [kxj: 压力评分转换 - 0-1分NORMAL, 1-2分MEDIUM, 3-4分HIGH, 5+分CRITICAL]
        if (pressureScore >= 5) {
            return PressureLevel.CRITICAL;
        } else if (pressureScore >= 3) {
            return PressureLevel.HIGH;
        } else if (pressureScore >= 1) {
            return PressureLevel.MEDIUM;
        }
        return PressureLevel.NORMAL;
    }

    /**
     * 应用压力控制
     */
    private void applyPressureControl(PressureLevel level, PressureLevel oldLevel) {
        // [kxj: 根据压力等级设置对应限流阈值，NORMAL时重置统计恢复最佳状态]
        int newLimit;

        switch (level) {
            case NORMAL:
                newLimit = Integer.MAX_VALUE; // 不限流
                // 重置统计数据，避免旧数据影响
                statistics.reset();
                break;
            case MEDIUM:
                newLimit = 50000; // 5万/秒
                break;
            case HIGH:
                newLimit = 10000; // 1万/秒
                break;
            case CRITICAL:
                newLimit = 1000; // 1000/秒
                break;
            default:
                newLimit = Integer.MAX_VALUE;
        }

        // 更新限流：只要等级变化就更新，确保降级时能放宽
        currentRateLimit.set(newLimit);
        // 重置PID控制器，避免积分累积
        pidController.reset();
    }

    /**
     * 微调控制（PID控制）
     * 只做保守调整，避免过度限流
     */
    private void fineTuneControl(long currentLatency) {
        if (currentLevel.get() == PressureLevel.NORMAL) {
            return;
        }

        // 计算误差
        double error = (double) (targetLatencyMs - currentLatency) / targetLatencyMs;

        // 只在延迟低于目标时才放宽限流（error > 0）
        // 避免在延迟高时进一步收紧限流
        if (error < 0) {
            return; // 延迟过高时不做微调，保持当前限流
        }

        // PID计算调整量
        double adjustment = pidController.calculate(error);

        // 只放宽限流（adjustment > 0），不收紧
        if (adjustment <= 0) {
            return;
        }

        // 应用调整（只放宽）
        int currentLimit = currentRateLimit.get();
        if (currentLimit != Integer.MAX_VALUE) {
            int newLimit = (int) (currentLimit * (1 + adjustment));
            newLimit = Math.min(newLimit, 100000); // 上限
            currentRateLimit.set(newLimit);
        }
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
        return new SystemStatus(
                currentLevel.get(),
                currentRateLimit.get(),
                statistics.getAvgLatency(),
                monitoredQueueSize.get(),
                statistics.currentRate
        );
    }

    // ============== 内部类和枚举 ==============

    public enum PressureLevel {
        NORMAL,
        MEDIUM,
        HIGH,
        CRITICAL
    }


    public record SystemStatus(PressureLevel pressureLevel, int rateLimit, long avgLatencyMs, int queueSize,
                               double currentRate) {
    }

    private record Sample(long timestamp, long latency, int queueSize) {}

    /**
     * 简单统计
     */
    private static class Statistics {
        private final AtomicLong totalLatency = new AtomicLong(0);
        private final AtomicLong sampleCount = new AtomicLong(0);
        private final AtomicLong lastRateCheck = new AtomicLong(0);
        private final AtomicLong lastRateCount = new AtomicLong(0);
        volatile double currentRate = 0;

        void update(long latency, int queueSize) {
            totalLatency.addAndGet(latency);
            long count = sampleCount.incrementAndGet();

            // 计算速率
            long now = System.currentTimeMillis();
            long lastCheck = lastRateCheck.get();
            if (now - lastCheck >= 1000) {
                long lastCount = lastRateCount.getAndSet(count);
                if (lastCheck > 0) {
                    currentRate = (count - lastCount) * 1000.0 / (now - lastCheck);
                }
                lastRateCheck.set(now);
            }
        }

        long getAvgLatency() {
            long count = sampleCount.get();
            return count > 0 ? totalLatency.get() / count : 0;
        }

        void reset() {
            totalLatency.set(0);
            sampleCount.set(0);
            lastRateCheck.set(0);
            lastRateCount.set(0);
            currentRate = 0;
        }
    }

    /**
     * PID控制器
     */
    private static class PIDController {
        private final double kp, ki, kd;
        private double integral;
        private double lastError;

        PIDController(double kp, double ki, double kd) {
            this.kp = kp;
            this.ki = ki;
            this.kd = kd;
        }

        double calculate(double error) {
            integral += error;
            double derivative = error - lastError;
            lastError = error;

            return kp * error + ki * integral + kd * derivative;
        }

        void reset() {
            integral = 0;
            lastError = 0;
        }
    }
}
