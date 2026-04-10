package com.kxj.streamingdataengine.execution;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.aggregation.MergeTreeAggregator;
import com.kxj.streamingdataengine.ai.AdaptiveWindowManager;
import com.kxj.streamingdataengine.ai.AnomalyDetector;
import com.kxj.streamingdataengine.ai.BackpressureController;
import com.kxj.streamingdataengine.core.model.*;
import com.kxj.streamingdataengine.core.operator.StreamOperator;
import com.kxj.streamingdataengine.window.Window;
import com.kxj.streamingdataengine.window.WindowState;
import com.kxj.streamingdataengine.window.trigger.Trigger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * 流处理执行引擎
 * 核心组件协调和调度
 */
@Slf4j
@RequiredArgsConstructor
public class ExecutionEngine {

    private final int parallelism;
    private final Duration watermarkInterval;
    private final boolean enableAdaptiveWindow;
    private final boolean enableBackpressure;

    private final ExecutorService executorService;
    private final WatermarkManager watermarkManager;
    private final AdaptiveWindowManager adaptiveWindowManager;
    private final AnomalyDetector anomalyDetector;
    private final BackpressureController backpressureController;

    private volatile boolean running = false;

    public ExecutionEngine() {
        this(Runtime.getRuntime().availableProcessors(),
             Duration.ofMillis(200),
             true, true);
    }

    public ExecutionEngine(int parallelism, Duration watermarkInterval,
                          boolean enableAdaptiveWindow, boolean enableBackpressure) {
        this.parallelism = parallelism;
        this.watermarkInterval = watermarkInterval;
        this.enableAdaptiveWindow = enableAdaptiveWindow;
        this.enableBackpressure = enableBackpressure;

        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        this.watermarkManager = new WatermarkManager();
        this.adaptiveWindowManager = enableAdaptiveWindow ?
                new AdaptiveWindowManager(Duration.ofSeconds(10)) : null;
        this.anomalyDetector = new AnomalyDetector(this::handleAnomaly);
        this.backpressureController = enableBackpressure ?
                new BackpressureController() : null;
    }

    /**
     * 启动引擎
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        log.info("ExecutionEngine started with parallelism={}", parallelism);

        // 启动Watermark生成器
        executorService.submit(this::generateWatermarks);

        // 启动自适应调整器
        if (enableAdaptiveWindow) {
            executorService.submit(this::adaptiveAdjustmentLoop);
        }

        // 启动背压监控
        if (enableBackpressure) {
            executorService.submit(this::backpressureMonitorLoop);
        }
    }

    /**
     * 处理单条记录
     */
    public <T> List<StreamRecord<T>> processRecord(StreamRecord<T> record,
                                                    List<StreamOperator<T>> operators,
                                                    DataSink<T> sink) {
        List<StreamRecord<T>> results = new ArrayList<>();
        results.add(record);

        // 背压检查
        if (enableBackpressure && !backpressureController.tryAcquire()) {
            // 限流：等待
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 处理Watermark
        if (record.getValue() instanceof Watermark) {
            Watermark watermark = (Watermark) record.getValue();
            watermarkManager.updateWatermark(watermark);

            // 通知所有算子
            for (StreamOperator<T> operator : operators) {
                operator.processWatermark(watermark);
            }
            return results;
        }

        // 自适应窗口采样
        if (enableAdaptiveWindow && adaptiveWindowManager != null) {
            adaptiveWindowManager.collectSample(record);
        }

        // 异常检测采样
        anomalyDetector.recordSample(1.0); // 简化为记录计数

        // 执行算子链
        long startTime = System.currentTimeMillis();

        for (StreamOperator<T> operator : operators) {
            List<StreamRecord<T>> nextResults = new ArrayList<>();
            for (StreamRecord<T> r : results) {
                List<StreamRecord<T>> processed = operator.processElement(r);
                if (processed != null) {
                    nextResults.addAll(processed);
                }
            }
            results = nextResults;
        }

        // 输出到sink
        if (sink != null) {
            for (StreamRecord<T> r : results) {
                try {
                    sink.write(r.getValue());
                } catch (Exception e) {
                    log.error("Sink write failed", e);
                }
            }
        }

        // 记录处理延迟
        long latency = System.currentTimeMillis() - startTime;
        if (enableBackpressure && backpressureController != null) {
            backpressureController.recordSample(record, latency);
        }

        return results;
    }

    /**
     * 生成Watermark
     */
    private void generateWatermarks() {
        while (running) {
            try {
                Thread.sleep(watermarkInterval.toMillis());

                long currentWatermark = watermarkManager.getCurrentWatermark();
                if (enableAdaptiveWindow && adaptiveWindowManager != null) {
                    // 使用自适应的Watermark延迟
                    long delay = adaptiveWindowManager.getRecommendedWatermarkDelay().toMillis();
                    currentWatermark = System.currentTimeMillis() - delay;
                }

                Watermark watermark = new Watermark(currentWatermark);
                watermarkManager.updateWatermark(watermark);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * 自适应调整循环
     */
    private void adaptiveAdjustmentLoop() {
        while (running) {
            try {
                Thread.sleep(5000); // 每5秒检查一次

                if (adaptiveWindowManager != null) {
                    Duration newSize = adaptiveWindowManager.getCurrentWindowSize();
                    log.debug("Adaptive window size: {}", newSize);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * 背压监控循环
     */
    private void backpressureMonitorLoop() {
        while (running) {
            try {
                Thread.sleep(1000);

                if (backpressureController != null) {
                    BackpressureController.SystemStatus status = backpressureController.getStatus();
                    if (status.getPressureLevel() != BackpressureController.PressureLevel.NORMAL) {
                        log.warn("Backpressure detected: {}", status);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * 处理异常告警
     */
    private void handleAnomaly(AnomalyDetector.AnomalyResult result) {
        log.warn("Anomaly detected: level={}, value={}, changeRate={}",
                result.getLevel(),
                String.format("%.2f", result.getCurrentValue()),
                String.format("%.2f%%", result.getChangeRate() * 100));

        // 根据异常级别调整系统参数
        switch (result.getLevel()) {
            case CRITICAL:
                if (enableBackpressure && backpressureController != null) {
                    // 强制降低处理速率
                    backpressureController.setQueueSize(100000); // 模拟高队列
                }
                break;
            case HIGH:
                // 记录告警，调整监控频率
                break;
            case MEDIUM:
                // 记录日志
                break;
            default:
                break;
        }
    }

    /**
     * 停止引擎
     */
    public void stop() {
        running = false;
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("ExecutionEngine stopped");
    }

    /**
     * 获取引擎状态
     */
    public EngineStatus getStatus() {
        return new EngineStatus(
                running,
                parallelism,
                watermarkManager.getCurrentWatermark(),
                enableBackpressure && backpressureController != null ?
                        backpressureController.getStatus() : null,
                anomalyDetector.getStatistics()
        );
    }

    // ============== 状态类 ==============

    public record EngineStatus(
            boolean running,
            int parallelism,
            long currentWatermark,
            BackpressureController.SystemStatus backpressureStatus,
            AnomalyDetector.TrafficStatistics anomalyStats
    ) {}
}
