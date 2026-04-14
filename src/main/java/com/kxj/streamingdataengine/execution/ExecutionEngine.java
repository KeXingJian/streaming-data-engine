package com.kxj.streamingdataengine.execution;

import com.kxj.streamingdataengine.ai.AdaptiveWindowManager;
import com.kxj.streamingdataengine.ai.AnomalyDetector;
import com.kxj.streamingdataengine.ai.BackpressureController;
import com.kxj.streamingdataengine.core.model.*;
import com.kxj.streamingdataengine.core.operator.StreamOperator;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * 流处理执行引擎
 * 核心组件协调和调度
 */
@Slf4j
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

    public ExecutionEngine(int parallelism, Duration watermarkInterval,
                          boolean enableAdaptiveWindow, boolean enableBackpressure) {
        this.parallelism = parallelism;                         // 并行度，同时处理任务的线程数
        this.watermarkInterval = watermarkInterval;             // Watermark生成间隔，推进事件时间
        this.enableAdaptiveWindow = enableAdaptiveWindow;       // 是否启用自适应窗口动态调整
        this.enableBackpressure = enableBackpressure;           // 是否启用背压控制防止系统过载

        this.executorService = Executors.newVirtualThreadPerTaskExecutor(); // 虚拟线程池，调度异步任务
        this.watermarkManager = new WatermarkManager();         // Watermark管理器，维护当前事件时间水位线
        this.adaptiveWindowManager = enableAdaptiveWindow ?
                new AdaptiveWindowManager(Duration.ofSeconds(10)) : null;   // 自适应窗口管理器，动态优化窗口参数
        this.anomalyDetector = new AnomalyDetector(this::handleAnomaly);    // 异常检测器，监控流量异常并告警
        this.backpressureController = enableBackpressure ?
                new BackpressureController() : null;            // 背压控制器，根据负载动态限流
    }

    /**
     * 启动引擎
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        // [kxj: 执行引擎启动 - 根据配置启动虚拟线程池、Watermark生成、自适应调整和背压监控]
        log.info("[kxj: 执行引擎启动] parallelism={}, 自适应窗口={}, 背压控制={}",
                parallelism, enableAdaptiveWindow, enableBackpressure);

        // [kxj: 启动Watermark生成器，按固定间隔推进事件时间]
        executorService.submit(this::generateWatermarks);

        // [kxj: 启动自适应窗口调整器，基于延迟样本动态优化窗口参数]
        if (enableAdaptiveWindow) {
            executorService.submit(this::adaptiveAdjustmentLoop);
        }

        // [kxj: 启动背压监控，根据系统负载动态限流]
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

        //kxj减速降温
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
        if (record.value() instanceof Watermark watermark) {
            watermarkManager.updateWatermark(watermark);

            // 通知所有算子
            for (StreamOperator<T> operator : operators) {
                operator.processWatermark(watermark);
            }
            return results;
        }

        // 自适应窗口采样
        if (enableAdaptiveWindow) {
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
                    sink.write(r.value());
                } catch (Exception e) {
                    log.error("Sink write failed", e);
                }
            }
        }

        // 记录处理延迟
        long latency = System.currentTimeMillis() - startTime;
        if (enableBackpressure) {
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
                if (enableAdaptiveWindow) {
                    // 使用自适应的Watermark延迟
                    currentWatermark = System.currentTimeMillis() - adaptiveWindowManager.getRecommendedWatermarkDelayMs();
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
            //kxj: 获取窗口大小
            try {
                Thread.sleep(5000); // 每5秒检查一次

                Duration newSize = adaptiveWindowManager.getCurrentWindowSize();
                log.debug("Adaptive window size: {}", newSize);
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

                BackpressureController.SystemStatus status = backpressureController.getStatus();
                if (status.pressureLevel() != BackpressureController.PressureLevel.NORMAL) {
                    log.warn("Backpressure detected: {}", status);
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
                if (enableBackpressure) {
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
        new StringBuffer();
        StringBuilder sb = new StringBuilder();
        return new EngineStatus(
                running,
                parallelism,
                watermarkManager.getCurrentWatermark(),
                enableBackpressure ? backpressureController.getStatus() : null,
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
