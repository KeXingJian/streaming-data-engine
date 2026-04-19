package com.kxj.streamingdataengine.execution;

import com.kxj.streamingdataengine.ai.AdaptiveWindowManager;
import com.kxj.streamingdataengine.ai.AnomalyDetector;
import com.kxj.streamingdataengine.ai.BackpressureController;
import com.kxj.streamingdataengine.ai.SeverityLevel;
import com.kxj.streamingdataengine.checkpoint.*;
import com.kxj.streamingdataengine.core.model.*;
import com.kxj.streamingdataengine.core.operator.StreamOperator;
import com.kxj.streamingdataengine.state.StateBackend;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

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

    // 活跃算子链与sink，用于Watermark驱动
    private volatile List<StreamOperator<?>> activeOperators = Collections.emptyList();
    private volatile DataSink<Object> activeSink = null;

    // 统计计数
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong filteredCount = new AtomicLong(0);

    // Checkpoint 相关
    private CheckpointCoordinator checkpointCoordinator;
    private StateBackend stateBackend;
    private CheckpointConfig checkpointConfig;

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
                new BackpressureController() : null;            // 背压控制器，根据负载动态监控和限流
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

        // [kxj: 启动Checkpoint协调器，定时触发状态快照]
        if (checkpointCoordinator != null) {
            checkpointCoordinator.start();
        }

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
     * 初始化 Checkpoint 支持
     */
    public void initializeCheckpoint(CheckpointConfig config, StateBackend stateBackend,
                                     CheckpointCoordinator.CheckpointListener listener) {
        this.checkpointConfig = config;
        this.stateBackend = stateBackend;
        if (config != null && config.isEnabled()) {
            this.checkpointCoordinator = new CheckpointCoordinator(config, stateBackend, listener);
            log.info("[kxj: Checkpoint 已初始化] interval={}ms", config.getInterval().toMillis());
        }
    }

    /**
     * 触发手动 Checkpoint
     */
    public void triggerCheckpoint() {
        if (checkpointCoordinator != null) {
            checkpointCoordinator.triggerCheckpoint();
        }
    }

    /**
     * 获取 CheckpointCoordinator
     */
    public CheckpointCoordinator getCheckpointCoordinator() {
        return checkpointCoordinator;
    }

    /**
     * 处理单条记录
     */
    @SuppressWarnings("unchecked")
    public <T> List<StreamRecord<T>> processRecord(StreamRecord<T> record,
                                                    List<StreamOperator<T>> operators,
                                                    DataSink<T> sink) {
        List<StreamRecord<T>> results = new ArrayList<>();
        results.add(record);

        // 处理 Checkpoint Barrier
        if (record.value() instanceof CheckpointBarrier barrier) {
            processCheckpointBarrier(barrier, operators);
            return results;
        }

        // 处理Watermark
        if (record.value() instanceof Watermark watermark) {
            watermarkManager.updateWatermark(watermark);
            @SuppressWarnings("unchecked")
            List<StreamOperator<?>> rawOperators = (List<StreamOperator<?>>) (List<?>) operators;
            processWatermarkThroughPipeline(watermark, rawOperators, (DataSink<Object>) sink);
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

        // 统计
        if (results.isEmpty()) {
            filteredCount.incrementAndGet();
        } else {
            processedCount.addAndGet(results.size());
        }

        return results;
    }

    /**
     * 处理 Checkpoint Barrier
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T> void processCheckpointBarrier(CheckpointBarrier barrier, List<StreamOperator<T>> operators) {
        log.debug("[kxj: 处理 Checkpoint Barrier #{}]", barrier.getCheckpointNumber());

        // 触发各算子的状态快照
        for (StreamOperator operator : operators) {
            if (operator instanceof Snapshotable snapshotable) {
                try {
                    var snapshot = snapshotable.snapshotState(
                        barrier.getCheckpointId(),
                        barrier.getCheckpointNumber(),
                        stateBackend
                    );

                    // 通知 CheckpointCoordinator
                    if (checkpointCoordinator != null) {
                        checkpointCoordinator.acknowledgeCheckpoint(
                            barrier.getCheckpointNumber(),
                            snapshotable.getOperatorId(),
                            snapshot
                        );
                    }
                } catch (Exception e) {
                    log.error("[kxj: 算子快照失败 - operator={}]", operator.getName(), e);
                    if (checkpointCoordinator != null) {
                        checkpointCoordinator.acknowledgeCheckpoint(
                            barrier.getCheckpointNumber(),
                            snapshotable.getOperatorId(),
                            null
                        );
                    }
                }
            }
        }
    }

    /**
     * 设置当前活跃的算子链与sink，供全局Watermark驱动使用
     */
    @SuppressWarnings("unchecked")
    public <T> void setPipeline(List<StreamOperator<T>> operators, DataSink<T> sink) {
        this.activeOperators = (List<StreamOperator<?>>) (List<?>) operators;
        this.activeSink = (DataSink<Object>) sink;
    }

    /**
     * 将Watermark注入算子链，处理算子因Watermark推进而发射的记录
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void processWatermarkThroughPipeline(Watermark watermark,
                                                  List<StreamOperator<?>> operators,
                                                  DataSink<Object> sink) {
        List pending = List.of(watermark);
        for (StreamOperator operator : operators) {
            List nextPending = new ArrayList();
            for (Object item : pending) {
                if (item instanceof StreamRecord record) {
                    List processed = operator.processElement(record);
                    if (processed != null) {
                        nextPending.addAll(processed);
                    }
                }
            }
            List emitted = operator.processWatermark(watermark);
            System.out.println("[ExecutionEngine] processWatermarkThroughPipeline operator=" + operator.getName()
                    + " wm=" + watermark.getTimestamp()
                    + " emitted=" + (emitted != null ? emitted.size() : 0)
                    + " values=" + (emitted != null ? emitted : "null"));
            if (emitted != null) {
                nextPending.addAll(emitted);
            }
            pending = nextPending;
        }
        System.out.println("[ExecutionEngine] processWatermarkThroughPipeline final pending size=" + pending.size()
                + " values=" + pending);
        if (sink != null) {
            for (Object item : pending) {
                if (item instanceof StreamRecord record) {
                    try {
                        System.out.println("[ExecutionEngine] writing to sink: " + record.value());
                        sink.write(record.value());
                    } catch (Exception e) {
                        log.error("Sink write failed", e);
                    }
                }
            }
        }
    }

    /**
     * 创建带自然背压的流水线
     * [kxj: 管道级自然背压 - BlockingQueue + 虚拟线程消费者，队列满时主线程自动阻塞]
     */
    public <T> Pipeline<T> createPipeline(List<StreamOperator<T>> operators, DataSink<T> sink, int bufferSize) {
        return new Pipeline<>(operators, sink, bufferSize);
    }

    public long getProcessedCount() {
        return processedCount.get();
    }

    public long getFilteredCount() {
        return filteredCount.get();
    }

    /**
     * 流水线：生产者-消费者模型，利用 BlockingQueue 实现自然背压
     */
    public class Pipeline<T> {
        private final BlockingQueue<StreamRecord<T>> queue;
        private final CountDownLatch latch;

        Pipeline(List<StreamOperator<T>> operators, DataSink<T> sink, int bufferSize) {
            this.queue = new ArrayBlockingQueue<>(Math.max(bufferSize, 1));
            this.latch = new CountDownLatch(parallelism);

            for (int i = 0; i < parallelism; i++) {
                executorService.submit(() -> {
                    try {
                        while (true) {
                            StreamRecord<T> record = queue.take();
                            // 毒丸信号：key 和 value 均为 null
                            if (record.key() == null && record.value() == null) {
                                break;
                            }
                            processRecord(record, operators, sink);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        latch.countDown();
                    }
                });
            }
        }

        public void submit(StreamRecord<T> record) throws InterruptedException {
            queue.put(record);
        }

        /**
         * 发送毒丸并等待所有消费者结束
         */
        public void complete() throws InterruptedException {
            for (int i = 0; i < parallelism; i++) {
                queue.put(new StreamRecord<>(null, null, 0, 0, 0));
            }
            latch.await();
        }
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

                // [kxj: 将全局Watermark驱动注入活跃算子链]
                processWatermarkThroughPipeline(watermark, activeOperators, activeSink);

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
                if (status.pressureLevel() != SeverityLevel.NORMAL) {
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
                    // [kxj: CRITICAL异常流量 - 背压控制器已根据延迟自动限流]
                    log.warn("[kxj: CRITICAL异常流量] 背压控制器自动响应中");
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

        // [kxj: 停止Checkpoint协调器]
        if (checkpointCoordinator != null) {
            checkpointCoordinator.stop();
        }

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
