package com.kxj.streamingdataengine.demo;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.ai.AdaptiveWindowManager;
import com.kxj.streamingdataengine.ai.AnomalyDetector;
import com.kxj.streamingdataengine.ai.BackpressureController;
import com.kxj.streamingdataengine.core.model.DataStream;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.execution.ExecutionEngine;
import com.kxj.streamingdataengine.sink.ConsoleSink;
import com.kxj.streamingdataengine.stream.StreamBuilder;
import com.kxj.streamingdataengine.window.WindowAssigner;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 流处理引擎演示
 * 展示核心功能：LSM-Tree存储、Watermark、窗口聚合、AI自适应控制
 */
@Slf4j
public class StreamingDemo {

    public static void main(String[] args) throws Exception {
        log.info("=== 高性能流式数据处理引擎演示 ===");

        // 1. 基础流处理演示
        basicStreamDemo();

        // 2. 窗口聚合演示
        windowAggregationDemo();

        // 3. 自适应窗口演示
        adaptiveWindowDemo();

        // 4. 异常检测演示
        anomalyDetectionDemo();

        // 5. 背压控制演示
        backpressureDemo();

        log.info("=== 所有演示完成 ===");
    }

    /**
     * 基础流处理
     */
    private static void basicStreamDemo() {
        log.info("\n--- 基础流处理演示 ---");

        List<Integer> data = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        StreamBuilder builder = new StreamBuilder("basic-stream");

        builder.fromCollection(data)
                .filter(n -> n % 2 == 0)  // 过滤偶数
                .map(n -> n * n)           // 平方
                .addSink(new ConsoleSink<>(n -> "处理结果: " + n))
                .execute();

        log.info("基础流处理完成");
    }

    /**
     * 窗口聚合演示
     */
    private static void windowAggregationDemo() {
        log.info("\n--- 窗口聚合演示 ---");

        // 生成测试数据（模拟时间序列）
        List<SensorReading> readings = generateSensorData(100);

        StreamBuilder builder = new StreamBuilder("window-aggregation");

        builder.fromCollection(readings)
                .map(r -> "传感器" + r.sensorId + ": " + r.temperature + "°C")
                .addSink(new ConsoleSink<>())
                .execute();

        // 计数聚合
        long count = readings.size();
        log.info("总记录数: {}", count);

        // 求和聚合
        double avgTemp = readings.stream()
                .mapToDouble(r -> r.temperature)
                .average()
                .orElse(0);
        log.info("平均温度: {}°C", String.format("%.2f", avgTemp));
    }

    /**
     * 自适应窗口演示
     */
    private static void adaptiveWindowDemo() throws InterruptedException {
        log.info("\n--- 自适应窗口演示 ---");

        AdaptiveWindowManager manager = new AdaptiveWindowManager(Duration.ofSeconds(10));

        // 模拟数据到达
        Random random = new Random();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        CountDownLatch latch = new CountDownLatch(1);

        executor.submit(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    // 模拟不同延迟的数据
                    long latency = random.nextInt(500) + 100; // 100-600ms延迟
                    StreamRecord<String> record = StreamRecord.<String>builder()
                            .key("key" + i)
                            .value("data" + i)
                            .eventTime(System.currentTimeMillis() - latency)
                            .processingTime(System.currentTimeMillis())
                            .build();

                    manager.collectSample(record);

                    Thread.sleep(50); // 20条/秒
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        });

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        log.info("自适应窗口大小: {}", manager.getCurrentWindowSize());
        log.info("推荐Watermark延迟: {}", manager.getRecommendedWatermarkDelay());
    }

    /**
     * 异常检测演示
     */
    private static void anomalyDetectionDemo() throws InterruptedException {
        log.info("\n--- 异常检测演示 ---");

        AnomalyDetector detector = new AnomalyDetector(result -> {
            log.warn("异常告警: level={}, value={}",
                    result.getLevel(), String.format("%.2f", result.getCurrentValue()));
        });

        Random random = new Random();

        // 正常流量
        log.info("模拟正常流量...");
        for (int i = 0; i < 50; i++) {
            detector.recordSample(100 + random.nextDouble() * 20); // 100±20
            Thread.sleep(10);
        }

        // 突发流量（异常）
        log.info("模拟突发流量（异常）...");
        for (int i = 0; i < 10; i++) {
            detector.recordSample(300 + random.nextDouble() * 50); // 突然升高到300
            Thread.sleep(10);
        }

        // 恢复
        log.info("恢复正常流量...");
        for (int i = 0; i < 20; i++) {
            detector.recordSample(100 + random.nextDouble() * 20);
            Thread.sleep(10);
        }

        AnomalyDetector.TrafficStatistics stats = detector.getStatistics();
        log.info("流量统计: mean={}, stdDev={}",
                String.format("%.2f", stats.getMean()),
                String.format("%.2f", stats.getStdDev()));
    }

    /**
     * 背压控制演示
     */
    private static void backpressureDemo() throws InterruptedException {
        log.info("\n--- 背压控制演示 ---");

        BackpressureController controller = new BackpressureController(200); // 目标延迟200ms
        AtomicLong processedCount = new AtomicLong(0);

        ExecutorService producer = Executors.newSingleThreadExecutor();
        ExecutorService consumer = Executors.newSingleThreadExecutor();

        BlockingQueue<String> queue = new LinkedBlockingQueue<>(1000);

        // 生产者线程
        producer.submit(() -> {
            Random random = new Random();
            for (int i = 0; i < 500; i++) {
                // 检查限流
                while (!controller.tryAcquire()) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }

                try {
                    queue.put("data-" + i);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });

        // 消费者线程
        consumer.submit(() -> {
            Random random = new Random();
            for (int i = 0; i < 500; i++) {
                try {
                    String data = queue.take();
                    long startTime = System.currentTimeMillis();

                    // 模拟处理延迟
                    Thread.sleep(random.nextInt(10));

                    StreamRecord<String> record = StreamRecord.<String>builder()
                            .key("key")
                            .value(data)
                            .eventTime(System.currentTimeMillis())
                            .processingTime(System.currentTimeMillis())
                            .build();

                    long latency = System.currentTimeMillis() - startTime;
                    controller.recordSample(record, latency);
                    controller.setQueueSize(queue.size());

                    processedCount.incrementAndGet();

                    // 每100条打印状态
                    if (i % 100 == 0) {
                        BackpressureController.SystemStatus status = controller.getStatus();
                        log.info("处理进度: {}/{}, 压力等级: {}, 限流: {}/秒, 队列: {}",
                                i, 500, status.getPressureLevel(),
                                status.getRateLimit(), status.getQueueSize());
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });

        producer.shutdown();
        consumer.shutdown();
        producer.awaitTermination(30, TimeUnit.SECONDS);
        consumer.awaitTermination(30, TimeUnit.SECONDS);

        log.info("背压控制演示完成，处理记录数: {}", processedCount.get());
    }

    /**
     * 生成传感器测试数据
     */
    private static List<SensorReading> generateSensorData(int count) {
        Random random = new Random();
        List<SensorReading> data = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            data.add(new SensorReading(
                    "sensor-" + random.nextInt(5),
                    20 + random.nextDouble() * 15, // 20-35度
                    System.currentTimeMillis() + i * 1000
            ));
        }

        return data;
    }

    /**
     * 传感器读数
     */
    private record SensorReading(String sensorId, double temperature, long timestamp) {}
}
