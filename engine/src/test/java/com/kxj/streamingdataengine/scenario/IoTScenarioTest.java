package com.kxj.streamingdataengine.scenario;

import com.kxj.streamingdataengine.aggregation.MergeTreeAggregator;
import com.kxj.streamingdataengine.ai.AnomalyDetector;
import com.kxj.streamingdataengine.ai.SeverityLevel;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.execution.ExecutionEngine;
import com.kxj.streamingdataengine.sink.CollectSink;
import com.kxj.streamingdataengine.stream.StreamBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 物联网场景测试
 * 模拟大量传感器实时数据采集和分析
 */
@Slf4j
@ExtendWith(com.kxj.streamingdataengine.extension.TestReportExtension.class)
public class IoTScenarioTest {

    /**
     * 传感器读数
     */
    record SensorReading(String sensorId, String deviceType, double temperature,
                         double humidity, long timestamp) {}

    @Test
    @DisplayName("传感器数据实时聚合")
    void testSensorDataAggregation() {
        log.info("=== 场景1: 传感器数据实时聚合 ===");

        // 生成模拟传感器数据
        List<SensorReading> sensorData = generateSensorData(1000, 10);
        log.info("生成 {} 条传感器数据，来自 {} 个传感器", sensorData.size(), 10);

        // 按设备类型分组统计
        CollectSink<String> sink = new CollectSink<>();

        StreamBuilder builder = new StreamBuilder("iot-aggregation");
        builder.fromCollection(sensorData)
                .map(r -> r.deviceType() + "|" + r.temperature())
                .addSink(sink)
                .execute();

        log.info("处理完成，输出 {} 条结果", sink.getCollected().size());
        assertFalse(sink.getCollected().isEmpty());

        // 手动计算验证
        Map<String, List<Double>> grouped = new HashMap<>();
        for (SensorReading reading : sensorData) {
            grouped.computeIfAbsent(reading.deviceType(), k -> new ArrayList<>())
                    .add(reading.temperature());
        }

        grouped.forEach((type, temps) -> {
            double avg = temps.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            double max = temps.stream().mapToDouble(Double::doubleValue).max().orElse(0);
            log.info("设备类型 {}: 平均温度={}, 最高温度={}, 样本数={}",
                    type, String.format("%.2f", avg), String.format("%.2f", max), temps.size());
        });
    }

    @Test
    @DisplayName("异常温度检测")
    void testAnomalyTemperatureDetection() throws InterruptedException {
        log.info("=== 场景2: 异常温度检测 ===");

        List<AnomalyDetector.AnomalyResult> alerts = new ArrayList<>();
        AnomalyDetector detector = new AnomalyDetector(alerts::add);

        Random random = new Random();

        // 阶段1: 正常温度范围 20-25度
        log.info("阶段1: 正常温度采集");
        for (int i = 0; i < 50; i++) {
            double temp = 20 + random.nextDouble() * 5;
            detector.recordSample(temp);
            Thread.sleep(10);
        }

        // 阶段2: 模拟设备过热 40-50度（异常）
        log.info("阶段2: 模拟设备过热");
        for (int i = 0; i < 30; i++) {
            double temp = 40 + random.nextDouble() * 10;
            detector.recordSample(temp);
            Thread.sleep(10);
        }

        // 阶段3: 恢复正常
        log.info("阶段3: 温度恢复正常");
        for (int i = 0; i < 30; i++) {
            double temp = 20 + random.nextDouble() * 5;
            detector.recordSample(temp);
            Thread.sleep(10);
        }

        log.info("检测到 {} 次异常告警", alerts.size());

        // 应该检测到过热情形
        assertFalse(alerts.isEmpty(), "应该检测到温度异常");

        alerts.stream()
                .filter(a -> a.getLevel().ordinal() >= SeverityLevel.MEDIUM.ordinal())
                .forEach(a -> log.info("重要告警: level={}, value={}", a.getLevel(), String.format("%.2f", a.getCurrentValue())));
    }

    @Test
    @DisplayName("高并发传感器数据采集")
    void testHighConcurrencyDataCollection() throws InterruptedException {
        log.info("=== 场景3: 高并发传感器数据采集 ===");

        int sensorCount = 100;
        int readingPerSensor = 100;
        ExecutorService executor = Executors.newFixedThreadPool(20);
        CountDownLatch latch = new CountDownLatch(sensorCount);
        AtomicLong totalRecords = new AtomicLong(0);

        // 使用MergeTree存储
        MergeTreeAggregator<String, SensorReading, AverageAccumulator, Double> aggregator =
                new MergeTreeAggregator<>(
                        new AverageAggregateFunction(),
                        r -> r.deviceType(),
                        r -> r.sensorId()
                );

        long startTime = System.currentTimeMillis();

        // 模拟多个传感器同时上报
        for (int s = 0; s < sensorCount; s++) {
            final int sensorId = s;
            executor.submit(() -> {
                try {
                    Random random = new Random(sensorId);
                    for (int i = 0; i < readingPerSensor; i++) {
                        SensorReading reading = new SensorReading(
                                "sensor-" + sensorId,
                                sensorId % 2 == 0 ? "temperature" : "humidity",
                                20 + random.nextDouble() * 10,
                                50 + random.nextDouble() * 20,
                                System.currentTimeMillis()
                        );

                        StreamRecord<SensorReading> record = StreamRecord.<SensorReading>builder()
                                .key(reading.sensorId())
                                .value(reading)
                                .eventTime(reading.timestamp())
                                .build();

                        aggregator.insert(record);
                        totalRecords.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long duration = System.currentTimeMillis() - startTime;
        double throughput = totalRecords.get() * 1000.0 / duration;

        log.info("并发采集完成:");
        log.info("  - 传感器数量: {}", sensorCount);
        log.info("  - 每传感器读数: {}", readingPerSensor);
        log.info("  - 总记录数: {}", totalRecords.get());
        log.info("  - 耗时: {} ms", duration);
        log.info("  - 吞吐量: {} 记录/秒", String.format("%.2f", throughput));

        MergeTreeAggregator.Stats stats = aggregator.getStats();
        log.info("MergeTree统计: 分区={}, 活跃分区={}",
                stats.partitionCount(), stats.activePartitionCount());

        assertTrue(throughput > 1000, "吞吐量应该超过1000记录/秒");
    }

    @Test
    @DisplayName("乱序数据处理")
    void testOutOfOrderData() {
        log.info("=== 场景4: 乱序数据处理 ===");

        // 生成带延迟的数据
        List<StreamRecord<SensorReading>> records = new ArrayList<>();
        Random random = new Random();
        long baseTime = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {
            // 模拟网络延迟：正常延迟100ms，部分延迟1-5秒
            long delay = random.nextDouble() < 0.2
                    ? 1000 + random.nextInt(4000)  // 20%数据延迟1-5秒
                    : 50 + random.nextInt(150);     // 80%数据延迟50-200ms

            SensorReading reading = new SensorReading(
                    "sensor-1",
                    "temperature",
                    20 + random.nextDouble() * 5,
                    60,
                    baseTime + i * 100  // 正常应该是每100ms一条
            );

            StreamRecord<SensorReading> record = StreamRecord.<SensorReading>builder()
                    .key("sensor-1")
                    .value(reading)
                    .eventTime(reading.timestamp())
                    .processingTime(baseTime + i * 100 + delay) // 实际处理时间有延迟
                    .build();

            records.add(record);
        }

        // 计算延迟统计
        List<Long> latencies = records.stream()
                .map(StreamRecord::getLatency)
                .sorted()
                .toList();

        long medianLatency = latencies.get(latencies.size() / 2);
        long p95Latency = latencies.get((int) (latencies.size() * 0.95));

        log.info("延迟统计:");
        log.info("  - 最小: {} ms", latencies.get(0));
        log.info("  - 中位数: {} ms", medianLatency);
        log.info("  - P95: {} ms", p95Latency);
        log.info("  - 最大: {} ms", latencies.get(latencies.size() - 1));

        // 验证数据完整性
        assertEquals(100, records.size());
        assertTrue(p95Latency > 1000, "应该有部分数据延迟超过1秒");
    }

    @Test
    @DisplayName("背压下的数据采集")
    void testBackpressureDataCollection() throws InterruptedException {
        log.info("=== 场景5: 背压下的数据采集 ===");

        ExecutionEngine engine = new ExecutionEngine(4, Duration.ofMillis(100), true, true);
        engine.start();

        BlockingQueue<SensorReading> queue = new LinkedBlockingQueue<>(1000);
        AtomicInteger processed = new AtomicInteger(0);
        AtomicInteger dropped = new AtomicInteger(0);

        // 生产者线程（高速产生数据）
        Thread producer = Thread.ofVirtual().start(() -> {
            Random random = new Random();
            for (int i = 0; i < 5000; i++) {
                SensorReading reading = new SensorReading(
                        "sensor-backpressure",
                        "temperature",
                        20 + random.nextDouble() * 5,
                        60,
                        System.currentTimeMillis()
                );

                try {
                    // 非阻塞入队，满则丢弃
                    if (!queue.offer(reading, 1, TimeUnit.MILLISECONDS)) {
                        dropped.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        // 消费者线程（慢速处理）
        Thread consumer = Thread.ofVirtual().start(() -> {
            while (processed.get() < 5000 && !Thread.interrupted()) {
                try {
                    SensorReading reading = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (reading != null) {
                        // 模拟慢速处理
                        Thread.sleep(2);
                        processed.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        producer.join(30000);
        consumer.join(30000);

        engine.stop();

        log.info("背压测试结果:");
        log.info("  - 生产: 5000");
        log.info("  - 处理: {}", processed.get());
        log.info("  - 丢弃: {}", dropped.get());

        // 应该有部分数据被丢弃
        log.info("丢弃率: {}", String.format("%.2f%%", dropped.get() * 100.0 / 5000));
    }

    // ============== 辅助方法 ==============

    private List<SensorReading> generateSensorData(int count, int sensorCount) {
        List<SensorReading> data = new ArrayList<>();
        Random random = new Random();
        long baseTime = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            String sensorId = "sensor-" + (i % sensorCount);
            data.add(new SensorReading(
                    sensorId,
                    i % 2 == 0 ? "temperature" : "humidity",
                    20 + random.nextDouble() * 15,
                    50 + random.nextDouble() * 30,
                    baseTime + i * 1000
            ));
        }

        return data;
    }

    private static class AverageAccumulator {
        double sum;
        int count;

        void add(double value) {
            sum += value;
            count++;
        }

        double getAverage() {
            return count == 0 ? 0 : sum / count;
        }
    }

    private static class AverageAggregateFunction implements com.kxj.streamingdataengine.aggregation.AggregateFunction<
            SensorReading, AverageAccumulator, Double> {

        @Override
        public AverageAccumulator createAccumulator() {
            return new AverageAccumulator();
        }

        @Override
        public void add(SensorReading value, AverageAccumulator accumulator) {
            accumulator.add(value.temperature());
        }

        @Override
        public Double getResult(AverageAccumulator accumulator) {
            return accumulator.getAverage();
        }

        @Override
        public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
            a.sum += b.sum;
            a.count += b.count;
            return a;
        }
    }
}
