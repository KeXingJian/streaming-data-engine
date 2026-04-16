package com.kxj.streamingdataengine.performance;

import com.kxj.streamingdataengine.aggregation.MergeTreeAggregator;
import com.kxj.streamingdataengine.ai.BackpressureController;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.storage.lsm.LSMTree;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 性能基准测试
 */
@Slf4j
@ExtendWith(com.kxj.streamingdataengine.extension.TestReportExtension.class)
public class PerformanceBenchmarkTest {

    @Test
    @DisplayName("LSM-Tree写入吞吐量")
    void testLSMTreeWriteThroughput() {
        log.info("=== LSM-Tree写入吞吐量测试 ===");

        LSMTree<String, String> tree = new LSMTree<>();

        int[] batchSizes = {1000, 10000, 100000};

        for (int batchSize : batchSizes) {
            Map<String, String> data = new HashMap<>();
            for (int i = 0; i < batchSize; i++) {
                data.put("key" + i, "value" + i);
            }

            // 预热
            tree.putAll(data);

            // 正式测试
            long start = System.nanoTime();
            tree.putAll(data);
            long duration = System.nanoTime() - start;

            double throughput = batchSize * 1_000_000_000.0 / duration;
            double latency = duration / 1_000_000.0 / batchSize;

            log.info("批量大小 {}: 吞吐量={}, 平均延迟={} ms/op",
                    batchSize, String.format("%.2f", throughput), String.format("%.3f", latency));
        }
    }

    @Test
    @DisplayName("LSM-Tree读取性能")
    void testLSMTreeReadPerformance() {
        log.info("=== LSM-Tree读取性能测试 ===");

        LSMTree<String, String> tree = new LSMTree<>();

        // 准备数据
        int dataSize = 10000;
        for (int i = 0; i < dataSize; i++) {
            tree.put("key" + i, "value" + i);
        }

        // 随机读取测试
        Random random = new Random();
        int readCount = 10000;

        long start = System.nanoTime();
        int hitCount = 0;
        for (int i = 0; i < readCount; i++) {
            String key = "key" + random.nextInt(dataSize * 2); // 50%命中率
            if (tree.get(key).isPresent()) {
                hitCount++;
            }
        }
        long duration = System.nanoTime() - start;

        double throughput = readCount * 1_000_000_000.0 / duration;
        double avgLatency = duration / 1_000_000.0 / readCount;

        log.info("随机读取 {} 次: 命中率={}, 吞吐量={}, 平均延迟={} μs",
                readCount, String.format("%.1f%%", hitCount * 100.0 / readCount),
                String.format("%.2f", throughput), String.format("%.3f", avgLatency));

        assertTrue(throughput > 100000, "读取吞吐量应超过10万ops/s");
    }

    @Test
    @DisplayName("MergeTree聚合性能")
    void testMergeTreeAggregationPerformance() {
        log.info("=== MergeTree聚合性能测试 ===");

        MergeTreeAggregator<String, Double, Double, Double> aggregator =
                new MergeTreeAggregator<>(
                        new SumAggregateFunction(),
                        v -> "partition1",
                        v -> "key" + (int) (v % 100)
                );

        int[] recordCounts = {10000, 50000, 100000};

        for (int count : recordCounts) {
            List<StreamRecord<Double>> records = new ArrayList<>();
            Random random = new Random();

            for (int i = 0; i < count; i++) {
                records.add(StreamRecord.<Double>builder()
                        .key("key" + i)
                        .value(random.nextDouble() * 100)
                        .eventTime(System.currentTimeMillis())
                        .build());
            }

            long start = System.nanoTime();
            aggregator.insertBatch(records);
            long duration = System.nanoTime() - start;

            double throughput = count * 1_000_000_000.0 / duration;

            log.info("聚合 {} 条记录: 吞吐量={} 记录/s", count, String.format("%.2f", throughput));
        }
    }

    @Test
    @DisplayName("背压控制器并发性能")
    void testBackpressureConcurrency() throws InterruptedException {
        log.info("=== 背压控制器并发性能测试 ===");

        BackpressureController controller = new BackpressureController(200);

        int threadCount = 20;
        int iterations = 10000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicLong successCount = new AtomicLong(0);
        AtomicLong failCount = new AtomicLong(0);

        long start = System.nanoTime();

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    Random random = new Random();
                    for (int i = 0; i < iterations; i++) {
                        if (controller.tryAcquire()) {
                            successCount.incrementAndGet();

                            StreamRecord<String> record = StreamRecord.<String>builder()
                                    .key("test")
                                    .value("value")
                                    .eventTime(System.currentTimeMillis())
                                    .processingTime(System.currentTimeMillis())
                                    .build();

                            controller.recordSample(record, random.nextInt(100));
                        } else {
                            failCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        long duration = System.nanoTime() - start;
        long totalOps = threadCount * iterations;
        double throughput = totalOps * 1_000_000_000.0 / duration;

        log.info("并发性能: 线程={}, 每线程操作={}, 总操作={}, 吞吐量={} ops/s",
                threadCount, iterations, totalOps, String.format("%.2f", throughput));
        log.info("成功: {}, 拒绝: {}", successCount.get(), failCount.get());

        assertTrue(throughput > 1000000, "并发吞吐量应超过100万ops/s");
    }

    @Test
    @DisplayName("内存使用测试")
    void testMemoryUsage() throws InterruptedException {
        log.info("=== 内存使用测试 ===");

        Runtime runtime = Runtime.getRuntime();

        // 记录初始内存
        System.gc();
        Thread.sleep(1000);
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();

        // 创建大量数据
        LSMTree<String, String> tree = new LSMTree<>();
        int count = 100000;

        for (int i = 0; i < count; i++) {
            tree.put("key" + i, "value" + UUID.randomUUID().toString());
        }

        long afterInsertMemory = runtime.totalMemory() - runtime.freeMemory();
        long memoryPerRecord = (afterInsertMemory - initialMemory) / count;

        log.info("内存使用: 初始={}, 插入后={}, 每条记录约={} bytes",
                initialMemory / 1024 / 1024,
                afterInsertMemory / 1024 / 1024,
                memoryPerRecord);

        assertTrue(memoryPerRecord < 1000, "每条记录内存占用应小于1KB");
    }

    @Test
    @DisplayName("端到端延迟测试")
    void testEndToEndLatency() throws InterruptedException {
        log.info("=== 端到端延迟测试 ===");

        int messageCount = 1000;
        List<Long> latencies = new ArrayList<>();

        BlockingQueue<String> queue = new LinkedBlockingQueue<>();

        // 消费者
        Thread consumer = new Thread(() -> {
            for (int i = 0; i < messageCount; i++) {
                try {
                    queue.take();
                    // 模拟处理
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        consumer.start();

        // 生产者
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < messageCount; i++) {
            long sendTime = System.currentTimeMillis();
            queue.offer("message" + i);
            latencies.add(sendTime);
        }

        consumer.join(30000);

        long totalTime = System.currentTimeMillis() - startTime;
        double avgLatency = totalTime * 1.0 / messageCount;

        log.info("端到端延迟: 总时间={}ms, 消息数={}, 平均延迟={}ms",
                totalTime, messageCount, String.format("%.2f", avgLatency));

        assertTrue(avgLatency < 20, "平均端到端延迟应小于20ms");
    }

    // ============== 辅助类 ==============

    private static class SumAggregateFunction implements com.kxj.streamingdataengine.aggregation.AggregateFunction<
            Double, Double, Double> {

        @Override
        public Double createAccumulator() {
            return 0.0;
        }

        @Override
        public void add(Double value, Double accumulator) {
            // 这里不做实际累加，仅模拟
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }
}
