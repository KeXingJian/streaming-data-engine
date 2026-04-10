package com.kxj.streamingdataengine.scenario;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.ai.AnomalyDetector;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 电商实时大屏场景测试
 * 模拟双11大促等高峰流量场景
 */
@Slf4j
public class ECommerceScenarioTest {

    record OrderEvent(String orderId, String userId, double amount,
                      String category, String city, long timestamp) {}

    @Test
    @DisplayName("实时GMV统计")
    void testRealTimeGMV() {
        log.info("=== 场景1: 实时GMV统计 ===");

        // 生成订单数据
        List<OrderEvent> orders = generateOrders(10000, 100);
        log.info("生成 {} 个订单", orders.size());

        // 计算总GMV
        DoubleAdder gmv = new DoubleAdder();
        LongAdder orderCount = new LongAdder();

        StreamBuilder builder = new StreamBuilder("gmv-stats");
        builder.fromCollection(orders)
                .map(o -> {
                    gmv.add(o.amount());
                    orderCount.increment();
                    return String.format("订单: %s, 金额: %.2f", o.orderId(), o.amount());
                })
                .addSink(new CollectSink<>())
                .execute();

        double calculatedGmv = orders.stream().mapToDouble(OrderEvent::amount).sum();

        log.info("GMV统计结果:");
        log.info("  - 订单数: {}", orderCount.sum());
        log.info("  - 总GMV: {}", String.format("%.2f", gmv.sum()));
        log.info("  - 客单价: {}", String.format("%.2f", gmv.sum() / orderCount.sum()));

        assertEquals(calculatedGmv, gmv.sum(), 0.01);
    }

    @Test
    @DisplayName("品类销售排行")
    void testCategoryRanking() {
        log.info("=== 场景2: 品类销售排行 ===");

        List<OrderEvent> orders = generateOrders(5000, 50);

        // 按品类聚合
        Map<String, Double> categorySales = new ConcurrentHashMap<>();

        for (OrderEvent order : orders) {
            categorySales.merge(order.category(), order.amount(), Double::sum);
        }

        // 排序输出
        List<Map.Entry<String, Double>> ranking = categorySales.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .toList();

        log.info("品类销售TOP5:");
        ranking.stream().limit(5).forEach(entry ->
                log.info("  {}: {}", entry.getKey(), String.format("%.2f", entry.getValue())));

        assertFalse(ranking.isEmpty());
    }

    @Test
    @DisplayName("大促流量洪峰模拟")
    void testFlashSaleTraffic() throws InterruptedException {
        log.info("=== 场景3: 大促流量洪峰模拟 ===");

        ExecutionEngine engine = new ExecutionEngine(8, Duration.ofMillis(50), true, true);
        engine.start();

        // 模拟流量洪峰
        int normalQPS = 100;
        int spikeQPS = 5000;
        int durationSeconds = 5;

        AtomicLong totalProcessed = new AtomicLong(0);
        AtomicLong totalDropped = new AtomicLong(0);

        BlockingQueue<OrderEvent> queue = new LinkedBlockingQueue<>(50000);

        // 模拟流量洪峰产生
        ExecutorService producer = Executors.newSingleThreadExecutor();
        producer.submit(() -> {
            Random random = new Random();
            long startTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - startTime < durationSeconds * 1000L) {
                int qps = (System.currentTimeMillis() - startTime < 2000) ? normalQPS : spikeQPS;
                int batchSize = qps / 10; // 每100ms产生一批

                for (int i = 0; i < batchSize; i++) {
                    OrderEvent order = new OrderEvent(
                            UUID.randomUUID().toString(),
                            "user-" + random.nextInt(10000),
                            100 + random.nextDouble() * 900,
                            random.nextBoolean() ? "electronics" : "clothing",
                            "city-" + random.nextInt(100),
                            System.currentTimeMillis()
                    );

                    if (!queue.offer(order)) {
                        totalDropped.incrementAndGet();
                    }
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        // 消费者处理
        ExecutorService consumer = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 4; i++) {
            consumer.submit(() -> {
                while (!Thread.interrupted()) {
                    try {
                        OrderEvent order = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (order != null) {
                            // 模拟处理延迟
                            Thread.sleep(randomDelay());
                            totalProcessed.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
        }

        Thread.sleep((durationSeconds + 2) * 1000L);

        producer.shutdown();
        consumer.shutdownNow();

        engine.stop();

        long total = totalProcessed.get() + totalDropped.get();
        double dropRate = totalDropped.get() * 100.0 / total;

        log.info("洪峰测试结果:");
        log.info("  - 总请求: {}", total);
        log.info("  - 成功处理: {}", totalProcessed.get());
        log.info("  - 丢弃: {}", totalDropped.get());
        log.info("  - 丢弃率: {}", String.format("%.2f%%", dropRate));

        assertTrue(totalProcessed.get() > 0, "应该有处理成功的请求");
    }

    @Test
    @DisplayName("交易异常检测")
    void testTransactionAnomalyDetection() throws InterruptedException {
        log.info("=== 场景4: 交易异常检测 ===");

        List<AnomalyDetector.AnomalyResult> alerts = Collections.synchronizedList(new ArrayList<>());
        AnomalyDetector detector = new AnomalyDetector(
                AnomalyDetector.DetectionConfig.sensitiveConfig(),
                alerts::add
        );

        Random random = new Random();

        // 正常交易时段
        log.info("模拟正常交易时段...");
        for (int i = 0; i < 100; i++) {
            detector.recordSample(50 + random.nextGaussian() * 10); // 50单/秒
            Thread.sleep(10);
        }

        // 秒杀开始 - 流量激增
        log.info("模拟秒杀开始 - 流量激增...");
        for (int i = 0; i < 30; i++) {
            detector.recordSample(2000 + random.nextGaussian() * 200); // 2000单/秒
            Thread.sleep(10);
        }

        // 秒杀结束 - 流量骤降
        log.info("模拟秒杀结束 - 流量骤降...");
        for (int i = 0; i < 30; i++) {
            detector.recordSample(30 + random.nextGaussian() * 5);
            Thread.sleep(10);
        }

        log.info("检测到 {} 次异常", alerts.size());

        alerts.forEach(alert -> log.info("告警: level={}, value={}, 变化率={}",
                alert.getLevel(),
                String.format("%.0f", alert.getCurrentValue()),
                String.format("%.0f%%", alert.getChangeRate() * 100)));

        assertFalse(alerts.isEmpty(), "应该检测到流量异常");
    }

    @Test
    @DisplayName("地域订单分布热力图")
    void testRegionalOrderDistribution() {
        log.info("=== 场景5: 地域订单分布热力图 ===");

        List<OrderEvent> orders = generateOrders(10000, 100);

        // 按城市聚合
        Map<String, LongAdder> cityOrders = new ConcurrentHashMap<>();
        Map<String, DoubleAdder> cityGMV = new ConcurrentHashMap<>();

        for (OrderEvent order : orders) {
            cityOrders.computeIfAbsent(order.city(), k -> new LongAdder()).increment();
            cityGMV.computeIfAbsent(order.city(), k -> new DoubleAdder()).add(order.amount());
        }

        // 找出TOP10城市
        List<Map.Entry<String, LongAdder>> topCities = cityOrders.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().sum(), a.getValue().sum()))
                .limit(10)
                .toList();

        log.info("TOP10 订单量城市:");
        topCities.forEach(entry -> {
            String city = entry.getKey();
            long orderCount = entry.getValue().sum();
            double gmv = cityGMV.get(city).sum();
            log.info("  {}: {}单, GMV={}", city, orderCount, String.format("%.2f", gmv));
        });

        assertFalse(topCities.isEmpty());
    }

    // ============== 辅助方法 ==============

    private List<OrderEvent> generateOrders(int count, long timeSpanSeconds) {
        List<OrderEvent> orders = new ArrayList<>();
        Random random = new Random();
        String[] categories = {"electronics", "clothing", "food", "home", "beauty"};
        long baseTime = System.currentTimeMillis() - timeSpanSeconds * 1000;

        for (int i = 0; i < count; i++) {
            orders.add(new OrderEvent(
                    UUID.randomUUID().toString(),
                    "user-" + random.nextInt(100000),
                    50 + random.nextDouble() * 950,
                    categories[random.nextInt(categories.length)],
                    "city-" + random.nextInt(300),
                    baseTime + (long) (random.nextDouble() * timeSpanSeconds * 1000)
            ));
        }

        return orders;
    }

    private long randomDelay() {
        // 10%概率产生慢请求
        return Math.random() < 0.1 ? 50 + (long) (Math.random() * 100) : 5;
    }
}
