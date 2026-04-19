package com.kxj.streamingdataengine.app;

import com.kxj.streamingdataengine.connector.kafka.*;
import com.kxj.streamingdataengine.core.model.DataSink;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.stream.StreamBuilder;
import com.kxj.streamingdataengine.window.WindowAssigner;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 实时数据处理应用示例
 * 
 * 场景：电商订单实时统计
 * - 从 Kafka 读取订单数据
 * - 按用户 ID 分组统计订单金额
 * - 5 秒窗口聚合
 * - 输出到 Kafka 结果主题
 * 
 * 演示如何将引擎封装为实际应用
 */
@Slf4j
public class RealTimeOrderAnalyticsApp {
    
    private final AppConfig config;
    private volatile boolean running = false;
    
    // 统计指标
    private final AtomicLong totalOrders = new AtomicLong(0);
    private final AtomicLong totalAmount = new AtomicLong(0);
    private final Map<String, AtomicLong> userOrderCounts = new ConcurrentHashMap<>();
    
    public RealTimeOrderAnalyticsApp(AppConfig config) {
        this.config = config;
    }
    
    /**
     * 启动应用
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        
        log.info("[kxj: 实时订单分析应用启动]");
        log.info("  输入 Topic: {}", config.getInputTopic());
        log.info("  输出 Topic: {}", config.getOutputTopic());
        log.info("  Kafka 集群: {}", config.getBootstrapServers());
        
        // 创建 StreamBuilder
        StreamBuilder builder = new StreamBuilder("order-analytics");
        
        // 构建流处理管道
        builder.kafkaTextStream(
                config.getBootstrapServers(),
                config.getInputTopic(),
                config.getConsumerGroupId()
            )
            .map(this::parseOrder)                    // 解析 JSON 订单
            .filter(this::validateOrder)              // 过滤无效订单
            .keyBy(Order::getUserId)                  // 按用户分组
            .window(WindowAssigner.tumblingTimeWindow(Duration.ofSeconds(5)))  // 5 秒窗口
            .aggregate(new OrderSumAggregate())       // 聚合订单金额
            .map(this::formatResult)                  // 格式化输出
            .addSink(createKafkaSink())               // 输出到 Kafka
            .execute();                               // 执行
    }
    
    /**
     * 停止应用
     */
    public void stop() {
        running = false;
        log.info("[kxj: 实时订单分析应用停止]");
        log.info("  总订单数: {}", totalOrders.get());
        log.info("  总交易额: {}", totalAmount.get());
    }
    
    /**
     * 获取统计信息
     */
    public Map<String, Object> getStats() {
        return Map.of(
            "totalOrders", totalOrders.get(),
            "totalAmount", totalAmount.get(),
            "userCount", userOrderCounts.size()
        );
    }
    
    // ===== 私有方法 =====
    
    private Order parseOrder(String json) {
        // 简化：实际应该使用 Jackson 解析 JSON
        // {"userId": "user-123", "orderId": "order-456", "amount": 100.0}
        try {
            String[] parts = json.replace("{", "").replace("}", "").replace("\"", "").split(",");
            String userId = parts[0].split(":")[1].trim();
            String orderId = parts[1].split(":")[1].trim();
            double amount = Double.parseDouble(parts[2].split(":")[1].trim());
            
            totalOrders.incrementAndGet();
            totalAmount.addAndGet((long) amount);
            userOrderCounts.computeIfAbsent(userId, k -> new AtomicLong(0)).incrementAndGet();
            
            return new Order(userId, orderId, amount, System.currentTimeMillis());
        } catch (Exception e) {
            log.error("[kxj: 订单解析失败] json={}", json, e);
            return null;
        }
    }
    
    private boolean validateOrder(Order order) {
        return order != null && order.getAmount() > 0 && order.getUserId() != null;
    }
    
    private String formatResult(OrderSumResult result) {
        return String.format(
            "{\"window\":\"%s\",\"userId\":\"%s\",\"orderCount\":%d,\"totalAmount\":%.2f}",
            result.getWindowEnd(),
            result.getUserId(),
            result.getOrderCount(),
            result.getTotalAmount()
        );
    }
    
    private DataSink<String> createKafkaSink() {
        KafkaSink.KafkaSinkConfig sinkConfig = KafkaSink.KafkaSinkConfig.builder()
            .bootstrapServers(config.getBootstrapServers())
            .topic(config.getOutputTopic())
            .acks("all")
            .build();
        
        KafkaSink<String> kafkaSink = new KafkaSink<>("order-result-sink", sinkConfig, 
            new StringSerializer());
        
        // 包装为 DataSink
        return new DataSink<String>() {
            @Override
            public void write(String value) throws Exception {
                kafkaSink.write(null, value);
            }
            
            @Override
            public void open() {
                kafkaSink.start();
            }
            
            @Override
            public void close() {
                kafkaSink.stop();
            }
        };
    }
    
    // ===== 数据模型 =====
    
    @Getter
    @Builder
    static class Order {
        private final String userId;
        private final String orderId;
        private final double amount;
        private final long timestamp;
    }
    
    @Getter
    @Builder
    static class OrderSumResult {
        private final String userId;
        private final long windowEnd;
        private final int orderCount;
        private final double totalAmount;
    }
    
    /**
     * 订单聚合函数
     */
    static class OrderSumAggregate 
        implements com.kxj.streamingdataengine.aggregation.AggregateFunction<Order, OrderSumAccumulator, OrderSumResult> {
        
        @Override
        public OrderSumAccumulator createAccumulator() {
            return new OrderSumAccumulator();
        }
        
        @Override
        public void add(Order value, OrderSumAccumulator accumulator) {
            if (accumulator.userId == null) {
                accumulator.userId = value.getUserId();
            }
            accumulator.count++;
            accumulator.sum += value.getAmount();
        }
        
        @Override
        public OrderSumResult getResult(OrderSumAccumulator accumulator) {
            return OrderSumResult.builder()
                .userId(accumulator.userId)
                .windowEnd(System.currentTimeMillis())
                .orderCount(accumulator.count)
                .totalAmount(accumulator.sum)
                .build();
        }
        
        @Override
        public OrderSumAccumulator merge(OrderSumAccumulator a, OrderSumAccumulator b) {
            OrderSumAccumulator result = new OrderSumAccumulator();
            result.userId = a.userId != null ? a.userId : b.userId;
            result.count = a.count + b.count;
            result.sum = a.sum + b.sum;
            return result;
        }
    }
    
    /**
     * 订单求和累加器
     */
    static class OrderSumAccumulator {
        int count = 0;
        double sum = 0;
        String userId = null;
    }
    
    // ===== 应用配置 =====
    
    @Getter
    @Builder
    public static class AppConfig {
        private final String bootstrapServers;
        private final String inputTopic;
        private final String outputTopic;
        
        @Builder.Default
        private String consumerGroupId = "order-analytics-group";
        
        @Builder.Default
        private int windowSeconds = 5;
    }
    
    // ===== 主入口 =====
    
    public static void main(String[] args) {
        // 配置
        AppConfig config = AppConfig.builder()
            .bootstrapServers("localhost:9092")
            .inputTopic("orders")
            .outputTopic("order-stats")
            .build();
        
        // 创建并启动应用
        RealTimeOrderAnalyticsApp app = new RealTimeOrderAnalyticsApp(config);
        
        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("收到关闭信号，正在停止应用...");
            app.stop();
            System.out.println("应用已停止，统计: " + app.getStats());
        }));
        
        // 启动
        app.start();
    }
}
