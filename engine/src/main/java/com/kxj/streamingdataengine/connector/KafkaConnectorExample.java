package com.kxj.streamingdataengine.connector;

import com.kxj.streamingdataengine.connector.kafka.*;
import com.kxj.streamingdataengine.core.model.DataSink;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.stream.StreamBuilder;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;

/**
 * Kafka Connector 使用示例
 * 
 * 注意：运行这些示例需要启动 Kafka 服务
 * 可以使用 Docker 启动: docker run -d -p 9092:9092 --name kafka apache/kafka:latest
 */
public class KafkaConnectorExample {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "test-topic";
    private static final String GROUP_ID = "streaming-engine-group";

    /**
     * 示例1: 简单的 Kafka Source
     * 
     * 从 Kafka 读取字符串消息并打印
     */
    public static void example1_SimpleSource() {
        System.out.println("=== 示例1: 简单的 Kafka Source ===");

        KafkaSource.KafkaSourceConfig config = KafkaSource.KafkaSourceConfig.builder()
            .bootstrapServers(BOOTSTRAP_SERVERS)
            .topics(java.util.List.of(TOPIC))
            .groupId(GROUP_ID)
            .autoOffsetReset("earliest")
            .build();

        KafkaSource<String> source = new KafkaSource<>("example1-source", config, 
            new StringDeserializer());

        source.start();

        // 消费10条消息后停止
        int count = 0;
        while (count < 10) {
            try {
                java.util.List<StreamRecord<String>> records = source.poll(Duration.ofSeconds(1));
                for (StreamRecord<String> record : records) {
                    System.out.println("收到消息: " + record.value() + 
                                      " 来自分区: " + record.attributes().get("partition"));
                    count++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        source.stop();
        System.out.println("消费完成，共 " + count + " 条消息");
    }

    /**
     * 示例2: 简单的 Kafka Sink
     * 
     * 发送字符串消息到 Kafka
     */
    public static void example2_SimpleSink() {
        System.out.println("=== 示例2: 简单的 Kafka Sink ===");

        KafkaSink.KafkaSinkConfig config = KafkaSink.KafkaSinkConfig.builder()
            .bootstrapServers(BOOTSTRAP_SERVERS)
            .topic(TOPIC)
            .acks("all")
            .batchSize(1)  // 小批次，快速发送
            .lingerMs(0)
            .build();

        KafkaSink<String> sink = new KafkaSink<>("example2-sink", config, 
            new StringSerializer());

        sink.start();

        // 发送10条消息
        for (int i = 0; i < 10; i++) {
            String message = "Hello Kafka " + i + " at " + System.currentTimeMillis();
            sink.write("key-" + i, message);
            System.out.println("发送: " + message);
        }

        // 等待发送完成
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        sink.stop();
        System.out.println("发送完成，成功: " + sink.getSuccessCount() + 
                          ", 失败: " + sink.getFailCount());
    }

    /**
     * 示例3: 使用 StreamBuilder 创建 Kafka Source
     * 
     * 使用流式 API 处理 Kafka 数据
     */
    public static void example3_StreamBuilderSource() {
        System.out.println("=== 示例3: 使用 StreamBuilder 创建 Kafka Source ===");

        StreamBuilder builder = new StreamBuilder("kafka-stream-job");

        // 创建 Kafka Source 流
        KafkaSourceStream<String> stream = builder.kafkaTextStream(BOOTSTRAP_SERVERS, TOPIC, GROUP_ID);
        
        // 启动流
        stream.execute();
        
        System.out.println("Kafka 流已启动，正在消费...");
        System.out.println("注意：这是一个持续运行的流，需要手动停止");
    }

    /**
     * 示例4: Exactly-Once 模式
     * 
     * 使用事务保证 Exactly-Once 语义
     */
    public static void example4_ExactlyOnce() {
        System.out.println("=== 示例4: Exactly-Once 模式 ===");

        // Source 配置 - 读取已提交的消息
        KafkaSource.KafkaSourceConfig sourceConfig = KafkaSource.KafkaSourceConfig.builder()
            .bootstrapServers(BOOTSTRAP_SERVERS)
            .topics(java.util.List.of("input-topic"))
            .groupId(GROUP_ID)
            .exactlyOnce(true)
            .autoOffsetReset("earliest")
            .build();

        KafkaSource<String> source = new KafkaSource<>("eos-source", sourceConfig, 
            new StringDeserializer());

        // Sink 配置 - 使用事务
        KafkaSink.KafkaSinkConfig sinkConfig = KafkaSink.KafkaSinkConfig.builder()
            .bootstrapServers(BOOTSTRAP_SERVERS)
            .topic("output-topic")
            .exactlyOnce(true)
            .transactionalIdPrefix("my-app")
            .acks("all")
            .build();

        KafkaSink<String> sink = new KafkaSink<>("eos-sink", sinkConfig, 
            new StringSerializer());

        source.start();
        sink.start();

        // 模拟处理流程
        String transactionId = "txn-" + System.currentTimeMillis();
        sink.beginTransaction(transactionId);

        try {
            // 读取并处理消息
            java.util.List<StreamRecord<String>> records = source.poll(Duration.ofSeconds(5));
            for (StreamRecord<String> record : records) {
                // 处理消息
                String processed = processMessage(record.value());
                // 发送到输出 topic
                sink.write(record.key(), processed);
            }

            // 预提交
            sink.preCommit();
            
            // 提交 Source 的 offset（需要实现）
            // source.commitOffsets();
            
            // 提交事务
            sink.commitTransaction();
            
            System.out.println("事务 " + transactionId + " 提交成功");

        } catch (Exception e) {
            // 回滚事务
            sink.abortTransaction();
            System.err.println("事务 " + transactionId + " 回滚: " + e.getMessage());
        }

        source.stop();
        sink.stop();
    }

    /**
     * 示例5: 使用统一的连接器配置
     */
    public static void example5_UnifiedConfig() {
        System.out.println("=== 示例5: 使用统一的连接器配置 ===");

        KafkaConnectorConfig connectorConfig = KafkaConnectorConfig.builder()
            .bootstrapServers(BOOTSTRAP_SERVERS)
            .sourceTopic("input-topic")
            .sinkTopic("output-topic")
            .consumerGroupId(GROUP_ID)
            .exactlyOnce(true)
            .acks("all")
            .compressionType("lz4")
            .build();

        // 创建 Source
        KafkaSource<String> source = new KafkaSource<>("unified-source", 
            connectorConfig.toSourceConfig(), new StringDeserializer());

        // 创建 Sink
        KafkaSink<String> sink = new KafkaSink<>("unified-sink", 
            connectorConfig.toSinkConfig(), new StringSerializer());

        System.out.println("Source 和 Sink 配置完成");
        System.out.println("Source topics: " + connectorConfig.toSourceConfig().getTopics());
        System.out.println("Sink topic: " + connectorConfig.toSinkConfig().getTopic());
    }

    // 模拟消息处理
    private static String processMessage(String message) {
        return "[PROCESSED] " + message.toUpperCase();
    }

    public static void main(String[] args) {
        System.out.println("Kafka Connector 使用示例");
        System.out.println("=========================");
        System.out.println();
        System.out.println("这些示例展示了如何使用 Kafka Connector:");
        System.out.println("1. example1_SimpleSource() - 简单的 Kafka Source");
        System.out.println("2. example2_SimpleSink() - 简单的 Kafka Sink");
        System.out.println("3. example3_StreamBuilderSource() - 使用 StreamBuilder");
        System.out.println("4. example4_ExactlyOnce() - Exactly-Once 模式");
        System.out.println("5. example5_UnifiedConfig() - 统一配置");
        System.out.println();
        System.out.println("注意：运行这些示例需要 Kafka 服务运行在 " + BOOTSTRAP_SERVERS);
        System.out.println("可以使用 Docker 启动: docker run -d -p 9092:9092 --name kafka apache/kafka:latest");
    }
}
