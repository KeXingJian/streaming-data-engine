package com.kxj.streamingdataengine.connector.kafka;

import lombok.Builder;
import lombok.Getter;

import java.util.Properties;

/**
 * Kafka 连接器配置
 * 统一管理 Source 和 Sink 的配置
 */
@Getter
@Builder
public class KafkaConnectorConfig {

    // ===== 通用配置 =====
    
    /**
     * Kafka 集群地址
     */
    private final String bootstrapServers;
    
    /**
     * 是否启用 Exactly-Once 语义
     */
    @Builder.Default
    private boolean exactlyOnce = false;
    
    // ===== Source 配置 =====
    
    /**
     * 消费主题（Source）
     */
    private String sourceTopic;
    
    /**
     * 消费者组 ID
     */
    private String consumerGroupId;
    
    /**
     * 消费者客户端 ID
     */
    private String consumerClientId;
    
    /**
     * 自动偏移量重置策略
     */
    @Builder.Default
    private String autoOffsetReset = "latest";
    
    /**
     * 最大拉取记录数
     */
    @Builder.Default
    private int maxPollRecords = 500;
    
    // ===== Sink 配置 =====
    
    /**
     * 生产主题（Sink）
     */
    private String sinkTopic;
    
    /**
     * 生产者客户端 ID
     */
    private String producerClientId;
    
    /**
     * 生产者 ACK 配置
     */
    @Builder.Default
    private String acks = "1";
    
    /**
     * 批次大小（KB）
     */
    @Builder.Default
    private int batchSize = 16;
    
    /**
     * 发送延迟（毫秒）
     */
    @Builder.Default
    private int lingerMs = 5;
    
    /**
     * 压缩类型
     */
    @Builder.Default
    private String compressionType = "lz4";
    
    /**
     * 事务 ID 前缀
     */
    @Builder.Default
    private String transactionalIdPrefix = "streaming-engine";
    
    // ===== 自定义属性 =====
    
    /**
     * 额外的 Kafka 属性
     */
    private Properties properties;
    
    /**
     * 创建 Source 配置
     */
    public KafkaSource.KafkaSourceConfig toSourceConfig() {
        return KafkaSource.KafkaSourceConfig.builder()
            .bootstrapServers(bootstrapServers)
            .topics(java.util.List.of(sourceTopic))
            .groupId(consumerGroupId)
            .clientId(consumerClientId)
            .autoOffsetReset(autoOffsetReset)
            .exactlyOnce(exactlyOnce)
            .maxPollRecords(maxPollRecords)
            .properties(properties)
            .build();
    }
    
    /**
     * 创建 Sink 配置
     */
    public KafkaSink.KafkaSinkConfig toSinkConfig() {
        return KafkaSink.KafkaSinkConfig.builder()
            .bootstrapServers(bootstrapServers)
            .topic(sinkTopic)
            .clientId(producerClientId)
            .acks(acks)
            .batchSize(batchSize)
            .lingerMs(lingerMs)
            .compressionType(compressionType)
            .exactlyOnce(exactlyOnce)
            .transactionalIdPrefix(transactionalIdPrefix)
            .properties(properties)
            .build();
    }
    
    /**
     * 快速创建配置
     */
    public static KafkaConnectorConfig create(String bootstrapServers, String topic, String groupId) {
        return KafkaConnectorConfig.builder()
            .bootstrapServers(bootstrapServers)
            .sourceTopic(topic)
            .sinkTopic(topic)
            .consumerGroupId(groupId)
            .build();
    }
}
