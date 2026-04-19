package com.kxj.streamingdataengine.connector.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kafka Connector 配置测试
 * 
 * 注意：这些测试验证配置创建和结构，不依赖真实 Kafka 服务
 * 完整集成测试需要 Kafka Testcontainers 或真实集群
 */
@DisplayName("Kafka Connector 配置测试")
class KafkaConnectorConfigTest {

    @Test
    @DisplayName("测试创建 Source 配置")
    void testCreateSourceConfig() {
        KafkaConnectorConfig connectorConfig = KafkaConnectorConfig.builder()
            .bootstrapServers("localhost:9092")
            .sourceTopic("test-input")
            .consumerGroupId("test-group")
            .consumerClientId("test-client")
            .autoOffsetReset("earliest")
            .exactlyOnce(true)
            .maxPollRecords(1000)
            .build();

        KafkaSource.KafkaSourceConfig sourceConfig = connectorConfig.toSourceConfig();

        assertEquals("localhost:9092", sourceConfig.getBootstrapServers());
        assertEquals(1, sourceConfig.getTopics().size());
        assertEquals("test-input", sourceConfig.getTopics().get(0));
        assertEquals("test-group", sourceConfig.getGroupId());
        assertEquals("test-client", sourceConfig.getClientId());
        assertEquals("earliest", sourceConfig.getAutoOffsetReset());
        assertTrue(sourceConfig.isExactlyOnce());
        assertEquals(1000, sourceConfig.getMaxPollRecords());
    }

    @Test
    @DisplayName("测试创建 Sink 配置")
    void testCreateSinkConfig() {
        KafkaConnectorConfig connectorConfig = KafkaConnectorConfig.builder()
            .bootstrapServers("localhost:9092")
            .sinkTopic("test-output")
            .producerClientId("test-producer")
            .acks("all")
            .batchSize(32)
            .lingerMs(10)
            .compressionType("snappy")
            .exactlyOnce(true)
            .transactionalIdPrefix("my-app")
            .build();

        KafkaSink.KafkaSinkConfig sinkConfig = connectorConfig.toSinkConfig();

        assertEquals("localhost:9092", sinkConfig.getBootstrapServers());
        assertEquals("test-output", sinkConfig.getTopic());
        assertEquals("test-producer", sinkConfig.getClientId());
        assertEquals("all", sinkConfig.getAcks());
        assertEquals(32, sinkConfig.getBatchSize());
        assertEquals(10, sinkConfig.getLingerMs());
        assertEquals("snappy", sinkConfig.getCompressionType());
        assertTrue(sinkConfig.isExactlyOnce());
        assertEquals("my-app", sinkConfig.getTransactionalIdPrefix());
    }

    @Test
    @DisplayName("测试快速创建配置")
    void testQuickCreate() {
        KafkaConnectorConfig config = KafkaConnectorConfig.create(
            "kafka:9092",
            "my-topic",
            "my-group"
        );

        assertEquals("kafka:9092", config.getBootstrapServers());
        assertEquals("my-topic", config.getSourceTopic());
        assertEquals("my-topic", config.getSinkTopic());
        assertEquals("my-group", config.getConsumerGroupId());
    }

    @Test
    @DisplayName("测试 Source 配置默认值")
    void testSourceConfigDefaults() {
        KafkaSource.KafkaSourceConfig config = KafkaSource.KafkaSourceConfig.builder()
            .bootstrapServers("localhost:9092")
            .topics(java.util.List.of("test"))
            .groupId("group")
            .build();

        assertEquals("latest", config.getAutoOffsetReset());
        assertFalse(config.isExactlyOnce());
        assertEquals(500, config.getMaxPollRecords());
        assertEquals(Duration.ofMillis(100), config.getPollTimeout());
        assertEquals(10000, config.getQueueCapacity());
        assertEquals(StringDeserializer.class, config.getValueDeserializerClass());
    }

    @Test
    @DisplayName("测试 Sink 配置默认值")
    void testSinkConfigDefaults() {
        KafkaSink.KafkaSinkConfig config = KafkaSink.KafkaSinkConfig.builder()
            .bootstrapServers("localhost:9092")
            .topic("test")
            .build();

        assertEquals(16, config.getBatchSize());
        assertEquals(5, config.getLingerMs());
        assertEquals("lz4", config.getCompressionType());
        assertEquals("1", config.getAcks());
        assertTrue(config.isIdempotence());
        assertFalse(config.isExactlyOnce());
        assertEquals("streaming-engine", config.getTransactionalIdPrefix());
        assertEquals(3, config.getMaxRetries());
        assertEquals(30000, config.getRequestTimeoutMs());
        assertEquals(10000, config.getQueueCapacity());
        assertEquals(2, config.getSendParallelism());
        assertEquals(StringSerializer.class, config.getValueSerializerClass());
    }

    @Test
    @DisplayName("测试自定义属性传递")
    void testCustomProperties() {
        Properties customProps = new Properties();
        customProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");
        customProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");

        KafkaConnectorConfig connectorConfig = KafkaConnectorConfig.builder()
            .bootstrapServers("localhost:9092")
            .sourceTopic("input")
            .sinkTopic("output")
            .consumerGroupId("group")
            .properties(customProps)
            .build();

        assertNotNull(connectorConfig.getProperties());
        assertEquals("600000", connectorConfig.getProperties().get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals("67108864", connectorConfig.getProperties().get(ProducerConfig.BUFFER_MEMORY_CONFIG));
    }
}
