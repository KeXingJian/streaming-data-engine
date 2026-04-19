package com.kxj.streamingdataengine.connector.kafka;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.checkpoint.CheckpointBarrier;
import com.kxj.streamingdataengine.checkpoint.Snapshotable;
import com.kxj.streamingdataengine.state.Snapshot;
import com.kxj.streamingdataengine.state.StateBackend;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka Source
 * 
 * 功能：
 * 1. 消费者组并行消费
 * 2. 自动/手动 offset 提交
 * 3. Checkpoint 支持（exactly-once）
 * 4. 自动重平衡处理
 */
@Slf4j
public class KafkaSource<T> implements Snapshotable {

    private final String operatorId;
    private final KafkaSourceConfig config;
    private final Deserializer<T> valueDeserializer;

    private KafkaConsumer<String, T> consumer;
    private ExecutorService consumerExecutor;
    private BlockingQueue<StreamRecord<T>> recordQueue;

    // 运行时状态
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    private volatile Map<TopicPartition, Long> currentOffsets = new ConcurrentHashMap<>();
    private volatile Map<TopicPartition, Long> committedOffsets = new ConcurrentHashMap<>();

    // Checkpoint 相关
    private volatile boolean checkpointInProgress = false;
    private volatile Map<TopicPartition, Long> checkpointOffsets = new ConcurrentHashMap<>();

    public KafkaSource(String operatorId, KafkaSourceConfig config, Deserializer<T> valueDeserializer) {
        this.operatorId = operatorId;
        this.config = config;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public String getOperatorId() {
        return operatorId;
    }

    /**
     * 启动 Kafka Source
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("[kxj: KafkaSource 启动] topics={}, groupId={}", config.getTopics(), config.getGroupId());
            
            this.recordQueue = new LinkedBlockingQueue<>(config.getQueueCapacity());
            this.consumer = createConsumer();
            this.consumerExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "kafka-consumer-" + operatorId);
                t.setDaemon(true);
                return t;
            });

            // 订阅主题
            consumer.subscribe(config.getTopics(), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    log.info("[kxj: 分区被回收] partitions={}", partitions);
                    // Checkpoint 模式下，回收分区前提交 offset
                    if (config.isExactlyOnce()) {
                        commitOffsets();
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.info("[kxj: 分区被分配] partitions={}", partitions);
                    // 恢复 offset
                    for (TopicPartition partition : partitions) {
                        Long offset = committedOffsets.get(partition);
                        if (offset != null) {
                            consumer.seek(partition, offset);
                            log.info("[kxj: 恢复 offset] partition={}, offset={}", partition, offset);
                        }
                    }
                }
            });

            // 启动消费线程
            consumerExecutor.submit(this::consumeLoop);
        }
    }

    /**
     * 停止 Kafka Source
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("[kxj: KafkaSource 停止]");
            
            if (consumer != null) {
                if (config.isExactlyOnce()) {
                    commitOffsets();
                }
                consumer.wakeup();
                consumer.close();
            }

            if (consumerExecutor != null) {
                consumerExecutor.shutdown();
                try {
                    if (!consumerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        consumerExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    consumerExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * 拉取数据（非阻塞）
     */
    public List<StreamRecord<T>> poll() {
        List<StreamRecord<T>> records = new ArrayList<>();
        recordQueue.drainTo(records, config.getMaxPollRecords());
        return records;
    }

    /**
     * 拉取数据（阻塞）
     */
    public List<StreamRecord<T>> poll(Duration timeout) throws InterruptedException {
        List<StreamRecord<T>> records = new ArrayList<>();
        StreamRecord<T> record = recordQueue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (record != null) {
            records.add(record);
            recordQueue.drainTo(records, config.getMaxPollRecords() - 1);
        }
        return records;
    }

    /**
     * 消费循环
     */
    private void consumeLoop() {
        while (running.get()) {
            try {
                ConsumerRecords<String, T> records = consumer.poll(config.getPollTimeout());
                
                for (ConsumerRecord<String, T> record : records) {
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    long offset = record.offset();
                    
                    // 更新当前 offset
                    currentOffsets.put(partition, offset + 1);
                    
                    // 创建 StreamRecord
                    StreamRecord<T> streamRecord = StreamRecord.<T>builder()
                        .key(record.key())
                        .value(record.value())
                        .eventTime(record.timestamp())
                        .partition(record.partition())
                        .sequenceNumber(sequenceNumber.incrementAndGet())
                        .attributes(Map.of(
                            "topic", record.topic(),
                            "offset", offset,
                            "partition", record.partition()
                        ))
                        .build();
                    
                    // 放入队列（阻塞等待空间）
                    while (!recordQueue.offer(streamRecord, 100, TimeUnit.MILLISECONDS)) {
                        if (!running.get()) {
                            return;
                        }
                    }
                }

                // 非 exactly-once 模式下自动提交
                if (!config.isExactlyOnce() && !records.isEmpty()) {
                    consumer.commitAsync((offsets, exception) -> {
                        if (exception != null) {
                            log.error("[kxj: Offset 提交失败]", exception);
                        }
                    });
                }

            } catch (WakeupException e) {
                // 正常停止
                break;
            } catch (Exception e) {
                log.error("[kxj: 消费异常]", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * 创建 Kafka Consumer
     */
    private KafkaConsumer<String, T> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getClientId() != null ? 
                  config.getClientId() : "streaming-engine-" + operatorId);
        
        // 反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                  org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                  config.getValueDeserializerClass());
        
        // 偏移量管理
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, !config.isExactlyOnce());
        
        // 性能调优
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getMaxPollRecords());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        
        // Exactly-once 配置
        if (config.isExactlyOnce()) {
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }

        // 自定义配置
        if (config.getProperties() != null) {
            props.putAll(config.getProperties());
        }

        return new KafkaConsumer<>(props);
    }

    /**
     * 提交 offset
     */
    private void commitOffsets() {
        if (currentOffsets.isEmpty()) {
            return;
        }

        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : currentOffsets.entrySet()) {
            offsetsToCommit.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
        }

        try {
            consumer.commitSync(offsetsToCommit);
            committedOffsets.putAll(currentOffsets);
            log.info("[kxj: Offset 提交成功] offsets={}", offsetsToCommit);
        } catch (Exception e) {
            log.error("[kxj: Offset 提交失败]", e);
        }
    }

    @Override
    public Snapshot snapshotState(String checkpointId, long checkpointNumber, StateBackend stateBackend) {
        log.info("[kxj: KafkaSource 快照] checkpointId={}, offsets={}", checkpointId, currentOffsets);
        
        // 保存当前 offset 到 checkpoint
        checkpointOffsets = new ConcurrentHashMap<>(currentOffsets);
        
        // 创建快照
        Map<String, byte[]> snapshotData = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : checkpointOffsets.entrySet()) {
            String key = entry.getKey().topic() + "-" + entry.getKey().partition();
            snapshotData.put(key, String.valueOf(entry.getValue()).getBytes());
        }
        
        return new Snapshot(checkpointId, snapshotData);
    }

    @Override
    public void restoreState(Snapshot snapshot) {
        log.info("[kxj: KafkaSource 恢复状态] checkpointId={}", snapshot.checkpointId());
        
        Map<TopicPartition, Long> restoredOffsets = new HashMap<>();
        for (Map.Entry<String, byte[]> entry : snapshot.stateData().entrySet()) {
            String[] parts = entry.getKey().split("-");
            if (parts.length == 2) {
                TopicPartition partition = new TopicPartition(parts[0], Integer.parseInt(parts[1]));
                long offset = Long.parseLong(new String(entry.getValue()));
                restoredOffsets.put(partition, offset);
            }
        }
        
        this.committedOffsets = restoredOffsets;
        log.info("[kxj: KafkaSource 恢复 offset] offsets={}", restoredOffsets);
    }

    @Override
    public void initializeState(StateBackend stateBackend) {
        // 初始化状态
    }

    // ===== 配置类 =====

    @Getter
    @Builder
    public static class KafkaSourceConfig {
        private final String bootstrapServers;
        private final List<String> topics;
        private final String groupId;
        private String clientId;
        
        @Builder.Default
        private String autoOffsetReset = "latest";
        
        @Builder.Default
        private boolean exactlyOnce = false;
        
        @Builder.Default
        private int maxPollRecords = 500;
        
        @Builder.Default
        private Duration pollTimeout = Duration.ofMillis(100);
        
        @Builder.Default
        private int queueCapacity = 10000;
        
        @Builder.Default
        private Class<?> valueDeserializerClass = 
            org.apache.kafka.common.serialization.StringDeserializer.class;
        
        private Properties properties;
    }
}
