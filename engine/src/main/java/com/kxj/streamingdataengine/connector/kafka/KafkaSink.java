package com.kxj.streamingdataengine.connector.kafka;

import com.kxj.streamingdataengine.checkpoint.Snapshotable;
import com.kxj.streamingdataengine.state.Snapshot;
import com.kxj.streamingdataengine.state.StateBackend;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka Sink
 * 
 * 功能：
 * 1. 异步批量写入
 * 2. 两阶段提交（支持 exactly-once）
 * 3. 事务支持
 * 4. 失败重试
 */
@Slf4j
public class KafkaSink<T> implements Snapshotable {

    private final String operatorId;
    private final KafkaSinkConfig config;
    private final Serializer<T> valueSerializer;

    private KafkaProducer<String, T> producer;
    private ExecutorService sendExecutor;
    private BlockingQueue<PendingRecord<T>> pendingQueue;

    // 运行时状态
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    private final AtomicLong pendingCount = new AtomicLong(0);
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failCount = new AtomicLong(0);

    // 事务相关
    private volatile String currentTransactionId;
    private volatile boolean inTransaction = false;

    public KafkaSink(String operatorId, KafkaSinkConfig config, Serializer<T> valueSerializer) {
        this.operatorId = operatorId;
        this.config = config;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public String getOperatorId() {
        return operatorId;
    }

    /**
     * 启动 Kafka Sink
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("[kxj: KafkaSink 启动] topic={}, bootstrapServers={}", 
                    config.getTopic(), config.getBootstrapServers());
            
            this.pendingQueue = new LinkedBlockingQueue<>(config.getQueueCapacity());
            this.producer = createProducer();
            this.sendExecutor = Executors.newFixedThreadPool(
                config.getSendParallelism(),
                r -> {
                    Thread t = new Thread(r, "kafka-sender-" + operatorId);
                    t.setDaemon(true);
                    return t;
                }
            );

            // 启动发送线程
            for (int i = 0; i < config.getSendParallelism(); i++) {
                sendExecutor.submit(this::sendLoop);
            }
        }
    }

    /**
     * 停止 Kafka Sink
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("[kxj: KafkaSink 停止]");
            
            // 等待 pending 消息处理完成
            waitForPending(Duration.ofSeconds(10));
            
            if (producer != null) {
                producer.close();
            }

            if (sendExecutor != null) {
                sendExecutor.shutdown();
                try {
                    if (!sendExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        sendExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    sendExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * 写入单条记录
     */
    public void write(String key, T value) {
        if (!running.get()) {
            throw new IllegalStateException("KafkaSink is not running");
        }

        PendingRecord<T> record = new PendingRecord<>(
            sequenceNumber.incrementAndGet(),
            key,
            value,
            System.currentTimeMillis()
        );

        try {
            while (!pendingQueue.offer(record, 100, TimeUnit.MILLISECONDS)) {
                if (!running.get()) {
                    throw new IllegalStateException("KafkaSink is stopped");
                }
            }
            pendingCount.incrementAndGet();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while adding record to queue", e);
        }
    }

    /**
     * 批量写入
     */
    public void writeBatch(List<Map.Entry<String, T>> records) {
        for (Map.Entry<String, T> entry : records) {
            write(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 发送循环
     */
    private void sendLoop() {
        List<PendingRecord<T>> batch = new ArrayList<>(config.getBatchSize());
        
        while (running.get() || !pendingQueue.isEmpty()) {
            try {
                // 收集批次
                PendingRecord<T> record = pendingQueue.poll(100, TimeUnit.MILLISECONDS);
                if (record != null) {
                    batch.add(record);
                    pendingQueue.drainTo(batch, config.getBatchSize() - 1);
                }

                // 发送批次
                if (!batch.isEmpty()) {
                    sendBatch(batch);
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("[kxj: 发送异常]", e);
            }
        }

        // 发送剩余数据
        if (!batch.isEmpty()) {
            try {
                sendBatch(batch);
            } catch (Exception e) {
                log.error("[kxj: 发送剩余数据失败]", e);
            }
        }
    }

    /**
     * 发送批次
     */
    private void sendBatch(List<PendingRecord<T>> batch) {
        CountDownLatch latch = new CountDownLatch(batch.size());
        
        for (PendingRecord<T> record : batch) {
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(
                config.getTopic(),
                record.getKey(),
                record.getValue()
            );

            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("[kxj: 发送失败] key={}", record.getKey(), exception);
                    failCount.incrementAndGet();
                    
                    // 重试逻辑
                    if (record.getRetryCount() < config.getMaxRetries()) {
                        record.incrementRetryCount();
                        try {
                            pendingQueue.put(record);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                } else {
                    successCount.incrementAndGet();
                }
                pendingCount.decrementAndGet();
                latch.countDown();
            });
        }

        // 等待批次完成
        try {
            if (!latch.await(config.getRequestTimeoutMs(), TimeUnit.MILLISECONDS)) {
                log.warn("[kxj: 发送批次超时] batchSize={}", batch.size());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 创建 Kafka Producer
     */
    private KafkaProducer<String, T> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, 
                  config.getClientId() != null ? config.getClientId() : "streaming-sink-" + operatorId);
        
        // 序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                  org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                  config.getValueSerializerClass());
        
        // 性能调优
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getBatchSize() * 1024); // KB to bytes
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.getLingerMs());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024); // 32MB
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getCompressionType());
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10 * 1024 * 1024); // 10MB
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.getRequestTimeoutMs());
        
        // 重试配置
        props.put(ProducerConfig.RETRIES_CONFIG, config.getMaxRetries());
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        // ACK 配置
        props.put(ProducerConfig.ACKS_CONFIG, config.getAcks());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, config.isIdempotence());

        // Exactly-once 事务配置
        if (config.isExactlyOnce()) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, 
                      config.getTransactionalIdPrefix() + "-" + operatorId);
            props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000);
        }

        // 自定义配置
        if (config.getProperties() != null) {
            props.putAll(config.getProperties());
        }

        KafkaProducer<String, T> kafkaProducer = new KafkaProducer<>(props);
        
        // 初始化事务
        if (config.isExactlyOnce()) {
            kafkaProducer.initTransactions();
        }
        
        return kafkaProducer;
    }

    /**
     * 等待 pending 消息处理完成
     */
    private void waitForPending(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (pendingCount.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (pendingCount.get() > 0) {
            log.warn("[kxj: 仍有未完成的 pending 消息] count={}", pendingCount.get());
        }
    }

    /**
     * 开始事务（两阶段提交第一阶段）
     */
    public void beginTransaction(String transactionId) {
        if (!config.isExactlyOnce()) {
            return;
        }
        
        this.currentTransactionId = transactionId;
        this.inTransaction = true;
        producer.beginTransaction();
        log.debug("[kxj: 开始事务] transactionId={}", transactionId);
    }

    /**
     * 预提交（两阶段提交第二阶段 - prepare）
     */
    public void preCommit() {
        if (!config.isExactlyOnce() || !inTransaction) {
            return;
        }
        
        producer.flush();
        log.debug("[kxj: 预提交] transactionId={}", currentTransactionId);
    }

    /**
     * 提交事务（两阶段提交第三阶段 - commit）
     */
    public void commitTransaction() {
        if (!config.isExactlyOnce() || !inTransaction) {
            return;
        }
        
        try {
            producer.commitTransaction();
            log.info("[kxj: 提交事务成功] transactionId={}", currentTransactionId);
        } catch (Exception e) {
            log.error("[kxj: 提交事务失败] transactionId={}", currentTransactionId, e);
            throw e;
        } finally {
            inTransaction = false;
            currentTransactionId = null;
        }
    }

    /**
     * 回滚事务
     */
    public void abortTransaction() {
        if (!config.isExactlyOnce() || !inTransaction) {
            return;
        }
        
        try {
            producer.abortTransaction();
            log.warn("[kxj: 回滚事务] transactionId={}", currentTransactionId);
        } catch (Exception e) {
            log.error("[kxj: 回滚事务异常] transactionId={}", currentTransactionId, e);
        } finally {
            inTransaction = false;
            currentTransactionId = null;
        }
    }

    @Override
    public Snapshot snapshotState(String checkpointId, long checkpointNumber, StateBackend stateBackend) {
        log.info("[kxj: KafkaSink 快照] checkpointId={}", checkpointId);
        
        // 预提交当前事务
        preCommit();
        
        return new Snapshot(checkpointId, Map.of(
            "transactionId", currentTransactionId != null ? currentTransactionId.getBytes() : new byte[0],
            "pendingCount", String.valueOf(pendingCount.get()).getBytes(),
            "successCount", String.valueOf(successCount.get()).getBytes(),
            "failCount", String.valueOf(failCount.get()).getBytes()
        ));
    }

    @Override
    public void restoreState(Snapshot snapshot) {
        log.info("[kxj: KafkaSink 恢复状态] checkpointId={}", snapshot.checkpointId());
        
        // 如果存在未完成的事务，回滚它
        if (inTransaction) {
            abortTransaction();
        }
    }

    @Override
    public void initializeState(StateBackend stateBackend) {
        // 初始化状态
    }

    public long getPendingCount() {
        return pendingCount.get();
    }

    public long getSuccessCount() {
        return successCount.get();
    }

    public long getFailCount() {
        return failCount.get();
    }

    // ===== 内部类 =====

    @Getter
    private static class PendingRecord<T> {
        private final long sequenceNumber;
        private final String key;
        private final T value;
        private final long timestamp;
        private int retryCount = 0;

        PendingRecord(long sequenceNumber, String key, T value, long timestamp) {
            this.sequenceNumber = sequenceNumber;
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }

        void incrementRetryCount() {
            retryCount++;
        }
    }

    // ===== 配置类 =====

    @Getter
    @Builder
    public static class KafkaSinkConfig {
        private final String bootstrapServers;
        private final String topic;
        private String clientId;
        
        @Builder.Default
        private int batchSize = 16; // KB
        
        @Builder.Default
        private int lingerMs = 5;
        
        @Builder.Default
        private String compressionType = "lz4";
        
        @Builder.Default
        private String acks = "1";
        
        @Builder.Default
        private boolean idempotence = true;
        
        @Builder.Default
        private boolean exactlyOnce = false;
        
        @Builder.Default
        private String transactionalIdPrefix = "streaming-engine";
        
        @Builder.Default
        private int maxRetries = 3;
        
        @Builder.Default
        private int requestTimeoutMs = 30000;
        
        @Builder.Default
        private int queueCapacity = 10000;
        
        @Builder.Default
        private int sendParallelism = 2;
        
        @Builder.Default
        private Class<?> valueSerializerClass = 
            org.apache.kafka.common.serialization.StringSerializer.class;
        
        private Properties properties;
    }
}
