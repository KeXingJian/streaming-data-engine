package com.kxj.streamingdataengine.connector.kafka;

import com.kxj.streamingdataengine.core.model.DataSink;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.execution.ExecutionEngine;
import com.kxj.streamingdataengine.stream.DataStreamImpl;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Kafka Source 流适配器
 * 将 KafkaSource 包装为 DataStream，方便流处理
 */
@Slf4j
public class KafkaSourceStream<T> extends DataStreamImpl<T> {

    private final String jobName;
    @Getter
    private final KafkaSource<T> kafkaSource;
    private final ExecutorService pollExecutor;
    private volatile boolean running = false;

    public KafkaSourceStream(String jobName, KafkaSource<T> kafkaSource) {
        super(jobName, null, null);
        this.jobName = jobName;
        this.kafkaSource = kafkaSource;
        this.pollExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "kafka-source-poll-" + jobName);
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void execute() {
        if (running) {
            return;
        }
        
        running = true;
        kafkaSource.start();
        
        log.info("[kxj: KafkaSourceStream 启动] jobName={}", jobName);
        
        // 启动拉取线程
        pollExecutor.submit(this::pollLoop);
    }

    /**
     * 拉取循环
     */
    private void pollLoop() {
        ExecutionEngine engine = new ExecutionEngine(
            Runtime.getRuntime().availableProcessors(),
            Duration.ofMillis(200),
            false,
            false
        );

        while (running) {
            try {
                List<StreamRecord<T>> records = kafkaSource.poll(Duration.ofMillis(100));
                
                for (StreamRecord<T> record : records) {
                    // 使用父类的算子链处理记录
                    List<StreamRecord<T>> results = new ArrayList<>();
                    results.add(record);
                    
                    // 这里简化处理，实际应该调用完整的算子链
                    // 完整实现需要重构以支持增量执行
                    
                    // 输出到 Sinks
                    for (DataSink<T> sink : sinks) {
                        for (StreamRecord<T> r : results) {
                            try {
                                sink.write(r.value());
                            } catch (Exception e) {
                                log.error("[kxj: Sink 写入失败]", e);
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("[kxj: 拉取异常]", e);
            }
        }

        engine.stop();
    }

    /**
     * 停止 Kafka Source 流
     */
    public void stop() {
        running = false;
        kafkaSource.stop();
        pollExecutor.shutdown();
        try {
            if (!pollExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                pollExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            pollExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
