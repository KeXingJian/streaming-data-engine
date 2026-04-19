package com.kxj.streamingdataengine.aggregation;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.storage.lsm.LSMTree;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MergeTree 增量聚合引擎
 * 借鉴ClickHouse的MergeTree引擎设计
 * 核心特性：
 * 1. 按主键排序存储
 * 2. 后台合并相同主键的数据
 * 3. 支持增量聚合
 * 4. 数据分区和分片
 */
@Slf4j
public class MergeTreeAggregator<K extends Comparable<K>, T, ACC, R> {

    private final AggregateFunction<T, ACC, R> aggregateFunction; // 聚合函数

    private final Map<String, LSMTree<CompositeKey<K>, AggregatedValue<T, ACC, R>>> partitions; // 分区存储：分区键 -> LSMTree

    private final Set<String> activePartitions; // 活跃分区（正在写入）

    private final PartitionKeyExtractor<T> partitionKeyExtractor; // 分区键提取器

    private final PrimaryKeyExtractor<T, K> primaryKeyExtractor; // 主键提取器

    private final Thread mergeThread; // 后台合并线程
    private volatile boolean running = true;

    private final AtomicLong totalRecords; // 统计信息
    private final AtomicLong aggregatedRecords;

    public MergeTreeAggregator(
            AggregateFunction<T, ACC, R> aggregateFunction,
            PartitionKeyExtractor<T> partitionKeyExtractor,
            PrimaryKeyExtractor<T, K> primaryKeyExtractor) {
        this.aggregateFunction = aggregateFunction;
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.primaryKeyExtractor = primaryKeyExtractor;
        this.partitions = new ConcurrentHashMap<>();
        this.activePartitions = ConcurrentHashMap.newKeySet();
        this.totalRecords = new AtomicLong(0);
        this.aggregatedRecords = new AtomicLong(0);

        // 启动后台合并线程
        this.mergeThread = Thread.ofVirtual().start(this::mergeLoop);
    }

    /**
     * 插入数据（增量聚合）
     */
    public void insert(StreamRecord<T> record) {

        // [kxj: MergeTree增量聚合 - 按分区键和主键预聚合，减少存储和计算量]
        String partitionKey = partitionKeyExtractor.extract(record.value());
        K primaryKey = primaryKeyExtractor.extract(record.value());

        // [kxj: 按分区键获取或创建LSMTree分区，支持分区级并行写入]
        LSMTree<CompositeKey<K>, AggregatedValue<T, ACC, R>> partition =
                partitions.computeIfAbsent(partitionKey, k -> new LSMTree<>());

        CompositeKey<K> key = new CompositeKey<>(primaryKey, record.eventTime());

        // [kxj: 查询同主键现有聚合值，若存在则增量更新，不存在则创建新累加器]
        Optional<AggregatedValue<T, ACC, R>> existing = partition.get(key);
        ACC accumulator;

        if (existing.isPresent()) {
            accumulator = existing.get().accumulator;
            aggregateFunction.add(record.value(), accumulator);
            aggregatedRecords.incrementAndGet();
        } else {
            accumulator = aggregateFunction.createAccumulator();
            aggregateFunction.add(record.value(), accumulator);
        }

        // 写入新的聚合值
        AggregatedValue<T, ACC, R> newValue = new AggregatedValue<>(
                record.value(), accumulator, record.eventTime()
        );
        partition.put(key, newValue);

        totalRecords.incrementAndGet();
        activePartitions.add(partitionKey);
    }

    /**
     * 批量插入
     */
    public void insertBatch(List<StreamRecord<T>> records) {
        // 按分区键分组
        Map<String, List<StreamRecord<T>>> grouped = new HashMap<>();

        for (StreamRecord<T> record : records) {
            String partitionKey = partitionKeyExtractor.extract(record.value());
            grouped.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(record);
        }

        // 并行处理各分区
        grouped.entrySet().parallelStream().forEach(entry -> {
            // 按主键预聚合
            Map<K, List<StreamRecord<T>>> byKey = new HashMap<>();
            for (StreamRecord<T> record : entry.getValue()) {
                K key = primaryKeyExtractor.extract(record.value());
                byKey.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
            }

            // 批量写入聚合结果
            for (Map.Entry<K, List<StreamRecord<T>>> keyEntry : byKey.entrySet()) {
                ACC accumulator = aggregateFunction.createAccumulator();
                long maxTimestamp = 0;
                T firstValue = null;

                for (StreamRecord<T> record : keyEntry.getValue()) {
                    if (firstValue == null) firstValue = record.value();
                    aggregateFunction.add(record.value(), accumulator);
                    maxTimestamp = Math.max(maxTimestamp, record.eventTime());
                }

                LSMTree<CompositeKey<K>, AggregatedValue<T, ACC, R>> partition =
                        partitions.computeIfAbsent(entry.getKey(), k -> new LSMTree<>());

                CompositeKey<K> key = new CompositeKey<>(keyEntry.getKey(), maxTimestamp);
                AggregatedValue<T, ACC, R> value = new AggregatedValue<>(firstValue, accumulator, maxTimestamp);
                partition.put(key, value);
            }

            activePartitions.add(entry.getKey());
            totalRecords.addAndGet(entry.getValue().size());
        });
    }

    /**
     * 查询聚合结果
     */
    public List<R> query(String partitionKey, K startKey, K endKey) {
        LSMTree<CompositeKey<K>, AggregatedValue<T, ACC, R>> partition = partitions.get(partitionKey);
        if (partition == null) {
            return List.of();
        }

        CompositeKey<K> start = new CompositeKey<>(startKey, 0);
        CompositeKey<K> end = new CompositeKey<>(endKey, Long.MAX_VALUE);

        List<R> results = new ArrayList<>();
        List<Map.Entry<CompositeKey<K>, AggregatedValue<T, ACC, R>>> entries = partition.range(start, end);

        for (Map.Entry<CompositeKey<K>, AggregatedValue<T, ACC, R>> entry : entries) {
            R result = aggregateFunction.getResult(entry.getValue().accumulator);
            results.add(result);
        }

        return results;
    }

    /**
     * 合并循环
     */
    private void mergeLoop() {
        while (running) {
            try {
                Thread.sleep(60000); // 每分钟检查一次

                for (String partitionKey : activePartitions) {
                    LSMTree<CompositeKey<K>, AggregatedValue<T, ACC, R>> partition = partitions.get(partitionKey);
                    if (partition != null) {
                        try {
                            partition.compact();
                        } catch (IOException e) {
                            log.error("Compaction failed for partition {}", partitionKey, e);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * 获取统计信息
     */
    public Stats getStats() {
        return new Stats(
                totalRecords.get(),
                aggregatedRecords.get(),
                partitions.size(),
                activePartitions.size()
        );
    }

    /**
     * 关闭引擎
     */
    public void close() {
        running = false;
        mergeThread.interrupt();
    }

    // ============== 内部类和接口 ==============

    @FunctionalInterface
    public interface PartitionKeyExtractor<T> {
        String extract(T value);
    }

    @FunctionalInterface
    public interface PrimaryKeyExtractor<T, K> {
        K extract(T value);
    }

    /**
     * 复合键（主键 + 时间戳）
     */
    @AllArgsConstructor
    public static class CompositeKey<K extends Comparable<K>> implements Comparable<CompositeKey<K>> {
        private final K key;
        private final long timestamp;

        @Override
        public int compareTo(CompositeKey<K> other) {
            int cmp = this.key.compareTo(other.key);
            if (cmp != 0) return cmp;
            return Long.compare(this.timestamp, other.timestamp);
        }
    }

    /**
     * 聚合值包装
     */
    @AllArgsConstructor
    public static class AggregatedValue<T, ACC, R> {
        private final T originalValue;
        private final ACC accumulator;
        private final long timestamp;
    }

    public record Stats(long totalRecords, long aggregatedRecords, int partitionCount, int activePartitionCount) {

        public double getAggregationRatio() {
            return totalRecords > 0 ? (double) aggregatedRecords / totalRecords : 0;
        }
    }
}
