package com.kxj.streamingdataengine.core.model;

import lombok.Builder;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

/**
 * 流数据记录
 * 支持事件时间和处理时间
 *
 * @param key            记录唯一标识
 * @param value          实际数据
 * @param eventTime      事件时间（业务时间）
 * @param processingTime 处理时间（系统时间）
 * @param ingestionTime  数据到达时间
 * @param attributes     扩展属性
 * @param partition      数据分区
 * @param sequenceNumber 序列号（用于排序和去重）
 */
@Builder
public record StreamRecord<T>(String key, T value, long eventTime, long processingTime, long ingestionTime,
                              Map<String, Object> attributes, int partition,
                              long sequenceNumber) implements Serializable, Comparable<StreamRecord<T>> {

    @Serial
    private static final long serialVersionUID = 1L;

    public StreamRecord(String key, T value, long eventTime, int partition, long sequenceNumber) {
        this(key, value, eventTime, System.currentTimeMillis(), System.currentTimeMillis(),
                Map.of(), partition, sequenceNumber);
    }

    public StreamRecord(String key, T value, long eventTime, long processingTime, long ingestionTime,
                        Map<String, Object> attributes, int partition, long sequenceNumber) {
        this.key = key;                          // 记录唯一标识，用于分区路由和状态存储
        this.value = value;                      // 实际业务数据，转换/聚合的操作对象
        this.eventTime = eventTime;              // 事件时间（业务发生时间），Watermark计算和窗口分配的基础
        this.processingTime = processingTime;    // 处理时间（系统当前时间），用于延迟监控和超时判断
        this.ingestionTime = ingestionTime;      // 摄入时间（数据到达系统时间），用于端到端延迟分析
        this.attributes = attributes != null ? Map.copyOf(attributes) : Map.of();  // 扩展属性，传递上下文元数据
        this.partition = partition;              // 数据分区号，支持并行处理和分区级聚合
        this.sequenceNumber = sequenceNumber;    // 全局序列号，用于排序、去重和Exactly-Once语义
    }

    /**
     * 创建带有新事件时间的记录
     */
    public StreamRecord<T> withEventTime(long newEventTime) {
        return new StreamRecord<>(key, value, newEventTime, processingTime, ingestionTime,
                attributes, partition, sequenceNumber);
    }

    /**
     * 创建带有新值的记录
     */
    public <R> StreamRecord<R> withValue(R newValue) {
        return new StreamRecord<>(key, newValue, eventTime, processingTime, ingestionTime,
                attributes, partition, sequenceNumber);
    }

    @Override
    public int compareTo(StreamRecord<T> other) {
        int cmp = Long.compare(this.eventTime, other.eventTime);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.sequenceNumber, other.sequenceNumber);
    }

    /**
     * 计算延迟（事件时间与处理时间的差）
     */
    public long getLatency() {
        return processingTime - eventTime;
    }
}
